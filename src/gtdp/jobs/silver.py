"""Silver job: the bronze records of ``--date`` checked, deduplicated and published in silver."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import date

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gtdp.audit import Layer, Run
from gtdp.config import ConfigError, LakePaths
from gtdp.io import (
    INGESTION_DATE,
    LaterPartitionError,
    drop_partitions,
    list_partition_values,
    path_exists,
    read_table,
)
from gtdp.jobs.registry import JobContext, job
from gtdp.publish import publish
from gtdp.schemas import REJECTS_PARTITIONS, SILVER_REJECTS
from gtdp.silver import days
from gtdp.silver.pipelines import (
    PIPELINES,
    Pipeline,
    bronze_records,
    classified,
    file_columns,
    rejects_path,
    table_path,
    with_dependents,
)
from gtdp.silver.pseudonymize import check_salt, ensure_fingerprint, salt_frame
from gtdp.silver.quality import (
    DISPOSITION,
    Disposition,
    conform,
    duplicate_keys,
    measures,
    rejects,
)

logger = logging.getLogger(__name__)


class SilverCheckError(RuntimeError):
    """The records of a day do not add up, or a silver table breaks one of its invariants."""


class DayOrderError(RuntimeError):
    """An earlier day is not complete in silver, or a source's references are not built yet."""


@dataclass
class SourceReport:
    """What silver did with the bronze records of one source and day, stored under
    ``metrics_json.sources.<source>``."""

    rows_in: int
    """Records of the bronze partition, counted on their own."""
    unknown_columns: list[str]
    clean: int | None = None
    rejected: int | None = None
    duplicated: int | None = None
    measures: dict[str, int] = field(default_factory=dict)
    """Records per disposition, reject reason and flag (``gtdp.silver.quality.measures``)."""
    rows_built: int | None = None
    """Rows to publish: the day's partition, or the whole rebuilt table."""
    violations: dict[str, int] = field(default_factory=dict)
    """Rows to publish that break each invariant, checked before anything is written."""
    rejects_written: int | None = None
    rows_written: int | None = None
    written_violations: dict[str, int] = field(default_factory=dict)
    rolled_back: bool = False
    """The checks after the write failed and the run removed what it published."""

    def problems_before(self, partitioned: bool) -> list[str]:
        clean, rejected, duplicated = self.clean or 0, self.rejected or 0, self.duplicated or 0
        problems = []
        if clean + rejected + duplicated != self.rows_in:
            problems.append(
                f"{self.rows_in} bronze records, but clean {clean} + rejected {rejected} "
                f"+ duplicated {duplicated} = {clean + rejected + duplicated}"
            )
        if partitioned and self.rows_built != clean:
            problems.append(f"{clean} clean, {self.rows_built} rows to publish")
        return problems + _broken(self.violations)

    def problems_after(self) -> list[str]:
        problems = []
        if self.rejects_written != self.rejected:
            problems.append(f"{self.rejected} rejected, {self.rejects_written} in silver/rejects")
        if self.rows_written != self.rows_built:
            problems.append(f"{self.rows_built} rows published, {self.rows_written} read back")
        return problems + _broken(self.written_violations)


def _broken(violations: dict[str, int]) -> list[str]:
    return [f"{rows} rows break {name}" for name, rows in violations.items() if rows]


@job(
    "silver",
    Layer.SILVER,
    "Check, deduplicate and publish in silver the bronze records of --date (one --source, or all)",
    pseudonymizes=True,
)
def silver(ctx: JobContext) -> None:
    """Publish the silver tables and rejects of ``--date`` for one source, or all of them in
    dependency order (customers and products, then orders, then weblogs).

    Before any write: the salt must be fit for the environment and match the one silver was
    built with; every earlier day must be complete and no later day complete, unless
    ``--force``; orders need customers and products published for the same day. A run on a
    day makes every later day incomplete, and the sources that reference a replayed source due
    again for the day. Each source is checked before it is written (the records add up, the key
    stays unique, SCD2 holds) and read back after; a failed check after the write removes what
    the run published for the source. A source gets its marker once verified, the day its
    ``_SUCCESS`` once every source has one.
    """
    day = ctx.run.ingestion_date
    if day is None:
        raise ConfigError("silver needs --date")
    if ctx.run.source and ctx.run.source not in PIPELINES:
        raise ValueError(f"unknown source {ctx.run.source!r}, expected one of {sorted(PIPELINES)}")
    sources = [ctx.run.source] if ctx.run.source else list(PIPELINES)
    spark, paths, salt = ctx.spark, ctx.cfg.paths, ctx.cfg.pseudonymization.salt

    check_salt(ctx.cfg.env, salt)
    _check_order(spark, paths, day, sources, force=ctx.force)
    for source in sources:
        partition = f"{paths.bronze}/{source}/{INGESTION_DATE}={day}"
        if not path_exists(spark, partition):
            raise FileNotFoundError(f"{partition} not found: run bronze for {day} first")
    ensure_fingerprint(spark, paths.ops, salt, ctx.run.run_id)

    if later := days.unmark_after(spark, paths.silver, day):
        logger.warning("days %s are no longer complete: replay them in order", later)
    days.unmark(spark, paths.silver, day, with_dependents(sources))
    salted = salt_frame(spark, salt)
    reports: dict[str, SourceReport] = {}
    for source in sources:
        try:
            _publish(ctx, source, day, salted, reports)
        finally:
            _record(ctx.run, reports)
        days.mark_source(spark, paths.silver, day, source, ctx.run.run_id)
    days.mark_complete(spark, paths.silver, day, PIPELINES, ctx.run.run_id)


def _check_order(
    spark: SparkSession, paths: LakePaths, day: date, sources: list[str], *, force: bool
) -> None:
    complete = days.complete_days(spark, paths.silver)
    landed = {
        date.fromisoformat(value)
        for source in PIPELINES
        for value in list_partition_values(spark, f"{paths.bronze}/{source}", INGESTION_DATE)
    }
    problems: list[Exception] = []
    if incomplete := sorted(d for d in landed if d < day and d not in complete):
        problems.append(
            DayOrderError(
                f"{incomplete[-1]} is not complete in silver, which {day} deduplicates against; "
                f"run silver for the days in order (replay {incomplete[0]} first) or force it"
            )
        )
    if later := [d for d in complete if d > day]:
        problems.append(
            LaterPartitionError(
                f"silver already holds the complete day {later[-1]}, later than {day}; "
                "replay the days in order or force it, then replay the later days"
            )
        )
    if problems and not force:
        raise problems[0]
    for problem in problems:
        logger.warning("%s; forced", problem)
    for source in sources:
        for dependency in PIPELINES[source].depends_on:
            built = days.latest_day(spark, paths.silver, dependency)
            if dependency not in sources and built != day:
                raise DayOrderError(
                    f"{source} of {day} are checked against {dependency} as published for "
                    f"{day}, but {dependency} is published for {built}: run silver --date {day} "
                    f"--source {dependency} first, or the whole day"
                )


def _publish(
    ctx: JobContext, source: str, day: date, salt: DataFrame, reports: dict[str, SourceReport]
) -> None:
    spark, paths = ctx.spark, ctx.cfg.paths
    pipeline = PIPELINES[source]
    columns = pipeline.entity.source.columns
    in_file = file_columns(spark, paths.bronze, source, day)
    report = reports[source] = SourceReport(
        rows_in=bronze_records(spark, paths.bronze, source, day, history=False).count(),
        unknown_columns=[column for column in in_file if column not in columns],
    )

    target, rejected = table_path(paths.silver, source), rejects_path(paths.silver, source)
    of_day = F.col(INGESTION_DATE) == F.lit(day)
    trusted = [d for d in days.complete_days(spark, paths.silver) if d < day]
    records = classified(spark, paths, source, day, salt).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        report.measures = measures(records.where(of_day))
        report.clean = report.measures.get(Disposition.CLEAN.value, 0)
        report.rejected = report.measures.get(Disposition.REJECTED.value, 0)
        report.duplicated = sum(
            report.measures.get(kind.value, 0)
            for kind in Disposition
            if kind not in (Disposition.CLEAN, Disposition.REJECTED)
        )
        clean = records.where(F.col(DISPOSITION) == Disposition.CLEAN.value)
        rows = pipeline.build(clean.where(of_day) if pipeline.partitioned else clean)
        report.rows_built = rows.count()
        earlier = _published(spark, paths, pipeline, trusted) if pipeline.partitioned else None
        report.violations = _violations(pipeline, rows, earlier)
        if problems := report.problems_before(pipeline.partitioned):
            raise SilverCheckError(f"{source} {day}, nothing published: " + "; ".join(problems))
        line = [column for column in in_file if column in columns]
        rejected_rows = rejects(records.where(of_day), line, source=source, run_id=ctx.run.run_id)
        rejected_rows = conform(rejected_rows, SILVER_REJECTS)
        publish(rejected_rows, rejected, SILVER_REJECTS, REJECTS_PARTITIONS, [day])
        if pipeline.partitioned:
            publish(rows, target, pipeline.schema, pipeline.partitions, [day])
        else:
            publish(rows, target, pipeline.schema)
    finally:
        records.unpersist()

    report.rejects_written = read_table(spark, rejected, SILVER_REJECTS).where(of_day).count()
    written = _published(spark, paths, pipeline, [*trusted, day])
    report.rows_written = (written.where(of_day) if pipeline.partitioned else written).count()
    report.written_violations = _violations(pipeline, written)
    if problems := report.problems_after():
        _roll_back(ctx, source, day, salt)
        report.rolled_back = True
        raise SilverCheckError(f"{source} {day}, publication removed: " + "; ".join(problems))


def _published(
    spark: SparkSession, paths: LakePaths, pipeline: Pipeline, trusted: list[date]
) -> DataFrame:
    """The rows of the table readers can trust: the partitions of ``trusted`` days, or all."""
    table = read_table(
        spark, table_path(paths.silver, pipeline.entity.source.name), pipeline.schema
    )
    return table.where(F.col(INGESTION_DATE).isin(trusted)) if pipeline.partitioned else table


def _violations(
    pipeline: Pipeline, rows: DataFrame, earlier: DataFrame | None = None
) -> dict[str, int]:
    """Rows breaking each invariant: the key unique over ``rows`` and the ``earlier`` rows, then
    the table's own checks on ``rows``."""
    keys = rows.select(*pipeline.key)
    if earlier is not None:
        keys = keys.unionByName(earlier.select(*pipeline.key))
    found = {"duplicate_key": duplicate_keys(keys, pipeline.key)}
    if pipeline.checks is not None:
        found |= pipeline.checks(rows)
    return found


def _roll_back(ctx: JobContext, source: str, day: date, salt: DataFrame) -> None:
    """Remove what this run published for ``source`` and ``day``. A rebuilt table goes back to
    the latest earlier day the source was published for, rebuilt from bronze, or to empty."""
    spark, paths = ctx.spark, ctx.cfg.paths
    pipeline = PIPELINES[source]
    target = table_path(paths.silver, source)
    drop_partitions(spark, rejects_path(paths.silver, source), {INGESTION_DATE: [day]})
    if pipeline.partitioned:
        drop_partitions(spark, target, {INGESTION_DATE: [day]})
        return
    previous = days.latest_day(spark, paths.silver, source, before=day)
    if previous is None:
        publish(spark.createDataFrame([], pipeline.schema), target, pipeline.schema)
        return
    records = classified(spark, paths, source, previous, salt)
    clean = records.where(F.col(DISPOSITION) == Disposition.CLEAN.value)
    publish(pipeline.build(clean), target, pipeline.schema)


def _record(run: Run, reports: dict[str, SourceReport]) -> None:
    """Store the counts so far in the audit row, which a failed check also writes."""
    run.metrics["sources"] = {source: asdict(report) for source, report in reports.items()}
    run.metrics["unknown_columns"] = {
        source: report.unknown_columns for source, report in reports.items()
    }
    run.counters.rows_in = sum(report.rows_in for report in reports.values())
    run.counters.rows_out = _total([report.clean for report in reports.values()])
    run.counters.rows_rejected = _total([report.rejected for report in reports.values()])
    run.counters.rows_duplicated = _total([report.duplicated for report in reports.values()])


def _total(counts: list[int | None]) -> int | None:
    return None if None in counts else sum(c for c in counts if c is not None)
