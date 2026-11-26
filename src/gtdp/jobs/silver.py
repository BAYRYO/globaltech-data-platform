"""Silver job: the bronze records of ``--date`` checked, deduplicated and published in silver."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gtdp.audit import Layer, Run
from gtdp.io import (
    INGESTION_DATE,
    ensure_no_later_partition,
    path_exists,
    read_table,
    replace_table,
    write_partitions,
)
from gtdp.jobs.registry import JobContext, job
from gtdp.schemas import SILVER_REJECTS
from gtdp.silver.pipelines import (
    PIPELINES,
    bronze_records,
    classified,
    file_columns,
    rejects_path,
    table_path,
)
from gtdp.silver.pseudonymize import salt_frame
from gtdp.silver.quality import (
    DISPOSITION,
    Disposition,
    conform,
    duplicate_keys,
    measures,
    rejects,
)


class SilverCheckError(RuntimeError):
    """The records of a day do not add up, or a silver table breaks one of its invariants."""


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
    rejects_written: int | None = None
    rows_written: int | None = None
    """Rows of the day's silver partition once written; ``None`` for a rebuilt table."""
    violations: dict[str, int] = field(default_factory=dict)

    def problems(self) -> list[str]:
        clean, rejected, duplicated = self.clean or 0, self.rejected or 0, self.duplicated or 0
        problems = []
        if clean + rejected + duplicated != self.rows_in:
            problems.append(
                f"{self.rows_in} bronze records, but clean {clean} + rejected {rejected} "
                f"+ duplicated {duplicated} = {clean + rejected + duplicated}"
            )
        if self.rejects_written != rejected:
            problems.append(f"{rejected} rejected, {self.rejects_written} in silver/rejects")
        if self.rows_written is not None and self.rows_written != clean:
            problems.append(f"{clean} clean, {self.rows_written} in the silver partition")
        problems += [f"{rows} rows break {name}" for name, rows in self.violations.items() if rows]
        return problems


@job(
    "silver",
    Layer.SILVER,
    "Check, deduplicate and publish in silver the bronze records of --date (one --source, or all)",
)
def silver(ctx: JobContext) -> None:
    """Publish the silver tables and rejects of ``--date`` for one source, or all of them.

    Refuses a day earlier than one already published, unless ``--force``. Fails when a source's
    records do not add up (``bronze = clean + rejected + duplicated``), when the written rows
    differ from the counted ones, or when a table breaks its key; the audit row keeps the counts.
    """
    day = ctx.run.ingestion_date
    if day is None:
        raise ValueError("silver needs --date")
    sources = [ctx.run.source] if ctx.run.source else list(PIPELINES)
    if unknown := [source for source in sources if source not in PIPELINES]:
        raise ValueError(f"unknown source {unknown}, expected one of {sorted(PIPELINES)}")
    for source in sources:
        ensure_no_later_partition(
            ctx.spark,
            rejects_path(ctx.cfg.paths.silver, source),
            INGESTION_DATE,
            day,
            force=ctx.force,
        )

    salt = salt_frame(ctx.spark, ctx.cfg.pseudonymization.salt)
    reports: dict[str, SourceReport] = {}
    for source in sources:
        try:
            _publish(ctx, source, day, salt, reports)
        finally:
            _record(ctx.run, reports)
        if problems := reports[source].problems():
            raise SilverCheckError(f"{source} {day}: " + "; ".join(problems))


def _publish(
    ctx: JobContext, source: str, day: date, salt: DataFrame, reports: dict[str, SourceReport]
) -> None:
    spark, paths = ctx.spark, ctx.cfg.paths
    pipeline = PIPELINES[source]
    columns = pipeline.entity.source.columns
    partition = f"{paths.bronze}/{source}/{INGESTION_DATE}={day}"
    if not path_exists(spark, partition):
        raise FileNotFoundError(f"{partition} not found: run bronze for {day} first")
    in_file = file_columns(spark, paths.bronze, source, day)
    report = reports[source] = SourceReport(
        rows_in=bronze_records(spark, paths.bronze, source, day, history=False).count(),
        unknown_columns=[column for column in in_file if column not in columns],
    )

    target, rejected = table_path(paths.silver, source), rejects_path(paths.silver, source)
    of_day = F.col(INGESTION_DATE) == F.lit(day)
    records = classified(spark, paths, source, day).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        report.measures = measures(records.where(of_day))
        report.clean = report.measures.get(Disposition.CLEAN.value, 0)
        report.rejected = report.measures.get(Disposition.REJECTED.value, 0)
        report.duplicated = sum(
            report.measures.get(kind.value, 0)
            for kind in Disposition
            if kind not in (Disposition.CLEAN, Disposition.REJECTED)
        )
        line = [column for column in in_file if column in columns]
        rejected_rows = rejects(records.where(of_day), line, source=source, run_id=ctx.run.run_id)
        write_partitions(conform(rejected_rows, SILVER_REJECTS), rejected, {INGESTION_DATE: [day]})
        clean = records.where(F.col(DISPOSITION) == Disposition.CLEAN.value)
        if pipeline.partitioned:
            write_partitions(
                pipeline.build(clean.where(of_day), salt), target, {INGESTION_DATE: [day]}
            )
        else:
            replace_table(pipeline.build(clean, salt), target)
    finally:
        records.unpersist()

    written = read_table(spark, target, pipeline.schema)
    report.rejects_written = read_table(spark, rejected, SILVER_REJECTS).where(of_day).count()
    if pipeline.partitioned:
        report.rows_written = written.where(of_day).count()
    report.violations = {"duplicate_key": duplicate_keys(written, pipeline.key)}
    if pipeline.checks is not None:
        report.violations |= pipeline.checks(written)


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
