"""Bronze job: the files landed for ``--date``, copied raw and lossless into Parquet partitions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gtdp.audit import Layer, Run
from gtdp.bronze.landing import count_data_lines, landed_files, read_header, read_records
from gtdp.bronze.transform import check_counts, to_bronze
from gtdp.io import INGESTION_DATE, read_table, write_partitions
from gtdp.jobs.registry import JobContext, job
from gtdp.schemas import CORRUPT_RECORD, SOURCE_FILE, SOURCES, bronze_schema


@dataclass
class FileReport:
    """The counts of one landed file, stored under ``metrics_json.files.<manifest path>``."""

    source: str
    manifest_rows: int
    landing_lines: int
    rows_read: int
    unknown_columns: list[str]
    rows_written: int | None = None
    corrupt_records: int | None = None


@job(
    "bronze",
    Layer.BRONZE,
    "Copy the files landed for --date (one --source, or all) to bronze/<source>/ingestion_date=",
)
def bronze(ctx: JobContext) -> None:
    """Replace ``bronze/<source>/ingestion_date=<date>`` with the day's landed files.

    Fails before writing anything if a file's data lines differ from its manifest row count or
    from the rows read, and after writing if the rows written differ from the data lines.
    """
    day = ctx.run.ingestion_date
    if day is None:
        raise ValueError("bronze needs --date")
    spark, paths = ctx.spark, ctx.cfg.paths
    files = landed_files(spark, paths.landing, day, ctx.run.source)

    reports: dict[str, FileReport] = {}
    frames: dict[str, list[DataFrame]] = {}
    for file in files:
        columns = read_header(spark, file)
        records = read_records(spark, file, columns)
        reports[file.key] = FileReport(
            source=file.source.name,
            manifest_rows=file.manifest_rows,
            landing_lines=count_data_lines(spark, file),
            rows_read=records.count(),
            unknown_columns=[c for c in columns if c not in file.source.columns],
        )
        frames.setdefault(file.source.name, []).append(
            to_bronze(
                records,
                columns,
                source_file=file.uri,
                run_id=ctx.run.run_id,
                ingested_at=ctx.run.started_at,
                ingestion_date=day,
            )
        )
    ctx.run.counters.rows_in = sum(r.landing_lines for r in reports.values())
    _record(ctx.run, reports)
    check_counts(
        {
            key: {"manifest": r.manifest_rows, "lines": r.landing_lines, "read": r.rows_read}
            for key, r in reports.items()
        }
    )

    for source, source_frames in frames.items():
        table = f"{paths.bronze}/{source}"
        rows = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), source_frames)
        write_partitions(rows, table, {INGESTION_DATE: [day]})
        written = {
            row[SOURCE_FILE]: (row["rows"], row["corrupt"])
            for row in read_table(spark, table, bronze_schema(SOURCES[source]))
            .where(F.col(INGESTION_DATE) == F.lit(day))
            .groupBy(SOURCE_FILE)
            .agg(F.count(F.lit(1)).alias("rows"), F.count(CORRUPT_RECORD).alias("corrupt"))
            .collect()
        }
        for file in files:
            if file.source.name == source:
                report = reports[file.key]
                report.rows_written, report.corrupt_records = written.get(file.uri, (0, 0))
    ctx.run.counters.rows_out = sum(r.rows_written or 0 for r in reports.values())
    _record(ctx.run, reports)
    check_counts(
        {
            key: {"lines": r.landing_lines, "written": r.rows_written or 0}
            for key, r in reports.items()
        }
    )


def _record(run: Run, reports: dict[str, FileReport]) -> None:
    """Store the counts so far in the audit row, which a failed check also writes."""
    by_source: dict[str, list[FileReport]] = {}
    for report in reports.values():
        by_source.setdefault(report.source, []).append(report)
    run.metrics["files"] = {key: asdict(report) for key, report in reports.items()}
    run.metrics["sources"] = {
        source: {
            "rows_written": _total([r.rows_written for r in group]),
            "corrupt_records": _total([r.corrupt_records for r in group]),
        }
        for source, group in sorted(by_source.items())
    }
    run.metrics["unknown_columns"] = {
        source: sorted({column for r in group for column in r.unknown_columns})
        for source, group in sorted(by_source.items())
    }


def _total(counts: list[int | None]) -> int | None:
    return None if None in counts else sum(c for c in counts if c is not None)
