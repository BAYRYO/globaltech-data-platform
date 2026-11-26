"""Print the state of the bronze tables, to check an ingestion or a replay.

    scripts/cluster/submit.sh src/gtdp/bronze/report.py [--env local] [--samples N]

1. One line per source and ingestion day: rows, corrupt records, landing files, the run that
   wrote the partition, and ``hash_sum``, a checksum of the record hashes that ignores row order.
   Two runs of a day that wrote the same records give the same ``hash_sum``.
2. Per source and day present in landing or in bronze, the landing lines whose SHA-256 is no
   record hash, and the records whose hash matches no landing line: their counts and at most
   ``--samples`` of their hashes, never their values (they hold personal data). A lossless bronze
   leaves out the CSV header lines only.
3. The orders rows that hold a ``discount_pct`` value (ADR-0007).
4. The audit rows of the bronze runs, then the counts of each run and landed file.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType

from gtdp.audit import audit_path
from gtdp.config import Env, load_config
from gtdp.io import INGESTION_DATE, list_partition_values, read_table
from gtdp.schemas import CORRUPT_RECORD, RECORD_HASH, RUN_ID, SOURCE_FILE, SOURCES, bronze_schema
from gtdp.spark import build_spark

SAMPLES = 5


def _bronze(spark: SparkSession, bronze: str, source: str) -> DataFrame:
    return read_table(spark, f"{bronze}/{source}", bronze_schema(SOURCES[source]))


def partitions(spark: SparkSession, bronze: str) -> DataFrame:
    """One row per source and ``ingestion_date`` of the bronze tables under ``bronze``."""
    per_source = [
        _bronze(spark, bronze, source)
        .groupBy(INGESTION_DATE)
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.count(CORRUPT_RECORD).alias("corrupt"),
            F.countDistinct(SOURCE_FILE).alias("files"),
            F.concat_ws(" ", F.array_sort(F.collect_set(RUN_ID))).alias("run_id"),
            # The first 60 bits of each SHA-256, summed: the same for the same multiset of records.
            F.sum(F.conv(F.substring(RECORD_HASH, 1, 15), 16, 10).cast("decimal(38,0)"))
            .cast("string")
            .alias("hash_sum"),
        )
        .select(F.lit(source).alias("source"), "*")
        for source in SOURCES
    ]
    return reduce(DataFrame.unionByName, per_source).orderBy("source", INGESTION_DATE)


def unmatched(spark: SparkSession, landing: str, bronze: str, samples: int = SAMPLES) -> DataFrame:
    """Per source and day of landing or bronze, the line hashes that only one side holds."""
    per_source = []
    for source in SOURCES:
        records = _bronze(spark, bronze, source).select(
            INGESTION_DATE, F.col(RECORD_HASH).alias("hash")
        )
        lines = records.limit(0)
        if list_partition_values(spark, f"{landing}/{source}", INGESTION_DATE):
            lines = spark.read.text(f"{landing}/{source}").select(
                INGESTION_DATE, F.sha2("value", 256).alias("hash")
            )
        per_source.append(
            _count(lines, "landing_lines")
            .join(_count(records, "bronze_rows"), INGESTION_DATE, "full")
            .join(
                _gap(lines.exceptAll(records), "landing_not_in_bronze", samples),
                INGESTION_DATE,
                "left",
            )
            .join(
                _gap(records.exceptAll(lines), "bronze_not_in_landing", samples),
                INGESTION_DATE,
                "left",
            )
            .fillna(0)
            .select(F.lit(source).alias("source"), "*")
        )
    return reduce(DataFrame.unionByName, per_source).orderBy("source", INGESTION_DATE)


def _count(frame: DataFrame, name: str) -> DataFrame:
    return frame.groupBy(INGESTION_DATE).agg(F.count(F.lit(1)).alias(name))


def _gap(hashes: DataFrame, name: str, samples: int) -> DataFrame:
    """Per day, the number of ``hashes`` as ``name``, and the ``samples`` smallest."""
    smallest = Window.partitionBy(INGESTION_DATE).orderBy("hash")
    return (
        hashes.withColumn("rank", F.row_number().over(smallest))
        .groupBy(INGESTION_DATE)
        .agg(
            F.count(F.lit(1)).alias(name),
            F.array_sort(F.collect_list(F.when(F.col("rank") <= samples, F.col("hash")))).alias(
                f"{name}_hashes"
            ),
        )
    )


def discounts(spark: SparkSession, bronze: str) -> DataFrame:
    """Per day, the orders rows and those with a ``discount_pct`` value."""
    return (
        _bronze(spark, bronze, "orders")
        .groupBy(INGESTION_DATE)
        .agg(F.count(F.lit(1)).alias("rows"), F.count("discount_pct").alias("with_discount_pct"))
        .orderBy(INGESTION_DATE)
    )


def runs(spark: SparkSession, audit: str) -> DataFrame:
    """The audit rows of the bronze runs, oldest first."""
    return (
        read_table(spark, audit)
        .where(F.col("job_name") == "bronze")
        .select(
            "started_at",
            "run_id",
            "source",
            INGESTION_DATE,
            "status",
            "rows_in",
            "rows_out",
            "duration_s",
            "yarn_app_id",
            "error_message",
            "metrics_json",
        )
        .orderBy("started_at")
    )


def file_counts(bronze_runs: DataFrame) -> DataFrame:
    """One row per run and landed file: its counts from ``metrics_json.files``."""
    names = ("manifest_rows", "landing_lines", "rows_read", "rows_written", "corrupt_records")
    counts = StructType([StructField(name, LongType()) for name in names])
    files = StructType([StructField("files", MapType(StringType(), counts))])
    return bronze_runs.select(
        "run_id",
        INGESTION_DATE,
        "status",
        F.explode(F.from_json("metrics_json", files).files).alias("file", "counts"),
    ).select("run_id", INGESTION_DATE, "status", "file", "counts.*")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env", choices=[env.value for env in Env], default=Env.CLUSTER.value)
    parser.add_argument("--samples", type=int, default=SAMPLES, help="hashes shown per gap")
    args = parser.parse_args(argv)
    cfg = load_config(args.env)
    spark = build_spark(cfg, f"{cfg.app.name}-bronze-report")
    try:
        partitions(spark, cfg.paths.bronze).show(100, truncate=False)
        unmatched(spark, cfg.paths.landing, cfg.paths.bronze, args.samples).show(
            100, truncate=False
        )
        discounts(spark, cfg.paths.bronze).show(truncate=False)
        bronze_runs = runs(spark, audit_path(cfg))
        bronze_runs.drop("metrics_json").show(100, truncate=False)
        file_counts(bronze_runs).show(400, truncate=False)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
