"""Print the silver reconciliation, to check a run or a replay; exit 1 on any mismatch.

    scripts/cluster/submit.sh src/gtdp/silver/report.py [--env local] [--customer ID]
        [--explain SOURCE --date YYYY-MM-DD]

1. Per source, landed day and measure: the count that the manifest predicts under the
   disposition table, the count in the audit row of the latest successful silver run of that day,
   and the count that the published tables show (rejects per reason, clean rows of a partitioned
   table). ``rows_in`` compares the manifest's rows with the bronze records the run counted.
   A measure the manifest does not predict, such as ``flagged.FUTURE_SIGNUP``, is ``info``; a
   landed day that no silver run covered yet is ``no run``.
2. Rows and content checksum of each partition of the silver tables (of the whole table when it
   has no partition), the rejects' ``_run_id`` aside: a replay that rewrites the same rows keeps
   the same ``content_sum``.
3. The audit rows of the silver runs.
4. The SCD2 history of P00042, and the price applicable to its orders on either side of its first
   price change, next to the price invoiced and the current price.
5. With ``--customer``, the customer's bronze records and its silver row (the salt is not shown).
6. With ``--explain`` and ``--date``, the plan of the classification, then the plans of the
   rejects and of the clean rows once it is persisted: both read it from memory.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gtdp.audit import audit_path
from gtdp.config import AppConfig, Env, load_config
from gtdp.io import INGESTION_DATE, list_partition_values, read_table
from gtdp.schemas import RUN_ID, SILVER_REJECTS, SOURCES, bronze_schema
from gtdp.silver.pipelines import PIPELINES, classified, rejects_path, table_path
from gtdp.silver.products import with_applicable_price
from gtdp.silver.pseudonymize import salt_frame
from gtdp.silver.quality import DISPOSITION, Disposition, rejects
from gtdp.silver.reconcile import (
    audit_measures,
    manifest_expected,
    read_manifest,
    table_measures,
)
from gtdp.spark import build_spark

HOT_PRODUCT = "P00042"
_COMPARED = ("rejected", "duplicated.", "clean")


def reconciliation(spark: SparkSession, cfg: AppConfig) -> list[tuple]:
    """``(source, day, measure, manifest, audit, table, status)`` for every compared measure."""
    paths = cfg.paths
    audited = {
        (row["source"], str(row[INGESTION_DATE])): row
        for row in audit_measures(read_table(spark, audit_path(cfg))).collect()
    }
    rows = []
    for day in list_partition_values(spark, f"{paths.landing}/_manifests", INGESTION_DATE):
        manifest = read_manifest(spark, paths.landing, date.fromisoformat(day))
        for source, predicted in sorted(manifest_expected(manifest).items()):
            run = audited.get((source, day))
            measured = dict(run["measures"]) if run else {}
            shown = table_measures(spark, paths.silver, source, date.fromisoformat(day))
            landed = sum(f["rows"] for f in manifest["files"] if f["source"] == source)
            rows.append(_line(source, day, "rows_in", landed, run["rows_in"] if run else None))
            names = predicted.keys() | measured.keys() | shown.keys()
            for name in sorted(names):
                known = name in predicted or name.startswith(_COMPARED)
                rows.append(
                    _line(
                        source,
                        day,
                        name,
                        predicted.get(name, 0) if known else None,
                        measured.get(name, 0) if run else None,
                        shown.get(name),
                    )
                )
    return rows


def _line(
    source: str, day: str, name: str, manifest: int | None, audit: int | None, table=None
) -> tuple:
    if manifest is None:
        status = "info"
    elif audit is None:
        status = "no run"
    elif audit == manifest and table in (None, audit):
        status = "ok"
    else:
        status = "MISMATCH"
    return source, day, name, manifest, audit, table, status


def contents(spark: SparkSession, cfg: AppConfig) -> DataFrame:
    """``table, ingestion_date, rows, content_sum`` for each silver table and partition."""
    frames = []
    for source, pipeline in PIPELINES.items():
        table = read_table(spark, table_path(cfg.paths.silver, source), pipeline.schema)
        frames.append(_checksum(table, pipeline.table, pipeline.partitioned))
        rejected = read_table(spark, rejects_path(cfg.paths.silver, source), SILVER_REJECTS)
        frames.append(_checksum(rejected.drop(RUN_ID), f"rejects/{source}", partitioned=True))
    return reduce(DataFrame.unionByName, frames).orderBy("table", INGESTION_DATE)


def _checksum(frame: DataFrame, name: str, partitioned: bool) -> DataFrame:
    """The first 60 bits of each row's SHA-256, summed: the same for the same multiset of rows."""
    day = F.col(INGESTION_DATE) if partitioned else F.lit(None)
    row_hash = F.sha2(F.to_json(F.struct(*frame.columns)), 256)
    return (
        frame.groupBy(day.cast("string").alias(INGESTION_DATE))
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.conv(F.substring(row_hash, 1, 15), 16, 10).cast("decimal(38,0)"))
            .cast("string")
            .alias("content_sum"),
        )
        .select(F.lit(name).alias("table"), INGESTION_DATE, "rows", "content_sum")
    )


def runs(spark: SparkSession, cfg: AppConfig) -> DataFrame:
    return (
        read_table(spark, audit_path(cfg))
        .where(F.col("job_name") == "silver")
        .select(
            "started_at",
            "run_id",
            "source",
            INGESTION_DATE,
            "status",
            "rows_in",
            "rows_out",
            "rows_rejected",
            "rows_duplicated",
            "duration_s",
            "yarn_app_id",
            "error_message",
        )
        .orderBy("started_at")
    )


def price_history(spark: SparkSession, cfg: AppConfig) -> tuple[DataFrame, DataFrame]:
    """The versions of P00042, and its orders on either side of its first price change."""
    products = read_table(
        spark, table_path(cfg.paths.silver, "products"), PIPELINES["products"].schema
    )
    versions = products.where(F.col("product_id") == HOT_PRODUCT).orderBy("version")
    second = versions.where(F.col("version") == 2).select("valid_from").first()
    current = versions.where("is_current").select("unit_price_eur").first()
    orders = read_table(spark, table_path(cfg.paths.silver, "orders"), PIPELINES["orders"].schema)
    hot = orders.where(F.col("product_id") == HOT_PRODUCT)
    if second is None:
        return versions, hot.limit(0)
    change = second["valid_from"]
    around = (
        hot.where(F.col("event_time") < change)
        .orderBy(F.col("event_time").desc())
        .limit(2)
        .unionByName(hot.where(F.col("event_time") >= change).orderBy("event_time").limit(2))
    )
    priced = with_applicable_price(around, products).select(
        "transaction_id",
        "event_time",
        F.col("unit_price_eur").alias("invoiced_unit_price"),
        "product_sk",
        "applicable_unit_price",
        F.lit(current["unit_price_eur"] if current else None).alias("current_unit_price"),
    )
    return versions, priced.orderBy("event_time")


def customer(spark: SparkSession, cfg: AppConfig, customer_id: str) -> tuple[DataFrame, DataFrame]:
    bronze = read_table(
        spark, f"{cfg.paths.bronze}/customers", bronze_schema(SOURCES["customers"])
    ).where(F.col("customer_id") == customer_id)
    silver = read_table(
        spark, table_path(cfg.paths.silver, "customers"), PIPELINES["customers"].schema
    ).where(F.col("customer_id") == customer_id)
    return bronze.orderBy(INGESTION_DATE), silver


def explain(spark: SparkSession, cfg: AppConfig, source: str, day: date) -> None:
    records = classified(spark, cfg.paths, source, day)
    print(f"== classification of {source} {day}, before persist")
    records.explain()
    persisted = records.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        print("== rejects, reading the persisted classification")
        rejects(persisted, list(SOURCES[source].columns), source=source, run_id="-").explain()
        print("== clean rows, reading the persisted classification")
        clean = persisted.where(F.col(DISPOSITION) == Disposition.CLEAN.value)
        salt = salt_frame(spark, cfg.pseudonymization.salt)
        PIPELINES[source].build(clean, salt).explain()
    finally:
        persisted.unpersist()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env", choices=[env.value for env in Env], default=Env.CLUSTER.value)
    parser.add_argument("--customer", help="a customer_id to show in bronze and silver")
    parser.add_argument("--explain", choices=sorted(PIPELINES), help="a source to explain")
    parser.add_argument("--date", type=date.fromisoformat, help="the day of --explain")
    args = parser.parse_args(argv)
    if args.explain and args.date is None:
        parser.error("--explain needs --date")
    cfg = load_config(args.env)
    spark = build_spark(cfg, f"{cfg.app.name}-silver-report")
    try:
        lines = reconciliation(spark, cfg)
        spark.createDataFrame(
            lines,
            "source string, ingestion_date string, measure string, manifest long, audit long,"
            " `table` long, status string",
        ).show(1000, truncate=False)
        mismatches = [line for line in lines if line[-1] == "MISMATCH"]
        not_run = sorted({(line[0], line[1]) for line in lines if line[-1] == "no run"})
        print(f"{len(lines)} measures, {len(mismatches)} mismatches, not run yet: {not_run}")
        contents(spark, cfg).show(100, truncate=False)
        runs(spark, cfg).show(100, truncate=False)
        for frame in price_history(spark, cfg):
            frame.show(20, truncate=False)
        if args.customer:
            for frame in customer(spark, cfg, args.customer):
                frame.show(truncate=False, vertical=True)
        if args.explain:
            explain(spark, cfg, args.explain, args.date)
    finally:
        spark.stop()
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
