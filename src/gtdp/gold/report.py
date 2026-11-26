"""Print the gold checks and samples, to verify a run or a replay; exit 1 on any mismatch.

    scripts/cluster/submit.sh src/gtdp/gold/report.py [--env local] [--explain YYYY-MM-DD]

1. Orders per ``order_date``: the silver orders of each drop (``ingestion_date``) and the rows of
   ``fact_sales``. ``ok`` when the counts are equal, ``not built`` when no gold run covered the
   date yet, ``MISMATCH`` otherwise. Shown for the last 7 order dates, the dates whose orders
   came in several drops (late orders) and every date that is not ``ok``.
2. The invariants of the star (unique keys, no orphan fact) and the optional references.
3. Rows and content checksum of each gold table: a replay that rewrites the same rows keeps the
   same ``content_sum``.
4. The versions of P00042 and its ``fact_sales`` rows on either side of its first price change.
5. Samples of the indicators: top products per country, moving average of the first and last
   days of FR, price deviation of P00042's categories, and every other indicator.
6. The audit rows of the gold runs.
7. With ``--explain``, the physical plan of the ``fact_sales`` build for the order dates of that
   drop, with the default join selection, then with broadcast joins disabled.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from gtdp.audit import audit_path
from gtdp.config import AppConfig, Env, load_config
from gtdp.gold import checks, facts
from gtdp.gold.kpis import KPIS
from gtdp.gold.tables import DIM_CUSTOMER, DIM_PRODUCT, FACT_SALES, ORDER_DATE, STAR, table_path
from gtdp.io import INGESTION_DATE, read_table
from gtdp.silver.pipelines import PIPELINES
from gtdp.silver.pipelines import table_path as silver_path
from gtdp.spark import build_spark

HOT_PRODUCT = "P00042"
SAMPLE_COUNTRY = "FR"
BROADCAST_THRESHOLD = "spark.sql.autoBroadcastJoinThreshold"


def _gold(spark: SparkSession, cfg: AppConfig, name: str, schema=None) -> DataFrame:
    return read_table(spark, table_path(cfg.paths.gold, name), schema)


def _silver_orders(spark: SparkSession, cfg: AppConfig) -> DataFrame:
    return read_table(spark, silver_path(cfg.paths.silver, "orders"), PIPELINES["orders"].schema)


def order_dates(spark: SparkSession, cfg: AppConfig) -> DataFrame:
    """``order_date``, silver orders per drop, ``silver``, ``fact_sales``, ``status``."""
    silver = _silver_orders(spark, cfg).select(
        facts.order_date().alias(ORDER_DATE), F.col(INGESTION_DATE).cast("string")
    )
    per_drop = silver.groupBy(ORDER_DATE).pivot(INGESTION_DATE).count()
    totals = silver.groupBy(ORDER_DATE).agg(
        F.countDistinct(INGESTION_DATE).alias("drops"), F.count(F.lit(1)).alias("silver")
    )
    built = (
        _gold(spark, cfg, FACT_SALES.name, FACT_SALES.schema)
        .groupBy(ORDER_DATE)
        .agg(F.count(F.lit(1)).alias("fact_sales"))
    )
    status = (
        F.when(F.col("fact_sales").isNull(), "not built")
        .when(F.col("silver") == F.col("fact_sales"), "ok")
        .otherwise("MISMATCH")
    )
    return (
        per_drop.join(totals, ORDER_DATE, "full")
        .join(built, ORDER_DATE, "full")
        .withColumn("status", status)
        .orderBy(ORDER_DATE)
    )


def star(spark: SparkSession, cfg: AppConfig) -> dict[str, DataFrame]:
    return {table.name: _gold(spark, cfg, table.name, table.schema) for table in STAR}


def integrity(tables: dict[str, DataFrame]) -> list[tuple[str, int, str]]:
    """``(check, rows, status)``: every invariant must be 0, optional references are info."""
    lines = [
        (name, rows, "ok" if rows == 0 else "VIOLATED")
        for name, rows in checks.violations(tables).items()
    ]
    lines += [(name, rows, "info") for name, rows in checks.optional_unmatched(tables).items()]
    return lines


def contents(spark: SparkSession, cfg: AppConfig) -> DataFrame:
    """``table, rows, content_sum`` for every gold table."""
    names = [table.name for table in STAR] + list(KPIS)
    return reduce(DataFrame.unionByName, [_checksum(_gold(spark, cfg, n), n) for n in names])


def _checksum(frame: DataFrame, name: str) -> DataFrame:
    """The first 60 bits of each row's SHA-256, summed: the same for the same multiset of rows."""
    row_hash = F.sha2(F.to_json(F.struct(*frame.columns)), 256)
    return frame.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.conv(F.substring(row_hash, 1, 15), 16, 10).cast("decimal(38,0)"))
        .cast("string")
        .alias("content_sum"),
    ).select(F.lit(name).alias("table"), "rows", "content_sum")


def price_history(spark: SparkSession, cfg: AppConfig) -> tuple[DataFrame, DataFrame]:
    """The versions of P00042, and its fact rows on either side of its first price change."""
    versions = (
        _gold(spark, cfg, DIM_PRODUCT.name, DIM_PRODUCT.schema)
        .where(F.col("product_id") == HOT_PRODUCT)
        .orderBy("version")
    )
    sales = _gold(spark, cfg, FACT_SALES.name, FACT_SALES.schema).where(
        F.col("product_id") == HOT_PRODUCT
    )
    second = versions.where(F.col("version") == 2).select("valid_from").first()
    current = versions.where("is_current").select("unit_price_eur").first()
    if second is None:
        return versions, sales.limit(0)
    change = second["valid_from"]
    around = (
        sales.where(F.col("event_time") < change)
        .orderBy(F.col("event_time").desc())
        .limit(2)
        .unionByName(sales.where(F.col("event_time") >= change).orderBy("event_time").limit(2))
    )
    return versions, around.select(
        "transaction_id",
        "event_time",
        "product_sk",
        "applicable_unit_price",
        "unit_price_invoiced",
        "quantity",
        "discount_pct",
        "revenue_eur",
        "invoiced_amount_eur",
        F.lit(current["unit_price_eur"] if current else None).alias("current_unit_price"),
    ).orderBy("event_time")


def samples(spark: SparkSession, cfg: AppConfig) -> list[tuple[str, DataFrame]]:
    def kpi(name: str) -> DataFrame:
        return _gold(spark, cfg, name)

    hot = _gold(spark, cfg, DIM_PRODUCT.name, DIM_PRODUCT.schema).where(
        F.col("product_id") == HOT_PRODUCT
    )
    categories = [row["category"] for row in hot.select("category").distinct().collect()]
    average = kpi("kpi_revenue_moving_avg").where(F.col("country") == SAMPLE_COUNTRY)
    daily = kpi("kpi_revenue_daily")
    return [
        (
            "kpi_top_products_country",
            kpi("kpi_top_products_country").orderBy("country", "rank", "product_id"),
        ),
        (
            f"kpi_revenue_moving_avg, first 8 days of {SAMPLE_COUNTRY}",
            average.orderBy(ORDER_DATE).limit(8),
        ),
        (
            f"kpi_revenue_moving_avg, last 8 days of {SAMPLE_COUNTRY}",
            average.orderBy(F.col(ORDER_DATE).desc()).limit(8).orderBy(ORDER_DATE),
        ),
        (
            f"kpi_price_deviation, categories of {HOT_PRODUCT}: {categories}",
            kpi("kpi_price_deviation")
            .where(F.col("category").isin(categories))
            .orderBy("category", "month"),
        ),
        (
            "kpi_revenue_daily, last 4 dates",
            daily.crossJoin(daily.agg(F.max(ORDER_DATE).alias("_last")))
            .where(F.col(ORDER_DATE) >= F.date_sub("_last", 3))
            .drop("_last")
            .orderBy(ORDER_DATE, "country"),
        ),
        ("kpi_cancel_return_rate", kpi("kpi_cancel_return_rate").orderBy("channel", "category")),
        ("kpi_avg_basket", kpi("kpi_avg_basket").orderBy("country", "segment")),
        (
            "kpi_http_errors_hourly, 8 hours with the highest 404 rate",
            kpi("kpi_http_errors_hourly").orderBy(F.col("rate_404").desc(), "event_hour").limit(8),
        ),
        ("kpi_conversion_funnel", kpi("kpi_conversion_funnel").orderBy("device")),
        (
            "kpi_data_quality",
            kpi("kpi_data_quality").orderBy(INGESTION_DATE, "source", "disposition", "code"),
        ),
    ]


def runs(spark: SparkSession, cfg: AppConfig) -> DataFrame:
    return (
        read_table(spark, audit_path(cfg))
        .where(F.col("job_name") == "gold")
        .select(
            "started_at",
            INGESTION_DATE,
            "status",
            "rows_in",
            "rows_out",
            "duration_s",
            "yarn_app_id",
            F.get_json_object("metrics_json", "$.partitions_refreshed").alias("partitions"),
            F.get_json_object("metrics_json", "$.build_violations").alias("build_violations"),
            "error_message",
        )
        .orderBy("started_at")
    )


def explain(spark: SparkSession, cfg: AppConfig, day: date) -> None:
    """The plan of the ``fact_sales`` build of the job for drop ``day``."""
    orders = _silver_orders(spark, cfg)
    of_day = orders.where(F.col(INGESTION_DATE) == F.lit(day))
    dates = [row[0] for row in of_day.select(facts.order_date()).distinct().collect()]
    of_dates = orders.where(facts.order_date().isin(dates))
    products = _gold(spark, cfg, DIM_PRODUCT.name, DIM_PRODUCT.schema)
    customers = _gold(spark, cfg, DIM_CUSTOMER.name, DIM_CUSTOMER.schema)
    print(f"== fact_sales build for the {len(dates)} order dates of {day}, default join selection")
    facts.sales(of_dates, products, customers).explain()
    previous = spark.conf.get(BROADCAST_THRESHOLD)
    spark.conf.set(BROADCAST_THRESHOLD, "-1")
    try:
        print(f"== the same build with {BROADCAST_THRESHOLD}=-1")
        facts.sales(of_dates, products, customers).explain()
    finally:
        spark.conf.set(BROADCAST_THRESHOLD, previous)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env", choices=[env.value for env in Env], default=Env.CLUSTER.value)
    parser.add_argument("--explain", type=date.fromisoformat, help="a drop whose build to explain")
    args = parser.parse_args(argv)
    cfg = load_config(args.env)
    spark = build_spark(cfg, f"{cfg.app.name}-gold-report")
    try:
        dates = order_dates(spark, cfg)
        recent = F.col(ORDER_DATE) > F.date_sub("_last", 7)
        shown = (F.col("drops") > 1) | (F.col("status") != "ok") | recent
        dates.withColumn("_last", F.max(ORDER_DATE).over(Window.partitionBy())).where(shown).drop(
            "_last"
        ).show(400, truncate=False)
        summary = dates.groupBy("status").agg(
            F.count(F.lit(1)).alias("order_dates"),
            F.sum("silver").alias("silver"),
            F.sum("fact_sales").alias("fact_sales"),
        )
        summary.show(truncate=False)
        mismatches = dates.where(F.col("status") == "MISMATCH").count()
        lines = integrity(star(spark, cfg))
        spark.createDataFrame(lines, "check string, rows long, status string").show(
            50, truncate=False
        )
        violated = [line for line in lines if line[2] == "VIOLATED"]
        contents(spark, cfg).show(50, truncate=False)
        for frame in price_history(spark, cfg):
            frame.show(20, truncate=False)
        for title, frame in samples(spark, cfg):
            print(f"== {title}")
            frame.show(60, truncate=False)
        runs(spark, cfg).show(100, truncate=False)
        print(f"{mismatches} order dates mismatched, {len(violated)} invariants violated")
        if args.explain:
            explain(spark, cfg, args.explain)
    finally:
        spark.stop()
    return 1 if mismatches or violated else 0


if __name__ == "__main__":
    sys.exit(main())
