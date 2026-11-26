"""Print the gold checks and samples, to verify a run or a replay; exit 1 on any mismatch.

    scripts/cluster/submit.sh src/gtdp/gold/report.py [--env local] [--explain YYYY-MM-DD]

1. Orders per ``order_date``: the silver orders of each complete drop (``ingestion_date``) and
   the rows of ``fact_sales``. ``ok`` when the counts are equal, ``not built`` when no gold run
   covered the date yet, ``MISMATCH`` otherwise. Shown for the last 7 order dates, the dates
   whose orders came in several drops (late orders) and every date that is not ``ok``.
2. The invariants of the star (unique keys, no orphan fact) and the optional references.
3. Gold against silver recomputed in one pass: orders, revenue at the applicable price, invoiced
   amount, orders without exactly one valid version or without customer, calendar coverage,
   completed revenue of ``kpi_revenue_daily``, and a funnel that never increases.
4. The top products of FR recomputed with a plain ``groupBy`` and ``orderBy``, beside the
   indicator.
5. Rows and content checksum of each gold table: a replay that rewrites the same rows keeps the
   same ``content_sum``.
6. The versions of P00042 and its ``fact_sales`` rows on either side of its first price change.
7. Samples of the indicators: top products per country, moving average of the first and last
   days of FR, price deviation of P00042's categories, and every other indicator.
8. The audit rows of the gold runs.
9. With ``--explain``, the physical plan of the ``fact_sales`` build for the order dates of that
   drop, with the default join selection, then with broadcast joins disabled.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date
from functools import reduce
from itertools import zip_longest

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from gtdp.audit import audit_path
from gtdp.config import AppConfig, Env, load_config
from gtdp.gold import checks, dimensions, facts
from gtdp.gold.kpis import COMPLETED, KPIS
from gtdp.gold.tables import DIM_CUSTOMER, DIM_PRODUCT, FACT_SALES, ORDER_DATE, STAR, table_path
from gtdp.io import INGESTION_DATE, read_table
from gtdp.silver.days import complete_days
from gtdp.silver.pipelines import PIPELINES
from gtdp.silver.pipelines import table_path as silver_path
from gtdp.spark import build_spark

HOT_PRODUCT = "P00042"
SAMPLE_COUNTRY = "FR"
BROADCAST_THRESHOLD = "spark.sql.autoBroadcastJoinThreshold"


def _gold(spark: SparkSession, cfg: AppConfig, name: str, schema=None) -> DataFrame:
    return read_table(spark, table_path(cfg.paths.gold, name), schema)


def _silver_orders(spark: SparkSession, cfg: AppConfig) -> DataFrame:
    """The silver orders of the days silver completed, the only ones gold reads."""
    orders = read_table(spark, silver_path(cfg.paths.silver, "orders"), PIPELINES["orders"].schema)
    return orders.where(F.col(INGESTION_DATE).isin(complete_days(spark, cfg.paths.silver)))


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


def _line(check: str, silver: object, gold: object) -> tuple[str, str, str, str]:
    return check, str(silver), str(gold), "ok" if silver == gold else "MISMATCH"


def reconciliation(spark: SparkSession, cfg: AppConfig) -> list[tuple[str, str, str, str]]:
    """``(check, silver, gold, status)``: gold beside silver recomputed in one pass."""
    orders = _silver_orders(spark, cfg)
    products = dimensions.products(
        read_table(spark, silver_path(cfg.paths.silver, "products"), PIPELINES["products"].schema)
    )
    customers = dimensions.customers(
        read_table(spark, silver_path(cfg.paths.silver, "customers"), PIPELINES["customers"].schema)
    )
    recomputed = facts.sales(orders, products, customers)
    net = F.lit(1) - F.col("discount_pct")
    invoiced = F.round(F.col("quantity") * F.col("unit_price_eur") * net, 2)
    silver = orders.agg(F.count(F.lit(1)), F.sum(invoiced)).first()
    revenue = recomputed.agg(F.sum("revenue_eur")).first()[0]
    sales = _gold(spark, cfg, FACT_SALES.name, FACT_SALES.schema)
    gold = sales.agg(
        F.count(F.lit(1)),
        F.sum("revenue_eur"),
        F.sum("invoiced_amount_eur"),
        F.sum(F.when(F.col("status") == COMPLETED, F.col("revenue_eur"))),
        F.min(ORDER_DATE),
        F.max(ORDER_DATE),
    ).first()
    broken = facts.build_violations(recomputed, silver[0])
    calendar = _gold(spark, cfg, "dim_date").agg(F.min("date"), F.max("date"), F.count(F.lit(1)))
    first, last, days = calendar.first()
    expected_days = (gold[5] - gold[4]).days + 1 if gold[4] else 0
    covered = (
        first is not None
        and first <= gold[4]
        and last >= gold[5]
        and days == (last - first).days + 1
    )
    daily = _gold(spark, cfg, "kpi_revenue_daily").agg(F.sum("revenue_eur")).first()[0]
    funnel = _gold(spark, cfg, "kpi_conversion_funnel")
    rising = funnel.where(
        (F.col("product_views") > F.col("visits"))
        | (F.col("carts") > F.col("product_views"))
        | (F.col("orders") > F.col("carts"))
    )
    return [
        _line("orders", silver[0], gold[0]),
        _line("revenue_eur (applicable price)", revenue, gold[1]),
        _line("invoiced_amount_eur", silver[1], gold[2]),
        *[_line(f"orders with {kind}", 0, count) for kind, count in broken.items()],
        _line(
            "dim_date covers the order dates, without gap",
            f"{gold[4]} .. {gold[5]} ({expected_days} days)",
            f"{first} .. {last} ({days} days)" if covered else f"NOT COVERED: {first} .. {last}",
        ),
        _line("kpi_revenue_daily revenue_eur = COMPLETED revenue", gold[3], daily),
        _line(
            "kpi_conversion_funnel never increases",
            [],
            [row["device"] for row in rising.select("device").collect()],
        ),
    ]


def top_products_check(spark: SparkSession, cfg: AppConfig, country: str) -> list[tuple]:
    """``(position, product, revenue)`` of a plain ``groupBy``/``orderBy`` beside ``(rank,
    product, revenue)`` of ``kpi_top_products_country``, and whether the two agree."""
    n = cfg.gold.top_n if cfg.gold else 0
    plain = (
        _gold(spark, cfg, FACT_SALES.name, FACT_SALES.schema)
        .where((F.col("status") == COMPLETED) & (F.col("country") == country))
        .groupBy("product_id")
        .agg(F.sum("revenue_eur").alias("revenue_eur"))
        .orderBy(F.col("revenue_eur").desc(), "product_id")
        .limit(n)
        .collect()
    )
    indicator = (
        _gold(spark, cfg, "kpi_top_products_country")
        .where(F.col("country") == country)
        .orderBy("rank", "product_id")
        .collect()
    )

    def agree(mine, kept) -> bool:
        if kept is None:
            return False
        if mine is None:  # a product tied with the last one the plain top N keeps
            return bool(plain) and kept["revenue_eur"] == plain[-1]["revenue_eur"]
        return (mine["product_id"], mine["revenue_eur"]) == (
            kept["product_id"],
            kept["revenue_eur"],
        )

    return [
        (
            position,
            mine["product_id"] if mine else None,
            str(mine["revenue_eur"]) if mine else None,
            kept["rank"] if kept else None,
            kept["product_id"] if kept else None,
            str(kept["revenue_eur"]) if kept else None,
            agree(mine, kept),
        )
        for position, (mine, kept) in enumerate(zip_longest(plain, indicator), start=1)
    ]


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
            _recent(kpi("kpi_revenue_daily"), 3).orderBy(ORDER_DATE, "country"),
        ),
        (
            "kpi_cancel_return_rate summed per country and day, last 2 dates",
            _recent(kpi("kpi_cancel_return_rate"), 1)
            .groupBy(ORDER_DATE, "country")
            .agg(*[F.sum(c).alias(c) for c in ("lines", "cancelled", "returned")])
            .select("*", _share("cancelled", "lines"), _share("returned", "lines"))
            .orderBy(ORDER_DATE, "country"),
        ),
        (
            "kpi_avg_basket summed per country and day, last 2 dates",
            _recent(kpi("kpi_avg_basket"), 1)
            .groupBy(ORDER_DATE, "country")
            .agg(F.sum("orders").alias("orders"), F.sum("revenue_eur").alias("revenue_eur"))
            .select("*", F.round(F.col("revenue_eur") / F.col("orders"), 2).alias("avg_basket_eur"))
            .orderBy(ORDER_DATE, "country"),
        ),
        (
            f"kpi_avg_basket, last date, {SAMPLE_COUNTRY}",
            _recent(kpi("kpi_avg_basket"), 0)
            .where(F.col("country") == SAMPLE_COUNTRY)
            .orderBy("segment"),
        ),
        (
            "kpi_http_errors_hourly, 8 hours with the highest 4xx rate",
            kpi("kpi_http_errors_hourly").orderBy(F.col("rate_4xx").desc(), "event_hour").limit(8),
        ),
        ("kpi_conversion_funnel", kpi("kpi_conversion_funnel").orderBy("device")),
        (
            "kpi_data_quality",
            kpi("kpi_data_quality").orderBy(INGESTION_DATE, "source", "disposition", "code"),
        ),
    ]


def _recent(frame: DataFrame, days_before: int) -> DataFrame:
    """The rows of the last ``order_date`` and of the ``days_before`` days before it."""
    last = frame.agg(F.max(ORDER_DATE).alias("_last"))
    return (
        frame.crossJoin(last)
        .where(F.col(ORDER_DATE) >= F.date_sub("_last", days_before))
        .drop("_last")
    )


def _share(part: str, whole: str):
    return F.round(F.col(part) / F.col(whole), 4).alias(f"{part}_rate")


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
        totals = reconciliation(spark, cfg)
        spark.createDataFrame(
            totals, "check string, silver string, gold string, status string"
        ).show(50, truncate=False)
        violated += [line for line in totals if line[3] != "ok"]
        top = top_products_check(spark, cfg, SAMPLE_COUNTRY)
        print(f"== top products of {SAMPLE_COUNTRY}: plain groupBy/orderBy beside the indicator")
        spark.createDataFrame(
            top,
            "position int, plain_product string, plain_revenue string, kpi_rank int,"
            " kpi_product string, kpi_revenue string, same boolean",
        ).show(50, truncate=False)
        violated += [line for line in top if not line[-1]]
        contents(spark, cfg).show(50, truncate=False)
        for frame in price_history(spark, cfg):
            frame.show(20, truncate=False)
        for title, frame in samples(spark, cfg):
            print(f"== {title}")
            frame.show(60, truncate=False)
        runs(spark, cfg).show(100, truncate=False)
        print(f"{mismatches} order dates mismatched, {len(violated)} checks failed")
        if args.explain:
            explain(spark, cfg, args.explain)
    finally:
        spark.stop()
    return 1 if mismatches or violated else 0


if __name__ == "__main__":
    sys.exit(main())
