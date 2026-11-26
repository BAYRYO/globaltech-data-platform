"""Gold job: ``fact_sales`` refreshed for the order dates of ``--date``, then the dimensions,
``fact_web_events`` and the indicators rebuilt whole from silver."""

from __future__ import annotations

from datetime import date
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gtdp.audit import Layer
from gtdp.gold import checks, dimensions, facts
from gtdp.gold.kpis import KPIS, Star
from gtdp.gold.tables import (
    DIM_CUSTOMER,
    DIM_DATE,
    DIM_PRODUCT,
    FACT_SALES,
    FACT_WEB_EVENTS,
    ORDER_DATE,
    Table,
    table_path,
)
from gtdp.io import INGESTION_DATE, path_exists, read_table, replace_table, write_partitions
from gtdp.jobs.registry import JobContext, job
from gtdp.schemas import SILVER_REJECTS
from gtdp.silver.pipelines import PIPELINES, rejects_path
from gtdp.silver.pipelines import table_path as silver_path


class GoldCheckError(RuntimeError):
    """An order breaks the grain of ``fact_sales``, or the published star breaks an invariant."""


@job(
    "gold",
    Layer.GOLD,
    "Refresh fact_sales for the order dates in silver orders of --date; rebuild the dimensions,"
    " fact_web_events and the indicators",
)
def gold(ctx: JobContext) -> None:
    """Publish the gold tables for ``--date``.

    Every ``order_date`` present in ``silver/orders`` of ``--date`` is rebuilt from all the silver
    orders of that date, whatever their ``ingestion_date``, so a late order never drops the orders
    already published for its date (ADR-0002); other dates keep their rows. Dimensions,
    ``fact_web_events`` and indicators are rebuilt from the whole of silver. Fails before writing
    ``fact_sales`` when an order matches no catalog version, several, or no customer; fails after
    writing when the star breaks a key or a reference, before the indicators are rebuilt.
    """
    day = ctx.run.ingestion_date
    if day is None:
        raise ValueError("gold needs --date")
    settings = ctx.cfg.gold
    if settings is None:
        raise ValueError("the configuration has no [gold] section")
    spark, silver = ctx.spark, ctx.cfg.paths.silver
    partition = f"{silver_path(silver, 'orders')}/{INGESTION_DATE}={day}"
    if not path_exists(spark, partition):
        raise FileNotFoundError(f"{partition} not found: run silver for {day} first")

    def publish(table: Table, frame: DataFrame) -> DataFrame:
        path = table_path(ctx.cfg.paths.gold, table.name)
        replace_table(frame, path)
        return read_table(spark, path, table.schema)

    orders, weblogs = _silver(spark, silver, "orders"), _silver(spark, silver, "weblogs")
    customers = publish(DIM_CUSTOMER, dimensions.customers(_silver(spark, silver, "customers")))
    products = publish(DIM_PRODUCT, dimensions.products(_silver(spark, silver, "products")))
    days = orders.select(facts.order_date().alias("date")).unionByName(
        weblogs.select(F.to_date("event_ts").alias("date"))
    )
    calendar = publish(DIM_DATE, dimensions.calendar(days))
    sales = _refresh_sales(ctx, day, orders, products, customers)
    web_events = publish(FACT_WEB_EVENTS, facts.web_events(weblogs))

    star = {
        DIM_DATE.name: calendar,
        DIM_CUSTOMER.name: customers,
        DIM_PRODUCT.name: products,
        FACT_SALES.name: sales,
        FACT_WEB_EVENTS.name: web_events,
    }
    ctx.run.metrics["table_rows"] = {name: frame.count() for name, frame in star.items()}
    ctx.run.metrics["checks"] = broken = checks.violations(star)
    ctx.run.metrics["unmatched"] = checks.optional_unmatched(star)
    if failed := {name: rows for name, rows in broken.items() if rows}:
        raise GoldCheckError(f"the gold star breaks its invariants: {failed}")

    rejects = reduce(
        DataFrame.unionByName,
        [read_table(spark, rejects_path(silver, source), SILVER_REJECTS) for source in PIPELINES],
    )
    inputs = Star(sales, web_events, customers, products, orders, rejects)
    kpi_rows: dict[str, int] = {}
    ctx.run.metrics["kpi_rows"] = kpi_rows
    for name, build in KPIS.items():
        path = table_path(ctx.cfg.paths.gold, name)
        replace_table(build(inputs, settings), path)
        kpi_rows[name] = read_table(spark, path).count()


def _silver(spark: SparkSession, silver: str, source: str) -> DataFrame:
    return read_table(spark, silver_path(silver, source), PIPELINES[source].schema)


def _refresh_sales(
    ctx: JobContext, day: date, orders: DataFrame, products: DataFrame, customers: DataFrame
) -> DataFrame:
    """Rebuild the ``fact_sales`` partitions of the order dates of ``day``; return the table."""
    path = table_path(ctx.cfg.paths.gold, FACT_SALES.name)
    of_day = orders.where(F.col(INGESTION_DATE) == F.lit(day))
    dates = sorted(
        row[ORDER_DATE]
        for row in of_day.select(facts.order_date().alias(ORDER_DATE)).distinct().collect()
    )
    ctx.run.metrics["partitions_refreshed"] = len(dates)
    ctx.run.metrics["order_dates"] = [str(d) for d in dates]
    of_dates = orders.where(facts.order_date().isin(dates))
    rows = facts.sales(of_dates, products, customers).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        ctx.run.counters.rows_in = orders_in = of_dates.count()
        ctx.run.metrics["build_violations"] = broken = facts.build_violations(rows, orders_in)
        if failed := {kind: count for kind, count in broken.items() if count}:
            raise GoldCheckError(f"orders of {day} break the grain of fact_sales: {failed}")
        if dates:
            write_partitions(rows.repartition(ORDER_DATE), path, {ORDER_DATE: dates})
    finally:
        rows.unpersist()

    sales = read_table(ctx.spark, path, FACT_SALES.schema)
    ctx.run.counters.rows_out = sales.where(F.col(ORDER_DATE).isin(dates)).count()
    if ctx.run.counters.rows_out != ctx.run.counters.rows_in:
        raise GoldCheckError(
            f"{ctx.run.counters.rows_in} silver orders on the refreshed dates, "
            f"{ctx.run.counters.rows_out} rows in their fact_sales partitions"
        )
    return sales
