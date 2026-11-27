"""Gold job: ``fact_sales`` built from every day silver completed, published for the order dates
whose rows changed; the dimensions, ``fact_web_events`` and the indicators rebuilt whole."""

from __future__ import annotations

from datetime import date
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, StructType

from gtdp.audit import Layer
from gtdp.config import ConfigError
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
from gtdp.io import INGESTION_DATE, drop_partitions, read_table, replace_table, write_partitions
from gtdp.jobs.registry import JobContext, job
from gtdp.schemas import SILVER_REJECTS
from gtdp.silver.days import complete_days
from gtdp.silver.pipelines import PIPELINES, rejects_path
from gtdp.silver.pipelines import table_path as silver_path


class GoldCheckError(RuntimeError):
    """An order breaks the grain of ``fact_sales``, or the published star breaks an invariant."""


@job(
    "gold",
    Layer.GOLD,
    "Rebuild gold from every day silver completed, --date among them: fact_sales published for"
    " the order dates that changed, the dimensions, fact_web_events and the indicators whole",
)
def gold(ctx: JobContext) -> None:
    """Publish the gold tables for ``--date``.

    Gold reads the days that silver completed (``gtdp.silver.days``) and needs ``--date`` among
    them. ``fact_sales`` is built from every silver order of those days: an order date whose rows
    differ from the published ones is rewritten, a published date that no order holds any more is
    removed, the others stay as they are. Whatever the order of the runs, the same complete days
    give the same gold. Dimensions, ``fact_web_events`` and indicators are rebuilt from the whole
    of silver. Fails before writing ``fact_sales`` when an order matches no catalog version,
    several, or no customer; fails after writing when the star breaks a key or a reference,
    before the indicators are rebuilt.
    """
    day = ctx.run.ingestion_date
    if day is None:
        raise ConfigError("gold needs --date")
    settings = ctx.cfg.gold
    if settings is None:
        raise ConfigError("the configuration has no [gold] section")
    spark, silver = ctx.spark, ctx.cfg.paths.silver
    complete = complete_days(spark, silver)
    if day not in complete:
        raise FileNotFoundError(f"silver has not completed {day}: run silver for {day} first")
    ctx.run.metrics["silver_days"] = [str(d) for d in complete]

    def publish(table: Table, frame: DataFrame) -> DataFrame:
        path = table_path(ctx.cfg.paths.gold, table.name)
        replace_table(frame, path)
        return read_table(spark, path, table.schema)

    def read(source: str) -> DataFrame:
        table = read_table(spark, silver_path(silver, source), PIPELINES[source].schema)
        return _of_days(table, complete) if PIPELINES[source].partitioned else table

    orders, weblogs = read("orders"), read("weblogs")
    customers = publish(DIM_CUSTOMER, dimensions.customers(read("customers")))
    products = publish(DIM_PRODUCT, dimensions.products(read("products")))
    days = orders.select(facts.order_date().alias("date")).unionByName(
        weblogs.select(F.to_date("event_ts").alias("date"))
    )
    calendar = publish(DIM_DATE, dimensions.calendar(days))
    sales = _publish_sales(ctx, orders, products, customers)
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
        [
            _of_days(read_table(spark, rejects_path(silver, source), SILVER_REJECTS), complete)
            for source in PIPELINES
        ],
    )
    inputs = Star(sales, web_events, customers, products, orders, rejects)
    kpi_rows: dict[str, int] = {}
    ctx.run.metrics["kpi_rows"] = kpi_rows
    for name, build in KPIS.items():
        path = table_path(ctx.cfg.paths.gold, name)
        replace_table(build(inputs, settings), path)
        kpi_rows[name] = read_table(spark, path).count()


def _of_days(table: DataFrame, days: list[date]) -> DataFrame:
    """The ``ingestion_date`` partitions of ``table`` among ``days``."""
    return table.where(F.col(INGESTION_DATE).isin(days))


def _publish_sales(
    ctx: JobContext, orders: DataFrame, products: DataFrame, customers: DataFrame
) -> DataFrame:
    """Build ``fact_sales`` from every order of ``orders``, publish the order dates whose rows
    differ from the published table, remove the dates no order holds any more; return the table.
    """
    spark, path = ctx.spark, table_path(ctx.cfg.paths.gold, FACT_SALES.name)
    rows = facts.sales(orders, products, customers).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        ctx.run.counters.rows_in = orders_in = orders.count()
        ctx.run.metrics["build_violations"] = broken = facts.build_violations(rows, orders_in)
        if failed := {kind: count for kind, count in broken.items() if count}:
            raise GoldCheckError(f"silver orders break the grain of fact_sales: {failed}")
        built, published = checks.fingerprints(rows), _published_fingerprints(spark, path)
        rewritten = sorted(day for day, print_ in built.items() if published.get(day) != print_)
        removed = sorted(published.keys() - built.keys())
        ctx.run.metrics.update(
            {
                "dates_rewritten": len(rewritten),
                "dates_removed": len(removed),
                "dates_unchanged": len(built) - len(rewritten),
            }
        )
        if rewritten:
            changed = rows.where(F.col(ORDER_DATE).isin(rewritten)).repartition(ORDER_DATE)
            write_partitions(changed, path, {ORDER_DATE: rewritten})
        if removed:
            drop_partitions(spark, path, {ORDER_DATE: removed})
    finally:
        rows.unpersist()

    sales = read_table(spark, path, FACT_SALES.schema)
    after = checks.fingerprints(sales)
    ctx.run.counters.rows_out = sum(count for count, _ in after.values())
    if differing := sorted(
        day for day in built.keys() | after.keys() if built.get(day) != after.get(day)
    ):
        raise GoldCheckError(f"fact_sales differs from the rows built from silver on {differing}")
    return sales


def _published_fingerprints(spark: SparkSession, path: str) -> dict[date, object]:
    """``checks.fingerprints`` of ``fact_sales`` as published. Every date of a table written with
    other columns or types than ``FACT_SALES`` (an earlier version of gold) gets ``None``, a
    fingerprint no build matches."""
    try:
        published = read_table(spark, path)
    except FileNotFoundError:
        return {}
    if _columns(published.schema) != _columns(FACT_SALES.schema):
        return {row[0]: None for row in published.select(ORDER_DATE).distinct().collect()}
    return checks.fingerprints(published)


def _columns(schema: StructType) -> list[tuple[str, DataType]]:
    return [(field.name, field.dataType) for field in schema.fields]
