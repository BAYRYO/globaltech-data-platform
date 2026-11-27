"""Gold job: every gold table built from the days silver completed and checked, then published;
``fact_sales`` only for the order dates whose rows changed."""

from __future__ import annotations

from datetime import date
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, StructType

from gtdp.audit import Layer
from gtdp.config import ConfigError, GoldSettings
from gtdp.gold import checks, dimensions, facts
from gtdp.gold.kpis import KPIS, Star
from gtdp.gold.tables import (
    DIM_CUSTOMER,
    DIM_DATE,
    DIM_PRODUCT,
    FACT_SALES,
    FACT_WEB_EVENTS,
    ORDER_DATE,
    table_path,
)
from gtdp.io import INGESTION_DATE, drop_partitions, read_table, replace_table, write_partitions
from gtdp.jobs.registry import JobContext, job
from gtdp.schemas import SILVER_REJECTS
from gtdp.silver.days import complete_days
from gtdp.silver.pipelines import PIPELINES, rejects_path
from gtdp.silver.pipelines import table_path as silver_path


class GoldCheckError(RuntimeError):
    """The gold built from silver breaks an invariant, or differs from it once published."""


@job(
    "gold",
    Layer.GOLD,
    "Rebuild gold from every day silver completed, --date among them: fact_sales published for"
    " the order dates that changed, the dimensions, fact_web_events and the indicators whole",
)
def gold(ctx: JobContext) -> None:
    """Publish the gold tables for ``--date``.

    Gold reads the days that silver completed (``gtdp.silver.days``) and needs ``--date`` among
    them. Every table is built from those days in full: whatever the order of the runs, the same
    complete days give the same gold. Nothing is written before every table is built and every
    invariant holds: an order with no catalog version or several, a duplicate key, a null in a
    required column, a fact pointing at no dimension row. Then ``fact_sales`` is published for the
    order dates whose rows differ from the published ones, a published date no order holds any
    more is removed, and the other tables are replaced whole. ``fact_sales`` is read back: it must
    hold exactly the rows built.
    """
    day = ctx.run.ingestion_date
    if day is None:
        raise ConfigError("gold needs --date")
    settings = ctx.cfg.gold
    if settings is None:
        raise ConfigError("the configuration has no [gold] section")
    complete = complete_days(ctx.spark, ctx.cfg.paths.silver)
    if day not in complete:
        raise FileNotFoundError(f"silver has not completed {day}: run silver for {day} first")
    ctx.run.metrics["silver_days"] = [str(d) for d in complete]

    try:
        _publish(ctx, _build(ctx, complete, settings))
    finally:
        ctx.spark.catalog.clearCache()


def _build(ctx: JobContext, complete: list[date], settings: GoldSettings) -> dict[str, DataFrame]:
    """Every gold table built from the ``complete`` days of silver, by name, persisted and
    checked; ``GoldCheckError`` on the first broken invariant."""
    spark, silver = ctx.spark, ctx.cfg.paths.silver

    def read(source: str) -> DataFrame:
        table = read_table(spark, silver_path(silver, source), PIPELINES[source].schema)
        return _of_days(table, complete) if PIPELINES[source].partitioned else table

    def kept(frame: DataFrame) -> DataFrame:
        return frame.persist(StorageLevel.MEMORY_AND_DISK)

    orders, weblogs = read("orders"), read("weblogs")
    customers = kept(dimensions.customers(read("customers")))
    products = kept(dimensions.products(read("products")))
    days = orders.select(facts.order_date().alias("date")).unionByName(
        weblogs.select(F.to_date("event_ts").alias("date"))
    )
    star = {
        DIM_DATE.name: kept(dimensions.calendar(days)),
        DIM_CUSTOMER.name: customers,
        DIM_PRODUCT.name: products,
        FACT_SALES.name: kept(facts.sales(orders, products, customers)),
        FACT_WEB_EVENTS.name: kept(facts.web_events(weblogs)),
    }
    ctx.run.counters.rows_in = orders_in = orders.count()
    ctx.run.metrics["table_rows"] = {name: frame.count() for name, frame in star.items()}
    sales = star[FACT_SALES.name]
    ctx.run.metrics["build_violations"] = broken = facts.build_violations(sales, orders_in)
    ctx.run.metrics["checks"] = invariants = checks.violations(star)
    ctx.run.metrics["unmatched"] = checks.optional_unmatched(star)
    if failed := {name: rows for name, rows in (broken | invariants).items() if rows}:
        raise GoldCheckError(f"the gold built from silver breaks its invariants: {failed}")

    rejects = reduce(
        DataFrame.unionByName,
        [
            _of_days(read_table(spark, rejects_path(silver, source), SILVER_REJECTS), complete)
            for source in PIPELINES
        ],
    )
    inputs = Star(sales, star[FACT_WEB_EVENTS.name], customers, products, orders, rejects)
    indicators = {name: kept(build(inputs, settings)) for name, build in KPIS.items()}
    ctx.run.metrics["kpi_rows"] = {name: frame.count() for name, frame in indicators.items()}
    return star | indicators


def _publish(ctx: JobContext, tables: dict[str, DataFrame]) -> None:
    """Write ``tables``: ``fact_sales`` for the order dates that changed, the others whole."""
    spark, gold = ctx.spark, ctx.cfg.paths.gold
    path = table_path(gold, FACT_SALES.name)
    sales = tables[FACT_SALES.name]
    built, published = checks.fingerprints(sales), _published_fingerprints(spark, path)
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
        changed = sales.where(F.col(ORDER_DATE).isin(rewritten)).repartition(ORDER_DATE)
        write_partitions(changed, path, {ORDER_DATE: rewritten})
    if removed:
        drop_partitions(spark, path, {ORDER_DATE: removed})
    for name, frame in tables.items():
        if name != FACT_SALES.name:
            replace_table(frame, table_path(gold, name))

    after = checks.fingerprints(read_table(spark, path, FACT_SALES.schema))
    ctx.run.counters.rows_out = sum(rows for rows, _ in after.values())
    if differing := sorted(
        day for day in built.keys() | after.keys() if built.get(day) != after.get(day)
    ):
        raise GoldCheckError(f"fact_sales differs from the rows built from silver on {differing}")


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


def _of_days(table: DataFrame, days: list[date]) -> DataFrame:
    """The ``ingestion_date`` partitions of ``table`` among ``days``."""
    return table.where(F.col(INGESTION_DATE).isin(days))
