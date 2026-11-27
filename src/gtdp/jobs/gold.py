"""Gold job: every gold table built from the days silver completed and checked, then published;
``fact_sales`` only for the order dates whose rows changed."""

from __future__ import annotations

import dataclasses
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
from gtdp.gold.state import State, Status, write_state
from gtdp.gold.tables import (
    DIM_CUSTOMER,
    DIM_DATE,
    DIM_PRODUCT,
    FACT_SALES,
    FACT_WEB_EVENTS,
    ORDER_DATE,
    STAR,
    table_path,
)
from gtdp.io import INGESTION_DATE, drop_partitions, read_table, table_lock
from gtdp.jobs.registry import JobContext, job
from gtdp.publish import publish
from gtdp.schemas import SILVER_REJECTS
from gtdp.silver.days import complete_days, published_sources, rebuilt_day
from gtdp.silver.pipelines import PIPELINES, rejects_path
from gtdp.silver.pipelines import table_path as silver_path


class GoldCheckError(RuntimeError):
    """The gold built from silver breaks an invariant, or differs from it once published."""


class IncompleteSilverError(RuntimeError):
    """A silver table rebuilt whole holds rows of a day silver has not completed, or was rebuilt
    for one."""


@job(
    "gold",
    Layer.GOLD,
    "Rebuild gold from every day silver completed, --date among them: fact_sales published for"
    " the order dates that changed, the dimensions, fact_web_events and the indicators whole",
)
def gold(ctx: JobContext) -> None:
    """Publish the gold tables for ``--date``.

    Gold reads the days that silver completed (``gtdp.silver.days``) and needs ``--date`` among
    them. ``silver/customers`` and ``silver/products_scd2`` hold no partition to leave out: gold
    refuses to run while the rows it read from them carry a day silver has not completed, or the
    table was rebuilt for one (``_check_whole_tables``). Every table is built from those days in
    full: whatever the order of the runs, the same complete days give the same gold.
    Nothing is written before every table is built and every invariant holds: an order with no
    catalog version or several, a duplicate key, a null in a required column, a fact pointing at
    no dimension row. Then ``fact_sales`` is published for the order dates whose rows differ from
    the published ones, a published date no order holds any more is removed, and the other tables
    are replaced whole. ``fact_sales`` is read back: it must hold exactly the rows built. The run
    holds the lock of the whole gold layer (``_gold.lock`` beside the gold directory) from the
    end of its check of silver to its end: a second run fails there, before building anything.
    ``gold/_state.json`` says ``PUBLISHING`` from the first write until the read-back holds, then
    ``PUBLISHED`` (``gtdp.gold.state``).
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
    silver = {
        source: read_table(ctx.spark, silver_path(ctx.cfg.paths.silver, source), pipeline.schema)
        for source, pipeline in PIPELINES.items()
    }
    _check_whole_tables(ctx.spark, ctx.cfg.paths.silver, silver, complete)

    with table_lock(ctx.spark, ctx.cfg.paths.gold):
        try:
            _publish(ctx, _build(ctx, silver, complete, settings), complete)
        finally:
            ctx.spark.catalog.clearCache()


def _check_whole_tables(
    spark: SparkSession, root: str, silver: dict[str, DataFrame], complete: list[date]
) -> None:
    """``IncompleteSilverError`` if a table of ``silver`` that silver rebuilds whole holds rows
    whose ``ingestion_date`` is not among the ``complete`` days, or was rebuilt for such a day.

    ``silver`` holds the DataFrames gold builds from. Spark listed their files when it read
    them: every later action reads those files or fails on a missing one, so a swap after this
    check cannot slip other rows in. The rows tell on their own, on a lake written before
    ``_built_for.json`` too: a day rebuilt into ``customers`` that adds no row of its own leaves
    the table as the day before left it. Not so for ``products_scd2``: a version sent again on a
    later day with the attributes of the version before removes a version and adds no row. Hence
    ``rebuilt_day`` too, read after the listing: it names the table listed, or a newer one whose
    swap fails the read.
    """
    whole = [source for source, pipeline in PIPELINES.items() if not pipeline.partitioned]
    held = reduce(
        DataFrame.unionByName,
        [silver[source].select(F.lit(source).alias("source"), INGESTION_DATE) for source in whole],
    )
    found: dict[date, list[str]] = {}
    for row in held.where(~F.col(INGESTION_DATE).isin(complete)).distinct().collect():
        table = PIPELINES[row["source"]].table
        found.setdefault(row[INGESTION_DATE], []).append(f"silver/{table} holds rows of it")
    for source in whole:
        built = rebuilt_day(spark, silver_path(root, source))
        if built is not None and built not in complete:
            table = PIPELINES[source].table
            found.setdefault(built, []).append(f"silver/{table} was rebuilt for it")
    if not found:
        return
    first = min(found)
    force = " --force" if any(day > first for day in complete) else ""
    later = ", ".join(str(day) for day in sorted({*found, *complete}) if day > first)
    reasons = [
        f"{day} is not complete (published: {_sources(spark, root, day)}; missing: "
        f"{_sources(spark, root, day, missing=True)}) and {' and '.join(sorted(found[day]))}"
        for day in sorted(found)
    ]
    raise IncompleteSilverError(
        f"gold reads complete silver days only, but {'; '.join(reasons)}. Run "
        f"scripts/cluster/submit.sh silver --date {first}{force}, "
        + (f"then silver for {later} in order, " if later else "")
        + "then gold again"
    )


def _sources(spark: SparkSession, silver: str, day: date, *, missing: bool = False) -> str:
    published = published_sources(spark, silver, day).keys()
    return ", ".join(sorted(PIPELINES.keys() - published if missing else published)) or "none"


def _build(
    ctx: JobContext, silver: dict[str, DataFrame], complete: list[date], settings: GoldSettings
) -> dict[str, DataFrame]:
    """Every gold table built from the ``silver`` tables, by name, persisted and checked: the
    ``complete`` days of a partitioned table, a table rebuilt whole as it is. ``GoldCheckError``
    on the first broken invariant."""
    spark, root = ctx.spark, ctx.cfg.paths.silver

    def read(source: str) -> DataFrame:
        partitioned = PIPELINES[source].partitioned
        return _of_days(silver[source], complete) if partitioned else silver[source]

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
        FACT_SALES.name: kept(facts.sales(orders, products)),
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
            _of_days(read_table(spark, rejects_path(root, source), SILVER_REJECTS), complete)
            for source in PIPELINES
        ],
    )
    inputs = Star(sales, star[FACT_WEB_EVENTS.name], customers, products, orders, rejects)
    indicators = {name: kept(build(inputs, settings)) for name, build in KPIS.items()}
    ctx.run.metrics["kpi_rows"] = {name: frame.count() for name, frame in indicators.items()}
    return star | indicators


def _publish(ctx: JobContext, tables: dict[str, DataFrame], complete: list[date]) -> None:
    """Write ``tables``, built from the ``complete`` days of silver: ``fact_sales`` for the order
    dates that changed, the others whole, between a ``PUBLISHING`` and a ``PUBLISHED`` state.
    A table of the star must have the columns of its ``Table``; an indicator has the columns its
    function in ``KPIS`` computes, which is its declaration."""
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

    _ensure_unlocked(spark, [table_path(gold, name) for name in tables])
    publishing = State(Status.PUBLISHING, ctx.run.run_id, tuple(complete))
    write_state(spark, gold, publishing)
    if rewritten:
        changed = sales.where(F.col(ORDER_DATE).isin(rewritten)).repartition(ORDER_DATE)
        publish(changed, path, FACT_SALES.schema, FACT_SALES.partitions, rewritten)
    if removed:
        drop_partitions(spark, path, {ORDER_DATE: removed})
    for table in STAR:
        if table is not FACT_SALES:
            publish(tables[table.name], table_path(gold, table.name), table.schema)
    for name in KPIS:
        publish(tables[name], table_path(gold, name), tables[name].schema)

    after = checks.fingerprints(read_table(spark, path, FACT_SALES.schema))
    ctx.run.counters.rows_out = sum(rows for rows, _ in after.values())
    if differing := sorted(
        day for day in built.keys() | after.keys() if built.get(day) != after.get(day)
    ):
        raise GoldCheckError(f"fact_sales differs from the rows built from silver on {differing}")
    write_state(spark, gold, dataclasses.replace(publishing, status=Status.PUBLISHED))


def _ensure_unlocked(spark: SparkSession, paths: list[str]) -> None:
    """``TableLockedError`` if another run holds the lock of a table at ``paths``, such as a run
    that died while writing it: checked before the first write, so a run that cannot write every
    table writes none."""
    for path in paths:
        with table_lock(spark, path):
            pass


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
