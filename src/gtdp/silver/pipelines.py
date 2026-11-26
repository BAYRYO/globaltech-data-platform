"""What silver publishes for each source, and the classified bronze records of a day."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from gtdp.config import LakePaths
from gtdp.io import INGESTION_DATE, read_table
from gtdp.schemas import (
    SILVER_CUSTOMERS,
    SILVER_ORDERS,
    SILVER_PRODUCTS_SCD2,
    SILVER_WEBLOGS,
    SOURCES,
    TECHNICAL_COLUMNS,
    bronze_schema,
)
from gtdp.silver import customers, orders, products, weblogs
from gtdp.silver.quality import Entity, classify, with_references


@dataclass(frozen=True)
class Pipeline:
    entity: Entity
    table: str
    """Directory of the table under the silver root."""
    schema: StructType
    build: Callable[[DataFrame, DataFrame], DataFrame]
    """(clean records, salt) -> the rows of the table."""
    key: tuple[str, ...]
    """Columns unique across the whole table, checked after each write."""
    partitioned: bool
    """True: day D replaces its own ``ingestion_date`` partition, built from bronze(D)
    (ADR-0005). False: the table is rebuilt from bronze up to D and swapped whole."""
    cross_day: bool = False
    """A key already in a partition before D is a cross-day duplicate (ADR-0005)."""
    checks: Callable[[DataFrame], dict[str, int]] | None = None
    """Further invariants of the written table: rows breaking each one."""


PIPELINES: Mapping[str, Pipeline] = MappingProxyType(
    {
        "orders": Pipeline(
            orders.ENTITY,
            "orders",
            SILVER_ORDERS,
            orders.table,
            ("transaction_id",),
            partitioned=True,
            cross_day=True,
        ),
        "customers": Pipeline(
            customers.ENTITY,
            "customers",
            SILVER_CUSTOMERS,
            customers.table,
            ("customer_id",),
            partitioned=False,
        ),
        "products": Pipeline(
            products.ENTITY,
            "products_scd2",
            SILVER_PRODUCTS_SCD2,
            products.table,
            ("product_id", "valid_from"),
            partitioned=False,
            checks=products.violations,
        ),
        "weblogs": Pipeline(
            weblogs.ENTITY,
            "weblogs",
            SILVER_WEBLOGS,
            weblogs.table,
            ("log_id",),
            partitioned=True,
        ),
    }
)


def table_path(silver: str, source: str) -> str:
    return f"{silver}/{PIPELINES[source].table}"


def rejects_path(silver: str, source: str) -> str:
    return f"{silver}/rejects/{source}"


def bronze_records(
    spark: SparkSession, bronze: str, source: str, day: date, *, history: bool
) -> DataFrame:
    """The records of ``bronze/<source>`` for ``day``, or up to ``day`` with ``history``.

    Every canonical column is there; one that a file lacks, such as ``discount_pct`` before J3,
    is null (ADR-0007).
    """
    table = read_table(
        spark, f"{bronze}/{source}", bronze_schema(SOURCES[source]), merge_schema=True
    )
    day_column = F.col(INGESTION_DATE)
    return table.where(day_column <= F.lit(day) if history else day_column == F.lit(day))


def file_columns(spark: SparkSession, bronze: str, source: str, day: date) -> list[str]:
    """The data columns of the bronze partition of ``day``, in file order, as bronze kept them
    from the files' headers: a missing canonical column is absent, an unknown one present."""
    try:
        partition = read_table(
            spark, f"{bronze}/{source}/{INGESTION_DATE}={day}", merge_schema=True
        )
    except FileNotFoundError:  # a day without records
        return []
    technical = set(TECHNICAL_COLUMNS.fieldNames())
    return [column for column in partition.columns if column not in technical]


def classified(spark: SparkSession, paths: LakePaths, source: str, day: date) -> DataFrame:
    """The bronze records that silver reads for ``source`` and ``day``, classified.

    A partitioned table reads bronze(D) alone; another reads every day up to D, classified day by
    day, to rebuild the table.
    """
    pipeline = PIPELINES[source]
    records = bronze_records(spark, paths.bronze, source, day, history=not pipeline.partitioned)
    identifiers = {
        column: bronze_records(spark, paths.bronze, reference, day, history=True)
        for column, reference in pipeline.entity.references.items()
    }
    earlier = None
    if pipeline.cross_day:
        published = read_table(spark, table_path(paths.silver, source), pipeline.schema)
        earlier = published.where(F.col(INGESTION_DATE) < F.lit(day))
    return classify(with_references(records, identifiers), pipeline.entity, earlier)
