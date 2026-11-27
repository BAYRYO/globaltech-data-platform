"""The Lot 7 benchmark tables, built from a generator landing, and the queries the benchmarks time.

``prepare`` turns the raw order, customer and product files into typed tables: the valid order
lines (the Lot 6 rules of ``INVALID_RULES``), one country per customer, the current category of
each product and the SCD2 versions of the catalogue. It writes the same tables once per
``Layout``, so every layout holds the same rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from gtdp.io import read_table
from gtdp.perf.revenue_by_country import INVALID_RULES, ORDER_COLUMNS, customer_countries

ORDERS = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("event_time", TimestampType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price_eur", DecimalType(10, 2)),
        StructField("channel", StringType()),
        StructField("status", StringType()),
        StructField("discount_pct", DecimalType(4, 3)),
        StructField("order_date", DateType()),
    ]
)
CUSTOMERS = StructType(
    [StructField("customer_id", StringType()), StructField("country", StringType())]
)
PRODUCTS = StructType(
    [StructField("product_id", StringType()), StructField("category", StringType())]
)
VERSIONS = StructType(
    [
        StructField("product_id", StringType()),
        StructField("product_sk", LongType()),
        StructField("unit_price_eur", DecimalType(10, 2)),
        StructField("valid_from", TimestampType()),
        StructField("valid_to", TimestampType()),
    ]
)
SCHEMAS = {"orders": ORDERS, "customers": CUSTOMERS, "products": PRODUCTS}
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"
OPEN_END = "9999-12-31 00:00:00"
PRODUCT_COLUMNS = ("product_id", "product_name", "category", "unit_price_eur", "updated_at")


@dataclass(frozen=True)
class Layout:
    """How a copy of the tables is stored: format, compression, and ``orders`` partitioned by
    ``order_date`` or not."""

    name: str
    fmt: str
    codec: str
    partitioned: bool


LAYOUTS = (
    Layout("csv", "csv", "none", False),
    Layout("parquet", "parquet", "snappy", False),
    Layout("parquet_part", "parquet", "snappy", True),
    Layout("parquet_part_zstd", "parquet", "zstd", True),
)
LAYOUT = {layout.name: layout for layout in LAYOUTS}


def valid_orders(lines: DataFrame, countries: DataFrame) -> DataFrame:
    """Typed ``ORDERS`` rows of the raw order lines (column ``value``) that pass every rule of
    ``INVALID_RULES``; ``countries`` (``customer_countries``) decides UNKNOWN_CUSTOMER. An absent
    or empty ``discount_pct`` is 0."""
    fields = F.split("value", ",", -1)
    parsed = lines.select(
        F.size(fields).alias("width"),
        *[F.get(fields, index).alias(name) for index, name in enumerate(ORDER_COLUMNS)],
    )
    failing = F.coalesce(*[F.when(fails(), F.lit(code)) for code, fails in INVALID_RULES])
    event_time = F.col("event_time").cast(TimestampType())
    return (
        parsed.join(F.broadcast(countries), "customer_id", "left")
        .where(failing.isNull())
        .select(
            "transaction_id",
            event_time.alias("event_time"),
            "customer_id",
            "product_id",
            F.col("quantity").cast(IntegerType()).alias("quantity"),
            F.col("unit_price_eur").cast(DecimalType(10, 2)).alias("unit_price_eur"),
            "channel",
            "status",
            F.coalesce(
                F.expr("try_cast(discount_pct AS DECIMAL(4,3))"), F.lit(Decimal("0.000"))
            ).alias("discount_pct"),
            F.to_date(event_time).alias("order_date"),
        )
    )


def product_versions(lines: DataFrame) -> DataFrame:
    """The valid catalogue versions of the raw product lines (column ``value``): ``product_id``,
    ``updated_at``, ``unit_price_eur``, ``category``. A version needs 5 fields, a positive price,
    a category and a timestamp; of two versions with the same (``product_id``, ``updated_at``),
    the higher price stays."""
    fields = F.split("value", ",", -1)
    parsed = lines.select(
        F.size(fields).alias("width"),
        *[F.get(fields, index).alias(name) for index, name in enumerate(PRODUCT_COLUMNS)],
    ).select(
        "width",
        "product_id",
        "category",
        F.expr("try_cast(unit_price_eur AS DECIMAL(10,2))").alias("unit_price_eur"),
        F.expr("try_cast(updated_at AS TIMESTAMP)").alias("updated_at"),
    )
    valid = parsed.where(
        (F.col("width") == len(PRODUCT_COLUMNS))
        & (F.col("unit_price_eur") > 0)
        & (F.coalesce("category", F.lit("")) != "")
        & F.col("updated_at").isNotNull()
    )
    latest = F.max(F.struct("unit_price_eur", "category"))
    return (
        valid.groupBy("product_id", "updated_at")
        .agg(latest.alias("v"))
        .select("product_id", "updated_at", "v.unit_price_eur", "v.category")
    )


def scd2(versions: DataFrame) -> DataFrame:
    """``VERSIONS`` rows of ``product_versions``: each version is valid from its ``updated_at`` to
    the next one of its product, the last one with no end."""
    following = F.lead("updated_at").over(Window.partitionBy("product_id").orderBy("updated_at"))
    return versions.select(
        "product_id",
        F.xxhash64("product_id", "updated_at").alias("product_sk"),
        "unit_price_eur",
        F.col("updated_at").alias("valid_from"),
        F.coalesce(following, F.lit(OPEN_END).cast(TimestampType())).alias("valid_to"),
    )


def current_categories(versions: DataFrame) -> DataFrame:
    """``PRODUCTS`` rows: the category of the latest version of each product."""
    return (
        versions.groupBy("product_id")
        .agg(F.max_by("category", "updated_at").alias("category"))
        .select("product_id", "category")
    )


def prepare(
    spark: SparkSession, landing: str, root: str, layouts: tuple[Layout, ...] = LAYOUTS
) -> dict[str, int]:
    """Write the tables of every layout under ``root/<layout>`` and the SCD2 versions under
    ``root/versions``, from the ``orders``, ``customers`` and ``products`` files of ``landing``
    (every ``ingestion_date``). Return the row count of each table as read back, per layout."""
    countries = customer_countries(spark.read.text(f"{landing}/customers"))
    versions = product_versions(spark.read.text(f"{landing}/products"))
    tables = {
        "orders": valid_orders(spark.read.text(f"{landing}/orders"), countries),
        "customers": countries,
        "products": current_categories(versions),
    }
    for layout in layouts:
        for name, frame in tables.items():
            write(frame, f"{root}/{layout.name}/{name}", layout, partitioned=name == "orders")
    scd2(versions).write.mode("overwrite").parquet(f"{root}/versions")
    counts = {
        f"{layout.name}/{name}": read(spark, root, layout, name).count()
        for layout in layouts
        for name in tables
    }
    counts["versions"] = read_table(spark, f"{root}/versions", VERSIONS).count()
    return counts


def write(frame: DataFrame, path: str, layout: Layout, *, partitioned: bool) -> None:
    """Write ``frame`` to ``path`` in ``layout``; ``partitioned`` tables of a partitioned layout
    get one directory per ``order_date`` holding one file."""
    writer = frame.write.mode("overwrite")
    if layout.fmt == "csv":
        writer.option("header", True).option("timestampFormat", TIMESTAMP_FORMAT).csv(path)
        return
    if layout.partitioned and partitioned:
        writer = frame.repartition("order_date").write.mode("overwrite").partitionBy("order_date")
    writer.option("compression", layout.codec).parquet(path)


def read(spark: SparkSession, root: str, layout: Layout, name: str) -> DataFrame:
    """The table ``name`` of ``layout`` under ``root``, read with its schema (no inference)."""
    path, schema = f"{root}/{layout.name}/{name}", SCHEMAS[name]
    if layout.fmt == "csv":
        return (
            spark.read.schema(schema)
            .option("header", True)
            .option("timestampFormat", TIMESTAMP_FORMAT)
            .csv(path)
        )
    return read_table(spark, path, schema)


def amount() -> Column:
    """The exact invoiced amount of a line: ``quantity * unit_price_eur * (1 - discount_pct)``."""
    return F.col("quantity") * F.col("unit_price_eur") * (F.lit(Decimal(1)) - F.col("discount_pct"))


def revenue_by_day(
    orders: DataFrame, customers: DataFrame, products: DataFrame, first: date, last: date
) -> DataFrame:
    """Completed revenue and lines per ``order_date``, ``country`` and ``category`` from ``first``
    to ``last``: the KAN-44 query. The date filter lets a partitioned table skip partitions."""
    return (
        orders.where(F.col("order_date").between(first, last) & (F.col("status") == "COMPLETED"))
        .join(customers, "customer_id")
        .join(products, "product_id")
        .groupBy("order_date", "country", "category")
        .agg(F.round(F.sum(amount()), 2).alias("revenue_eur"), F.count(F.lit(1)).alias("lines"))
    )


def priced_check(frame: DataFrame) -> list:
    """A one-row check of a priced join: rows, rows with a version, and revenue at the applicable
    price, ``quantity * applicable_unit_price * (1 - discount_pct)``."""
    revenue = F.col("quantity") * F.col("applicable_unit_price") * (1 - F.col("discount_pct"))
    row = frame.agg(
        F.count(F.lit(1)).alias("rows"),
        F.count("product_sk").alias("priced"),
        F.sum(revenue).alias("revenue"),
    ).first()
    return [row["rows"], row["priced"], str(row["revenue"])]
