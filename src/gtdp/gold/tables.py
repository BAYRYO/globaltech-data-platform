"""The tables of the star schema, their keys and the references between them.

Each table lives at ``<paths.gold>/<name>``. ``fact_sales`` is partitioned by ``order_date``; the
other tables, indicators included, are rebuilt whole by each run.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from gtdp.schemas import AMOUNT, DISCOUNT, QUALITY_FLAGS

ORDER_DATE = "order_date"
AMOUNT_EUR = DecimalType(12, 2)
"""A line amount: ``quantity * unit_price * (1 - discount_pct)`` rounded half up to the cent."""


@dataclass(frozen=True)
class Table:
    name: str
    schema: StructType
    key: tuple[str, ...]
    """Columns unique across the whole table, checked by each run."""


@dataclass(frozen=True)
class Reference:
    """``table.column`` points at the key of ``target``.

    A ``required`` reference has no orphan: every non-null value is a key of ``target``, checked
    by each run. Otherwise unmatched values are counted and reported only.
    """

    table: Table
    column: str
    target: Table
    required: bool = True

    @property
    def name(self) -> str:
        return f"{self.table.name}.{self.column}"


DIM_DATE = Table(
    "dim_date",
    StructType(
        [
            StructField("date_key", IntegerType(), False),
            StructField("date", DateType(), False),
            StructField("year", IntegerType(), False),
            StructField("quarter", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("iso_week", IntegerType(), False),
            StructField("day_of_week", IntegerType(), False),
            StructField("is_weekend", BooleanType(), False),
        ]
    ),
    ("date_key",),
)

DIM_CUSTOMER = Table(
    "dim_customer",
    StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("country", StringType(), False),
            StructField("region", StringType()),
            StructField("segment", StringType()),
            StructField("signup_date", DateType()),
            StructField("email_hash", StringType()),
            StructField(QUALITY_FLAGS, ArrayType(StringType()), False),
        ]
    ),
    ("customer_id",),
)

DIM_PRODUCT = Table(
    "dim_product",
    StructType(
        [
            StructField("product_sk", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType()),
            StructField("category", StringType(), False),
            StructField("unit_price_eur", AMOUNT, False),
            StructField("valid_from", TimestampType(), False),
            StructField("valid_to", TimestampType(), False),
            StructField("is_current", BooleanType(), False),
            StructField("version", IntegerType(), False),
        ]
    ),
    ("product_sk",),
)

FACT_SALES = Table(
    "fact_sales",
    StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("date_key", IntegerType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_sk", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("country", StringType(), False),
            StructField("channel", StringType(), False),
            StructField("status", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price_invoiced", AMOUNT, False),
            StructField("applicable_unit_price", AMOUNT, False),
            StructField("discount_pct", DISCOUNT, False),
            StructField("revenue_eur", AMOUNT_EUR, False),
            StructField("invoiced_amount_eur", AMOUNT_EUR, False),
            StructField(ORDER_DATE, DateType(), False),
        ]
    ),
    ("transaction_id",),
)

FACT_WEB_EVENTS = Table(
    "fact_web_events",
    StructType(
        [
            StructField("log_id", StringType(), False),
            StructField("event_ts", TimestampType(), False),
            StructField("date_key", IntegerType(), False),
            StructField("customer_id", StringType()),
            StructField("event_type", StringType(), False),
            StructField("product_id", StringType()),
            StructField("http_status", IntegerType(), False),
            StructField("device", StringType()),
        ]
    ),
    ("log_id",),
)

STAR = (DIM_DATE, DIM_CUSTOMER, DIM_PRODUCT, FACT_SALES, FACT_WEB_EVENTS)

REFERENCES = (
    Reference(FACT_SALES, "date_key", DIM_DATE),
    Reference(FACT_SALES, "customer_id", DIM_CUSTOMER),
    Reference(FACT_SALES, "product_sk", DIM_PRODUCT),
    Reference(FACT_WEB_EVENTS, "date_key", DIM_DATE),
    # Silver does not check the customer of a log line against the customer file: the data model
    # makes this link optional (0 or 1 customer), so unmatched values are reported only.
    Reference(FACT_WEB_EVENTS, "customer_id", DIM_CUSTOMER, required=False),
)


def table_path(gold: str, name: str) -> str:
    return f"{gold}/{name}"


def date_key(day: Column) -> Column:
    """``yyyyMMdd`` of a date as an integer: ``2025-12-30`` is ``20251230``."""
    return F.date_format(day, "yyyyMMdd").cast("int")
