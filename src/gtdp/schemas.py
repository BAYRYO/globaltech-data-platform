"""Canonical columns of the batch sources and the shape of their bronze tables (ADR-0007).

Bronze keeps each file's own columns as raw strings. Silver selects the canonical columns by name
and gives an absent one its default, so ``SOURCES`` is the one list of expected columns.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from gtdp.io import INGESTION_DATE


class Format(StrEnum):
    CSV = "csv"
    """Comma-separated, with a header line naming the file's columns."""
    TEXT = "text"
    """One record per line, kept whole in the column ``value``."""


@dataclass(frozen=True)
class Source:
    name: str
    format: Format
    columns: tuple[str, ...]
    """Every column the contract defines, in file order. A file may lack the ``optional`` ones and
    hold more (unknown columns, kept in bronze and reported in the audit)."""
    optional: frozenset[str] = frozenset()
    """Columns of ``columns`` that appeared after the first drop (ADR-0007): ``discount_pct``."""

    @property
    def required(self) -> tuple[str, ...]:
        """The columns every file header of the source must name."""
        return tuple(column for column in self.columns if column not in self.optional)


SOURCES: Mapping[str, Source] = MappingProxyType(
    {
        source.name: source
        for source in (
            Source(
                "orders",
                Format.CSV,
                (
                    "transaction_id",
                    "event_time",
                    "customer_id",
                    "product_id",
                    "quantity",
                    "unit_price_eur",
                    "channel",
                    "status",
                    "discount_pct",
                ),
                optional=frozenset({"discount_pct"}),
            ),
            Source(
                "customers",
                Format.CSV,
                (
                    "customer_id",
                    "full_name",
                    "email",
                    "phone",
                    "region",
                    "segment",
                    "signup_date",
                    "updated_at",
                ),
            ),
            Source(
                "products",
                Format.CSV,
                ("product_id", "product_name", "category", "unit_price_eur", "updated_at"),
            ),
            Source("weblogs", Format.TEXT, ("value",)),
        )
    }
)

CORRUPT_RECORD = "_corrupt_record"
SOURCE_FILE = "_source_file"
INGESTED_AT = "_ingested_at"
RUN_ID = "_run_id"
RECORD_HASH = "_record_hash"

TECHNICAL_COLUMNS = StructType(
    [
        StructField(CORRUPT_RECORD, StringType()),
        StructField(SOURCE_FILE, StringType()),
        StructField(INGESTED_AT, TimestampType()),
        StructField(RUN_ID, StringType()),
        StructField(RECORD_HASH, StringType()),
    ]
)


def bronze_schema(source: Source) -> StructType:
    """Schema of ``bronze/<source>`` read with ``mergeSchema``: the canonical columns as strings,
    the technical columns, then the partition column."""
    raw = [StructField(column, StringType()) for column in source.columns]
    return StructType([*raw, *TECHNICAL_COLUMNS.fields, StructField(INGESTION_DATE, DateType())])


AMOUNT = DecimalType(10, 2)
DISCOUNT = DecimalType(4, 3)
REJECT_REASONS = "reject_reasons"
QUALITY_FLAGS = "quality_flags"
RAW_RECORD = "raw_record"

SILVER_ORDERS = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price_eur", AMOUNT, False),
        StructField("channel", StringType(), False),
        StructField("status", StringType(), False),
        StructField("discount_pct", DISCOUNT, False),
        StructField(RECORD_HASH, StringType(), False),
        StructField(INGESTION_DATE, DateType(), False),
    ]
)
"""``silver/orders``, partitioned by ``ingestion_date``: one row per ``transaction_id``."""

SILVER_CUSTOMERS = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("email_hash", StringType()),
        StructField("region", StringType()),
        StructField("country", StringType(), False),
        StructField("segment", StringType()),
        StructField("signup_date", DateType()),
        StructField("updated_at", TimestampType()),
        StructField(QUALITY_FLAGS, ArrayType(StringType()), False),
        StructField(RECORD_HASH, StringType(), False),
        StructField(INGESTION_DATE, DateType(), False),
    ]
)
"""``silver/customers`` (SCD type 1): the latest record of each ``customer_id``, pseudonymised."""

SILVER_PRODUCTS_SCD2 = StructType(
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
        StructField(RECORD_HASH, StringType(), False),
        StructField(INGESTION_DATE, DateType(), False),
    ]
)
"""``silver/products_scd2`` (SCD type 2): one row per version, ``valid_from <= t < valid_to``."""

SILVER_WEBLOGS = StructType(
    [
        StructField("log_id", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("ip_hash", StringType(), False),
        StructField("ip_prefix", StringType(), False),
        StructField("customer_id", StringType()),
        StructField("method", StringType(), False),
        StructField("path", StringType(), False),
        StructField("protocol", StringType(), False),
        StructField("http_status", IntegerType(), False),
        StructField("bytes_sent", LongType()),
        StructField("referer", StringType()),
        StructField("user_agent", StringType()),
        StructField("device", StringType()),
        StructField("event_type", StringType(), False),
        StructField("product_id", StringType()),
        StructField(INGESTION_DATE, DateType(), False),
    ]
)
"""``silver/weblogs``, partitioned by ``ingestion_date``: one parsed, pseudonymised request."""

SILVER_REJECTS = StructType(
    [
        StructField("source", StringType(), False),
        StructField(REJECT_REASONS, ArrayType(StringType()), False),
        StructField(RAW_RECORD, StringType(), False),
        StructField(RECORD_HASH, StringType(), False),
        StructField(SOURCE_FILE, StringType()),
        StructField(RUN_ID, StringType(), False),
        StructField(INGESTION_DATE, DateType(), False),
    ]
)
"""``silver/rejects/<source>``, partitioned by ``ingestion_date``: one rejected bronze record."""
