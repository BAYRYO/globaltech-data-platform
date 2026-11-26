"""Canonical columns of the batch sources and the shape of their bronze tables (ADR-0007).

Bronze keeps each file's own columns as raw strings. Silver selects the canonical columns by name
and gives an absent one its default, so ``SOURCES`` is the one list of expected columns.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

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
    """Every column the contract defines, in file order; a file may hold fewer (``discount_pct``
    appears in J3) or more (unknown columns, kept in bronze and reported in the audit)."""


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
