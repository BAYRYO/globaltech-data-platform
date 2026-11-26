"""Pure bronze transformations: technical columns on raw records, and count reconciliation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from gtdp.io import INGESTION_DATE
from gtdp.schemas import CORRUPT_RECORD, INGESTED_AT, RECORD_HASH, RUN_ID, SOURCE_FILE


class ReconciliationError(RuntimeError):
    """A file's counts disagree: rows were lost or added between the manifest and bronze."""


def raw_line(columns: Sequence[str]) -> Column:
    """The record's line as it stands in the file, without its line break.

    A corrupt record is its own line. Any other record is its fields joined by ``,``: the CSV
    reader keeps quotes as text (quoting is off) and turns an empty field into null.
    """
    joined = F.concat_ws(",", *[F.coalesce(F.col(column), F.lit("")) for column in columns])
    return F.coalesce(F.col(CORRUPT_RECORD), joined)


def to_bronze(
    records: DataFrame,
    columns: Sequence[str],
    *,
    source_file: str,
    run_id: str,
    ingested_at: datetime,
    ingestion_date: date,
) -> DataFrame:
    """``records`` (``columns`` then ``_corrupt_record``) with the bronze technical columns.

    ``_record_hash`` is the SHA-256 of the raw line, so it depends on the line alone: a replay
    gives the same hash, and two identical lines give the same hash wherever they landed.
    """
    return records.select(
        *columns,
        CORRUPT_RECORD,
        F.lit(source_file).alias(SOURCE_FILE),
        F.lit(ingested_at).alias(INGESTED_AT),
        F.lit(run_id).alias(RUN_ID),
        F.sha2(raw_line(columns), 256).alias(RECORD_HASH),
        F.lit(ingestion_date).alias(INGESTION_DATE),
    )


def check_counts(counts: Mapping[str, Mapping[str, int]]) -> None:
    """Raise ``ReconciliationError`` naming every file whose counts are not all equal.

    ``counts`` maps a file to its counts by label, for example
    ``{"orders/orders_20260101.csv": {"manifest": 10, "landing lines": 9}}``.
    """
    mismatches = [
        f"{file}: " + ", ".join(f"{label} {count}" for label, count in by_label.items())
        for file, by_label in counts.items()
        if len(set(by_label.values())) > 1
    ]
    if mismatches:
        raise ReconciliationError("; ".join(mismatches))
