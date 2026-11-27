"""Typed values from the raw strings of bronze. A value that does not match its format is null.

Each parser checks the text against the contract's format before casting: a bare cast is too
lenient (``"1.5"`` casts to the integer 1, ``"2025-03-04 10:00"`` to a timestamp).
"""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DataType

from gtdp.io import INGESTION_DATE
from gtdp.schemas import AMOUNT

TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"
DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"
DAY_FIRST_DATE_PATTERN = r"^(\d{2})/(\d{2})/(\d{4})$"
INTEGER_PATTERN = r"^-?\d{1,9}$"
AMOUNT_PATTERN = r"^-?\d{1,8}(\.\d{1,2})?$"
EARLIEST = "1900-01-01"
"""The day version 1 of every product is valid from (``gtdp.silver.products``), the earliest
day the model knows: a date or time before it is not plausible."""


def blank(column: Column) -> Column:
    """Null, empty or only spaces: the CSV reader turns an empty field into null."""
    return F.coalesce(F.trim(column), F.lit("")) == ""


def matching(column: Column, pattern: str, to: DataType | str) -> Column:
    """``column`` cast to ``to`` when it matches ``pattern``, else null.

    The cast still rejects a well-formed but impossible value, such as ``2025-02-30``.
    """
    return F.when(column.rlike(pattern), column.cast(to))


def timestamp(column: Column) -> Column:
    """``yyyy-MM-dd'T'HH:mm:ss`` in the session time zone (UTC)."""
    return matching(column, TIMESTAMP_PATTERN, "timestamp")


def iso_date(column: Column) -> Column:
    return matching(column, DATE_PATTERN, "date")


def day_first_date(column: Column) -> Column:
    """``dd/MM/yyyy``, the format of the ``BAD_DATE`` anomaly."""
    iso = F.regexp_replace(column, DAY_FIRST_DATE_PATTERN, "$3-$2-$1")
    return F.when(column.rlike(DAY_FIRST_DATE_PATTERN), iso.cast("date"))


def integer(column: Column) -> Column:
    return matching(column, INTEGER_PATTERN, "int")


def amount(column: Column) -> Column:
    """A ``decimal(10,2)`` written with at most two decimals."""
    return matching(column, AMOUNT_PATTERN, AMOUNT)


def missing_or_not_positive(column: Column) -> Column:
    return column.isNull() | (column <= 0)


def ancient(column: Column) -> Column:
    """A date, or a time in UTC, before ``EARLIEST``; a null is not."""
    return column < F.lit(EARLIEST).cast("date")


def future(column: Column) -> Column:
    """A time later than the day after the drop: ``column > ingestion_date + 1 day``, the next
    midnight included."""
    return column > F.date_add(F.col(INGESTION_DATE), 1).cast("timestamp")


def outside(column: Column, values: Iterable[str]) -> Column:
    """Not one of ``values``; a null is outside."""
    return ~F.coalesce(column.isin(*values), F.lit(False))


def null_if(column: Column, *values: str) -> Column:
    return F.when(~column.isin(*values), column)
