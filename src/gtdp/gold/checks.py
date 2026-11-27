"""Integrity of the published star: unique keys, and facts that point at existing dimension rows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from functools import reduce
from operator import or_

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from gtdp.gold.tables import FACT_SALES, ORDER_DATE, REFERENCES, STAR, Reference, Table
from gtdp.silver.quality import duplicate_keys


def unmatched(tables: Mapping[str, DataFrame], reference: Reference) -> int:
    """Rows of ``reference.table`` whose non-null ``reference.column`` is not a key of
    ``reference.target``."""
    values = tables[reference.table.name].where(F.col(reference.column).isNotNull())
    keys = tables[reference.target.name].select(
        F.col(reference.target.key[0]).alias(reference.column)
    )
    return values.join(keys, reference.column, "left_anti").count()


def null_required(frame: DataFrame, table: Table) -> int:
    """Rows of ``frame`` with a null in a column that ``table.schema`` declares non-nullable."""
    required = [F.col(f.name).isNull() for f in table.schema.fields if not f.nullable]
    return frame.where(reduce(or_, required)).count()


def violations(tables: Mapping[str, DataFrame]) -> dict[str, int]:
    """Rows breaking each invariant of the star, by table name: ``duplicate_key.<table>``,
    ``null_required.<table>`` and ``orphan.<table>.<column>`` for every required reference. All
    must be 0."""
    counts = {f"duplicate_key.{t.name}": duplicate_keys(tables[t.name], t.key) for t in STAR}
    counts |= {f"null_required.{t.name}": null_required(tables[t.name], t) for t in STAR}
    counts |= {f"orphan.{r.name}": unmatched(tables, r) for r in REFERENCES if r.required}
    return counts


def optional_unmatched(tables: Mapping[str, DataFrame]) -> dict[str, int]:
    """Unmatched rows of each optional reference, reported only."""
    return {f"unmatched.{r.name}": unmatched(tables, r) for r in REFERENCES if not r.required}


def content_sum(columns: Sequence[str]) -> Column:
    """The first 60 bits of the SHA-256 of each row's ``columns`` as JSON, summed: the same for
    the same multiset of rows, whatever their order."""
    row = F.sha2(F.to_json(F.struct(*columns)), 256)
    return F.sum(F.conv(F.substring(row, 1, 15), 16, 10).cast("decimal(38,0)"))


def fingerprints(sales: DataFrame) -> dict[date, tuple[int, Decimal]]:
    """Rows and ``content_sum`` of every column of ``FACT_SALES``, per ``order_date``."""
    columns = [field.name for field in FACT_SALES.schema.fields]
    per_date = sales.groupBy(ORDER_DATE).agg(F.count(F.lit(1)), content_sum(columns))
    return {day: (rows, total) for day, rows, total in per_date.collect()}
