"""Integrity of the published star: unique keys, and facts that point at existing dimension rows."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gtdp.gold.tables import REFERENCES, STAR, Reference
from gtdp.silver.quality import duplicate_keys


def unmatched(tables: Mapping[str, DataFrame], reference: Reference) -> int:
    """Rows of ``reference.table`` whose non-null ``reference.column`` is not a key of
    ``reference.target``."""
    values = tables[reference.table.name].where(F.col(reference.column).isNotNull())
    keys = tables[reference.target.name].select(
        F.col(reference.target.key[0]).alias(reference.column)
    )
    return values.join(keys, reference.column, "left_anti").count()


def violations(tables: Mapping[str, DataFrame]) -> dict[str, int]:
    """Rows breaking each invariant of the star, by table name: ``duplicate_key.<table>`` and
    ``orphan.<table>.<column>`` for every required reference. All must be 0."""
    counts = {f"duplicate_key.{t.name}": duplicate_keys(tables[t.name], t.key) for t in STAR}
    counts |= {f"orphan.{r.name}": unmatched(tables, r) for r in REFERENCES if r.required}
    return counts


def optional_unmatched(tables: Mapping[str, DataFrame]) -> dict[str, int]:
    """Unmatched rows of each optional reference, reported only."""
    return {f"unmatched.{r.name}": unmatched(tables, r) for r in REFERENCES if not r.required}
