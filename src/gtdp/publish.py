"""The batch jobs' one way to write a lake table: as its registry declares it."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from gtdp.io import PartitionValue, replace_table, write_partitions


class SchemaMismatchError(RuntimeError):
    """The rows to publish lack a declared column, hold an undeclared one, or differ in type or
    column order from the declared schema."""


def publish(
    df: DataFrame,
    path: str,
    schema: StructType,
    partitions: Sequence[str] = (),
    values: Sequence[PartitionValue] = (),
    *,
    ordered: bool = True,
) -> None:
    """Write ``df`` at ``path`` with the columns ``schema`` declares and the registry's partition
    column, ``partitions`` (at most one).

    ``df`` must have the columns of ``schema`` with their ``simpleString`` types, in the same
    order unless ``ordered`` is false; otherwise ``SchemaMismatchError`` names the difference and
    nothing is written. With a partition column, the partitions ``values`` of that column are
    replaced (``write_partitions``). Without one, the table is replaced whole (``replace_table``)
    and ``values`` must be empty: a run that names partitions of an unpartitioned table would
    replace all of it with some of its rows.
    """
    if differences := _differences(df.schema, schema, ordered=ordered):
        raise SchemaMismatchError(
            f"{path}: the rows to publish are not the declared ones: {differences}"
        )
    if not partitions:
        if values:
            raise ValueError(
                f"{path} has no partition column, but partitions {list(values)} were named"
            )
        replace_table(df, path)
        return
    (column,) = partitions
    write_partitions(df, path, {column: values})


def _differences(actual: StructType, declared: StructType, *, ordered: bool) -> str:
    have = {f.name: f.dataType.simpleString() for f in actual.fields}
    want = {f.name: f.dataType.simpleString() for f in declared.fields}
    found = []
    if missing := [name for name in want if name not in have]:
        found.append(f"missing {missing}")
    if extra := [name for name in have if name not in want]:
        found.append(f"not declared {extra}")
    found += [
        f"{name}: {have[name]} instead of {kind}"
        for name, kind in want.items()
        if name in have and have[name] != kind
    ]
    if ordered and not found and list(have) != list(want):
        found.append(f"order {list(have)} instead of {list(want)}")
    return "; ".join(found)
