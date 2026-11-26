"""Classification of bronze records: each record gets exactly one disposition.

Order of operations (data-model.md, "Ordre des opérations et réconciliation"), per day:

1. exact duplicates: one record per ``_record_hash``, the others are ``duplicated.exact``;
2. one projection evaluates the source's rule registry into ``reject_reasons`` and
   ``quality_flags``. A record with a ``_corrupt_record`` gets ``MALFORMED_ROW`` alone: its other
   columns are shifted or null, and one record must count under one code only;
3. key duplicates among the records left: one survivor per key, the lowest ``_record_hash``, the
   others are ``duplicated.key``. A record whose key is partly null is never a key duplicate;
4. cross-day duplicates: a key published on an earlier day is ``duplicated.cross_day``.

``rejected`` records go to ``silver/rejects``; ``clean`` records, flags included, to the silver
table. Hence ``rows = clean + rejected + duplicated`` for every day.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType

from gtdp.bronze.transform import raw_line
from gtdp.io import INGESTION_DATE
from gtdp.schemas import (
    CORRUPT_RECORD,
    QUALITY_FLAGS,
    RAW_RECORD,
    RECORD_HASH,
    REJECT_REASONS,
    RUN_ID,
    SOURCE_FILE,
    Source,
)
from gtdp.silver.pseudonymize import SALT, with_salt

RAW = "raw"
"""Struct of a record's canonical columns, as the strings bronze holds."""
DISPOSITION = "disposition"
MALFORMED_ROW = "MALFORMED_ROW"
EXACT_DUPLICATE = "_exact_duplicate"
_KEY_RANK = "_key_rank"
_SEEN_BEFORE = "_seen_before"


class Disposition(StrEnum):
    CLEAN = "clean"
    REJECTED = "rejected"
    DUPLICATE_EXACT = "duplicated.exact"
    DUPLICATE_KEY = "duplicated.key"
    DUPLICATE_CROSS_DAY = "duplicated.cross_day"


class Action(StrEnum):
    REJECT = "reject"
    """The record goes to ``silver/rejects``, its code in ``reject_reasons``."""
    FLAG = "flag"
    """The record stays in silver, its code in ``quality_flags``."""


@dataclass(frozen=True)
class Rule:
    code: str
    action: Action
    broken: Callable[[], Column]
    """True when a well-formed record breaks the rule; null counts as not broken."""


@dataclass(frozen=True)
class Entity:
    """What silver checks in the records of one source."""

    source: Source
    columns: Callable[[], Mapping[str, Column]]
    """The typed columns, computed from ``raw``."""
    rules: tuple[Rule, ...]
    key: tuple[str, ...] = ()
    """Typed columns that identify a record within a day; ``()`` when its hash does."""
    references: Mapping[str, str] = field(default_factory=dict)
    """Raw column -> source whose bronze identifiers, up to the day, are the known values. Rules
    read the answer in ``known(column)``."""


def raw(column: str) -> Column:
    return F.col(f"{RAW}.{column}")


def known(column: str) -> Column:
    """Whether the record's ``column`` is among the identifiers of ``Entity.references``."""
    return F.col(_known_name(column))


def _known_name(column: str) -> str:
    return f"_known_{column}"


def records_sharing(*columns: Column) -> Column:
    """How many distinct well-formed records of the day hold the same values of ``columns``."""
    distinct = ~F.col(EXACT_DUPLICATE) & F.col(CORRUPT_RECORD).isNull()
    return F.count(F.when(distinct, 1)).over(Window.partitionBy(INGESTION_DATE, *columns))


def with_references(records: DataFrame, identifiers: Mapping[str, DataFrame]) -> DataFrame:
    """``records`` with ``known(column)`` for each ``column`` of ``identifiers``, which maps it to
    a DataFrame holding the known values in a column of the same name."""
    for column, values in identifiers.items():
        name = _known_name(column)
        flags = values.select(column).where(F.col(column).isNotNull()).distinct()
        records = records.join(
            F.broadcast(flags.withColumn(name, F.lit(True))), column, "left"
        ).withColumn(name, F.coalesce(F.col(name), F.lit(False)))
    return records


# Evaluated in this order: the first condition that holds gives the disposition.
_DISPOSITIONS: tuple[tuple[Disposition, Callable[[], Column]], ...] = (
    (Disposition.DUPLICATE_EXACT, lambda: F.col(EXACT_DUPLICATE)),
    (Disposition.REJECTED, lambda: F.size(REJECT_REASONS) > 0),
    (Disposition.DUPLICATE_KEY, lambda: F.col(_KEY_RANK) > 1),
    (Disposition.DUPLICATE_CROSS_DAY, lambda: F.col(_SEEN_BEFORE)),
)


def classify(
    records: DataFrame, entity: Entity, salt: DataFrame, earlier_keys: DataFrame | None = None
) -> DataFrame:
    """``records`` with ``raw``, the typed columns, ``reject_reasons``, ``quality_flags`` and
    ``disposition``.

    ``records`` holds the canonical columns of ``entity.source`` as strings, ``_corrupt_record``,
    ``_record_hash``, ``_source_file``, ``ingestion_date`` and the ``known`` columns. Every window
    is per ``ingestion_date``: several days are classified as if each ran alone. ``salt`` is the
    one-row frame of ``salt_frame``, which the typed columns read through ``salted``; the result
    holds the hashes, not the salt. ``earlier_keys`` holds the ``entity.key`` values that earlier
    days published.
    """
    columns = entity.source.columns
    same_record = Window.partitionBy(INGESTION_DATE, RECORD_HASH).orderBy(SOURCE_FILE)
    staged = records.select(
        F.struct(*columns).alias(RAW),
        *[column for column in records.columns if column not in columns],
        (F.row_number().over(same_record) > 1).alias(EXACT_DUPLICATE),
    )
    typed = (
        with_salt(staged, salt)
        .select("*", *[c.alias(name) for name, c in entity.columns().items()])
        .drop(SALT)
    )
    malformed = F.col(CORRUPT_RECORD).isNotNull()
    checked = typed.select(
        "*",
        F.when(malformed, F.array(F.lit(MALFORMED_ROW)))
        .otherwise(_codes(entity.rules, Action.REJECT))
        .alias(REJECT_REASONS),
        F.when(malformed, _no_codes())
        .otherwise(_codes(entity.rules, Action.FLAG))
        .alias(QUALITY_FLAGS),
    )

    if entity.key:
        candidate = ~F.col(EXACT_DUPLICATE) & (F.size(REJECT_REASONS) == 0)
        by_key = Window.partitionBy(INGESTION_DATE, *entity.key).orderBy(
            candidate.desc(), F.col(RECORD_HASH)
        )
        # A record with part of its key unreadable, such as a customer's updated_at, restates
        # no other record: it is never a key duplicate.
        incomplete = reduce(Column.__or__, [F.col(column).isNull() for column in entity.key])
        rank = F.when(incomplete, F.lit(1)).otherwise(F.row_number().over(by_key))
        checked = checked.withColumn(_KEY_RANK, rank)
    else:
        checked = checked.withColumn(_KEY_RANK, F.lit(1))
    if earlier_keys is None:
        checked = checked.withColumn(_SEEN_BEFORE, F.lit(False))
    else:
        seen = earlier_keys.select(*entity.key).distinct().withColumn(_SEEN_BEFORE, F.lit(True))
        checked = checked.join(seen, list(entity.key), "left").withColumn(
            _SEEN_BEFORE, F.coalesce(F.col(_SEEN_BEFORE), F.lit(False))
        )

    disposition = F.lit(Disposition.CLEAN.value)
    for kind, condition in reversed(_DISPOSITIONS):
        disposition = F.when(condition(), F.lit(kind.value)).otherwise(disposition)
    return checked.withColumn(DISPOSITION, disposition)


def _codes(rules: Sequence[Rule], action: Action) -> Column:
    codes = [F.when(rule.broken(), F.lit(rule.code)) for rule in rules if rule.action is action]
    if not codes:
        return _no_codes()
    return F.filter(F.array(*codes), lambda code: code.isNotNull())


def _no_codes() -> Column:
    return F.array().cast(ArrayType(StringType()))


def measures(classified: DataFrame) -> dict[str, int]:
    """Records per disposition, rejected records per reason and clean records per flag.

    For example ``{"clean": 9, "rejected": 2, "duplicated.exact": 1, "rejected.BAD_PRICE": 2,
    "flagged.BAD_EMAIL": 1}``: the names that ``gtdp.silver.reconcile`` compares to a manifest.
    """
    disposition = F.col(DISPOSITION)

    def prefixed(column: str, prefix: str, kind: Disposition) -> Column:
        names = F.transform(column, lambda code: F.concat(F.lit(prefix), code))
        return F.when(disposition == kind.value, names).otherwise(_no_codes())

    names = F.concat(
        F.array(disposition),
        prefixed(REJECT_REASONS, "rejected.", Disposition.REJECTED),
        prefixed(QUALITY_FLAGS, "flagged.", Disposition.CLEAN),
    )
    counts = classified.select(F.explode(names).alias("measure")).groupBy("measure").count()
    return {row["measure"]: row["count"] for row in counts.collect()}


def rejects(
    classified: DataFrame, line_columns: Sequence[str], *, source: str, run_id: str
) -> DataFrame:
    """The rejected records, shaped like ``silver/rejects``.

    ``raw_record`` is the record's line: ``_corrupt_record``, or the ``line_columns`` (the
    columns of its file, in order) joined by commas, as bronze hashed it into ``_record_hash``.
    """
    rejected = classified.where(F.col(DISPOSITION) == Disposition.REJECTED.value)
    lines = rejected.select(
        f"{RAW}.*", CORRUPT_RECORD, REJECT_REASONS, RECORD_HASH, SOURCE_FILE, INGESTION_DATE
    )
    return lines.select(
        F.lit(source).alias("source"),
        REJECT_REASONS,
        raw_line(line_columns).alias(RAW_RECORD),
        RECORD_HASH,
        SOURCE_FILE,
        F.lit(run_id).alias(RUN_ID),
        INGESTION_DATE,
    )


def conform(frame: DataFrame, schema: StructType) -> DataFrame:
    """``frame`` reduced to the columns of ``schema``, in its order and with its types."""
    return frame.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def duplicate_keys(table: DataFrame, key: Sequence[str]) -> int:
    """Rows of ``table`` beyond the first of each ``key`` value."""
    extra = table.groupBy(*key).count().agg(F.sum(F.col("count") - 1).alias("extra")).first()
    return int(extra["extra"] or 0) if extra else 0
