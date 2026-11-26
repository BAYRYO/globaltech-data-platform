"""Lot 6, Spark side: revenue per country and invalid-line counters from raw order lines.

The rules of mapreduce/mapper.py, written with native Spark functions. Each line is split on
commas as the mapper splits it, classified under the first rule of INVALID_RULES it fails, and a
single aggregation gives both the revenue per country and the counters. tests/mapreduce runs both
engines on the same files and compares their outputs.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from decimal import Decimal

from pyspark.sql import Column, DataFrame, Row
from pyspark.sql import functions as F

ORDER_COLUMNS = (
    "transaction_id",
    "event_time",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price_eur",
    "channel",
    "status",
    "discount_pct",
)
ORDER_WIDTHS = (8, 9)
CUSTOMER_ID, REGION, UPDATED_AT = 0, 4, 7
CUSTOMER_WIDTH = 8
CHANNELS = ("web", "store", "partner")
STATUSES = ("COMPLETED", "CANCELLED", "RETURNED")
MAX_DISCOUNT = Decimal("0.300")
# \A and \z rather than ^ and $: Java's $ also matches before a final U+2028 or U+0085.
QUANTITY_FORMAT = r"\A[1-9][0-9]{0,8}\z"
PRICE_FORMAT = r"\A[0-9]{1,8}(\.[0-9]{1,2})?\z"
TIMESTAMP_FORMAT = r"\A[1-9][0-9]{3}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\z"
DISCOUNT_FORMAT = r"\A0\.[0-9]{3}\z"
CONTROL_CHARACTER = r"[\x00-\x1f\x7f]"
HEADER, INVALID, COMPLETED, OTHER_STATUS = "HEADER", "INVALID", "COMPLETED", "OTHER_STATUS"


def _try_cast(column: str, sql_type: str) -> Column:
    """``column`` cast to ``sql_type``; NULL when it does not parse, even with ANSI mode on."""
    return F.expr(f"try_cast({column} AS {sql_type})")


def _discount() -> Column:
    return F.when(F.coalesce("discount_pct", F.lit("")) == "", F.lit(Decimal("0.000"))).otherwise(
        _try_cast("discount_pct", "DECIMAL(4,3)")
    )


INVALID_RULES: tuple[tuple[str, Callable[[], Column]], ...] = (
    ("MALFORMED_ROW", lambda: ~F.col("width").isin(*ORDER_WIDTHS)),
    ("BAD_QUANTITY", lambda: ~F.col("quantity").rlike(QUANTITY_FORMAT)),
    (
        "BAD_PRICE",
        lambda: ~F.col("unit_price_eur").rlike(PRICE_FORMAT)
        | (_try_cast("unit_price_eur", "DECIMAL(10,2)") == 0),
    ),
    (
        "BAD_TIMESTAMP",
        lambda: ~F.col("event_time").rlike(TIMESTAMP_FORMAT)
        | _try_cast("event_time", "TIMESTAMP").isNull(),
    ),
    ("NULL_CUSTOMER", lambda: F.col("customer_id") == ""),
    ("UNKNOWN_CUSTOMER", lambda: F.col("country").isNull()),
    ("BAD_ENUM", lambda: ~F.col("channel").isin(*CHANNELS) | ~F.col("status").isin(*STATUSES)),
    (
        "BAD_DISCOUNT",
        lambda: (F.col("discount_pct") != "")
        & (~F.col("discount_pct").rlike(DISCOUNT_FORMAT) | (_discount() > MAX_DISCOUNT)),
    ),
)
"""Precedence order: a line is counted once, under the first rule it fails. A rule builds its
condition on demand because a Column needs an active SparkContext."""


def customer_countries(lines: DataFrame) -> DataFrame:
    """``customer_id``, ``country`` from raw customer lines (column ``value``) of any files.

    A customer's row is the one with the greatest (updated_at, region); its country is the region
    prefix before ``-``, or UNKNOWN when that prefix is empty or holds an ASCII control character.
    Lines without 8 fields, the header and empty ids are ignored.
    """
    fields = F.split("value", ",", -1)
    customer_id = F.get(fields, CUSTOMER_ID)
    version = F.struct(
        F.get(fields, UPDATED_AT).alias("updated_at"), F.get(fields, REGION).alias("region")
    )
    latest = (
        lines.where((F.size(fields) == CUSTOMER_WIDTH) & ~customer_id.isin("", "customer_id"))
        .groupBy(customer_id.alias("customer_id"))
        .agg(F.max(version).alias("latest"))
    )
    prefix = F.substring_index("latest.region", "-", 1)
    return latest.select(
        "customer_id",
        F.when((prefix == "") | prefix.rlike(CONTROL_CHARACTER), "UNKNOWN")
        .otherwise(prefix)
        .alias("country"),
    )


def classify_orders(lines: DataFrame, countries: DataFrame) -> DataFrame:
    """``outcome``, ``reason``, ``country``, ``amount`` of each raw order line (column ``value``).

    ``outcome`` is HEADER, INVALID (``reason`` names the first failing rule), COMPLETED (with the
    customer's ``country``) or OTHER_STATUS. ``amount``, the invoiced amount
    ``quantity * unit_price_eur * (1 - discount_pct)`` as an exact decimal, is set on valid lines.
    ``countries`` is the output of ``customer_countries``.
    """
    fields = F.split("value", ",", -1)
    parsed = lines.select(
        F.size(fields).alias("width"),
        *[F.get(fields, index).alias(name) for index, name in enumerate(ORDER_COLUMNS)],
    )
    checked = parsed.join(F.broadcast(countries), "customer_id", "left").withColumn(
        "reason", F.coalesce(*[F.when(fails(), F.lit(code)) for code, fails in INVALID_RULES])
    )
    outcome = (
        F.when(F.col("transaction_id") == "transaction_id", HEADER)
        .when(F.col("reason").isNotNull(), INVALID)
        .when(F.col("status") == "COMPLETED", COMPLETED)
        .otherwise(OTHER_STATUS)
    )
    amount = (
        _try_cast("quantity", "INT")
        * _try_cast("unit_price_eur", "DECIMAL(10,2)")
        * (F.lit(Decimal(1)) - _discount())
    )
    return checked.withColumn("outcome", outcome).select(
        "outcome",
        F.when(F.col("outcome") == INVALID, F.col("reason")).alias("reason"),
        F.when(F.col("outcome") == COMPLETED, F.col("country")).alias("country"),
        F.when(F.col("outcome").isin(COMPLETED, OTHER_STATUS), amount).alias("amount"),
    )


def summarize(classified: DataFrame) -> DataFrame:
    """One row per (outcome, reason, country): ``lines`` and ``amount``, rounded half up."""
    return classified.groupBy("outcome", "reason", "country").agg(
        F.count(F.lit(1)).alias("lines"), F.round(F.sum("amount"), 2).alias("amount")
    )


def revenue_lines(summary: Iterable[Row]) -> list[str]:
    """``country<TAB>revenue_eur<TAB>completed_lines`` per country, the reducer's output format."""
    return sorted(
        f"{row.country}\t{row.amount}\t{row.lines}" for row in summary if row.outcome == COMPLETED
    )


def counters(summary: Iterable[Row]) -> dict[tuple[str, str], int]:
    """The mapper's counters: (Quality, reason) and (Lines, HEADER|COMPLETED|OTHER_STATUS)."""
    totals: Counter[tuple[str, str]] = Counter()
    for row in summary:
        key = ("Quality", row.reason) if row.outcome == INVALID else ("Lines", row.outcome)
        totals[key] += row.lines
    return dict(totals)
