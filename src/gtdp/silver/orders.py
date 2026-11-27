"""Orders: the rules of a transaction line, and the rows of ``silver/orders``."""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from gtdp.schemas import DISCOUNT, SILVER_ORDERS, SOURCES
from gtdp.silver.parse import (
    amount,
    ancient,
    blank,
    future,
    integer,
    matching,
    missing_or_not_positive,
    outside,
    timestamp,
)
from gtdp.silver.quality import Action, Entity, Rule, conform, known, raw

CHANNELS = ("web", "store", "partner")
STATUSES = ("COMPLETED", "CANCELLED", "RETURNED")
DISCOUNT_PATTERN = r"^\d(\.\d{1,3})?$"
MAX_DISCOUNT = Decimal("0.30")
REJECT = Action.REJECT


def _columns() -> dict[str, Column]:
    discount = raw("discount_pct")
    return {
        "transaction_id": raw("transaction_id"),
        "event_time": timestamp(raw("event_time")),
        "customer_id": raw("customer_id"),
        "product_id": raw("product_id"),
        "quantity": integer(raw("quantity")),
        "unit_price_eur": amount(raw("unit_price_eur")),
        "channel": raw("channel"),
        "status": raw("status"),
        # Absent before J3, and empty in the J1 and J2 rows that J3 resends (ADR-0007).
        "discount_pct": F.when(blank(discount), F.lit(0).cast(DISCOUNT)).otherwise(
            matching(discount, DISCOUNT_PATTERN, DISCOUNT)
        ),
    }


ENTITY = Entity(
    SOURCES["orders"],
    _columns,
    (
        Rule("MISSING_KEY", REJECT, lambda: blank(raw("transaction_id"))),
        Rule("NULL_CUSTOMER", REJECT, lambda: blank(raw("customer_id"))),
        Rule(
            "UNKNOWN_CUSTOMER",
            REJECT,
            lambda: ~blank(raw("customer_id")) & ~known("customer_id"),
        ),
        Rule("UNKNOWN_PRODUCT", REJECT, lambda: ~known("product_id")),
        Rule("BAD_QUANTITY", REJECT, lambda: missing_or_not_positive(F.col("quantity"))),
        Rule("BAD_PRICE", REJECT, lambda: missing_or_not_positive(F.col("unit_price_eur"))),
        Rule("BAD_TIMESTAMP", REJECT, lambda: F.col("event_time").isNull()),
        Rule("FUTURE_TIMESTAMP", REJECT, lambda: future(F.col("event_time"))),
        Rule("ANCIENT_TIMESTAMP", REJECT, lambda: ancient(F.col("event_time"))),
        Rule(
            "BAD_ENUM",
            REJECT,
            lambda: outside(F.col("channel"), CHANNELS) | outside(F.col("status"), STATUSES),
        ),
        Rule(
            "BAD_DISCOUNT",
            REJECT,
            lambda: ~F.coalesce(F.col("discount_pct").between(0, MAX_DISCOUNT), F.lit(False)),
        ),
    ),
    key=("transaction_id",),
    references={"customer_id": "customers", "product_id": "products"},
)


def table(clean: DataFrame) -> DataFrame:
    """The rows of ``silver/orders``."""
    return conform(clean, SILVER_ORDERS)
