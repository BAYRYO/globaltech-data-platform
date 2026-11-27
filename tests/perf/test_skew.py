"""Salting the interval join keeps the result of the gold fact build's join."""

from datetime import datetime
from decimal import Decimal

import pytest

from gtdp.perf.datasets import VERSIONS
from gtdp.perf.skew import salted_applicable_price
from gtdp.silver.products import with_applicable_price

ORDERS = "transaction_id STRING, product_id STRING, event_time TIMESTAMP, quantity INT"
END = datetime(9999, 12, 31)


def day(d: int) -> datetime:
    return datetime(2025, 1, d)


@pytest.fixture
def frames(spark):
    hot = [(f"T{i:03d}", "P42", day(1 + i % 20), 1) for i in range(60)]
    others = [
        ("U1", "P1", day(5), 2),
        ("U2", "P1", day(25), 3),
        ("U3", "P9", day(5), 1),
        ("U4", "P1", day(1), 1),
        ("U4", "P1", day(1), 1),
    ]
    versions = [
        ("P42", 1, Decimal("10.00"), day(1), day(10)),
        ("P42", 2, Decimal("12.00"), day(10), END),
        ("P1", 3, Decimal("5.00"), day(2), day(20)),
        ("P1", 4, Decimal("6.00"), day(20), END),
    ]
    return spark.createDataFrame(hot + others, ORDERS), spark.createDataFrame(versions, VERSIONS)


def rows(frame):
    return sorted(frame.collect(), key=lambda r: (r.transaction_id, r.event_time))


@pytest.mark.parametrize("salts", [1, 4, 16])
def test_the_salted_join_gives_the_rows_of_the_plain_join(frames, salts):
    orders, versions = frames
    plain = with_applicable_price(orders, versions)
    salted = salted_applicable_price(orders, versions, salts)
    assert salted.columns == plain.columns
    assert rows(salted) == rows(plain)


def test_an_order_without_a_valid_version_keeps_a_null_price(frames):
    orders, versions = frames
    priced = {r.transaction_id: r for r in salted_applicable_price(orders, versions, 4).collect()}
    assert priced["U3"].product_sk is None
    assert priced["U1"].applicable_unit_price == Decimal("5.00")
    assert priced["U2"].applicable_unit_price == Decimal("6.00")


def test_at_least_one_salt_is_required(frames):
    with pytest.raises(ValueError, match="at least 1"):
        salted_applicable_price(*frames, 0)
