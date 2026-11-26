"""KAN-29: products historised in SCD type 2, and the price applicable at an order's date."""

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from gtdp.silver.products import table, violations, with_applicable_price
from gtdp.silver.quality import duplicate_keys

pytestmark = pytest.mark.slow

EPOCH, OPEN = datetime(1900, 1, 1), datetime(9999, 12, 31)


def versions(spark, records):
    """``products.table`` of clean records ``(product_id, name, category, price, updated_at)``."""
    frame = spark.createDataFrame(
        [(*r, date(2026, 1, 1), f"h{i}") for i, r in enumerate(records)],
        "product_id string, product_name string, category string, unit_price_eur decimal(10,2),"
        " updated_at timestamp, ingestion_date date, _record_hash string",
    )
    return table(frame, salt=None).orderBy("product_id", "version").collect()


def test_a_new_version_starts_only_when_an_attribute_changes(spark):
    rows = versions(
        spark,
        [
            ("P1", "Lighting P1", "Lighting", Decimal(10), datetime(2024, 12, 31)),
            ("P1", "Lighting P1", "Lighting", Decimal(10), datetime(2025, 3, 1)),
            ("P1", "Lighting P1", "Lighting", Decimal(12), datetime(2025, 6, 1)),
            ("P1", "Lighting P1", "Safety", Decimal(12), datetime(2025, 9, 1)),
        ],
    )

    assert [(r.version, r.valid_from, r.valid_to, r.is_current) for r in rows] == [
        (1, EPOCH, datetime(2025, 6, 1), False),
        (2, datetime(2025, 6, 1), datetime(2025, 9, 1), False),
        (3, datetime(2025, 9, 1), OPEN, True),
    ]
    assert rows[0].product_sk == "P1-19000101000000"


def test_p00042_shows_its_2025_price_change(published):
    history = (
        published.table("products").where(F.col("product_id") == "P00042").orderBy("version")
    ).collect()

    assert len(history) >= 2
    assert history[0].valid_from == EPOCH
    assert any(v.valid_from.year == 2025 for v in history[1:])
    assert history[0].unit_price_eur != history[1].unit_price_eur
    assert [v.is_current for v in history] == [False] * (len(history) - 1) + [True]


def test_the_history_has_unique_contiguous_versions(published):
    products = published.table("products")

    assert duplicate_keys(products, ("product_id", "valid_from")) == 0
    assert set(violations(products).values()) == {0}
    assert products.where("is_current").count() == products.select("product_id").distinct().count()


def test_every_order_finds_exactly_one_applicable_price(published):
    orders = published.table("orders")
    priced = with_applicable_price(orders, published.table("products"))

    assert priced.count() == orders.count()
    assert priced.where(F.col("product_sk").isNull()).count() == 0


def test_the_price_at_the_order_date_differs_from_the_current_one(published):
    history = (
        published.table("products").where(F.col("product_id") == "P00042").orderBy("version")
    ).collect()
    first, second = history[0], history[1]
    orders = published.table("orders").where(F.col("product_id") == "P00042")
    before = orders.where(F.col("event_time") < second.valid_from).limit(1)
    after = orders.where(F.col("event_time") >= second.valid_from).limit(1)

    priced = with_applicable_price(before.unionByName(after), published.table("products"))
    prices = {
        row.event_time < second.valid_from: row.applicable_unit_price for row in priced.collect()
    }

    assert prices == {True: first.unit_price_eur, False: second.unit_price_eur}
