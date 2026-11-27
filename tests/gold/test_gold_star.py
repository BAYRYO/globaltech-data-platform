"""KAN-32, KAN-33: the star schema, the price applicable on the order date, the refresh rule."""

from datetime import date
from decimal import Decimal

import pytest

from gtdp.gold.kpis import KPIS
from gtdp.gold.tables import FACT_SALES, STAR
from gtdp.jobs.gold import GoldCheckError

from .silver_lake import J1, J2, customer, order, reject, request, versions

pytestmark = pytest.mark.slow


def sales_by_id(lake) -> dict:
    return {row.transaction_id: row for row in lake.gold("fact_sales").collect()}


def snapshot(lake) -> dict:
    """The rows of every gold table, by name."""
    names = [table.name for table in STAR] + list(KPIS)
    return {name: sorted(map(tuple, lake.gold(name).collect())) for name in names}


def rows_per_date(lake) -> dict:
    counts = lake.gold("fact_sales").groupBy("order_date").count().collect()
    return {str(row.order_date): row["count"] for row in counts}


def test_an_order_takes_the_price_of_the_version_valid_at_its_time(silver_lake):
    silver_lake.customers(customer("C1"))
    silver_lake.products(
        *versions("P00042", ("1900-01-01", "100.00"), ("2025-06-01T00:00:00", "90.00"))
    )
    silver_lake.orders(
        J1,
        order("T1", "2025-05-31T23:59:59", product="P00042", price="97.00"),
        order("T2", "2025-06-01T00:00:00", product="P00042", price="88.00"),
        order(
            "T3", "2025-07-01T10:00:00", product="P00042", quantity=2, price="85.00", discount="0.1"
        ),
    )

    silver_lake.run_gold(J1)

    sales = sales_by_id(silver_lake)
    assert {tid: (row.product_sk, row.applicable_unit_price) for tid, row in sales.items()} == {
        "T1": ("P00042-19000101000000", Decimal("100.00")),
        "T2": ("P00042-20250601000000", Decimal("90.00")),
        "T3": ("P00042-20250601000000", Decimal("90.00")),
    }
    assert (sales["T1"].revenue_eur, sales["T1"].invoiced_amount_eur) == (
        Decimal("100.00"),
        Decimal("97.00"),
    )
    # 2 x 90 x (1 - 0.1) at the catalog price, 2 x 85 x (1 - 0.1) invoiced.
    assert (sales["T3"].revenue_eur, sales["T3"].invoiced_amount_eur) == (
        Decimal("162.00"),
        Decimal("153.00"),
    )
    assert (sales["T3"].order_date, sales["T3"].date_key) == (date(2025, 7, 1), 20250701)


BROKEN_ORDERS = {
    "no_version": (
        versions("P1", ("2025-06-01T00:00:00", "10.00")),
        order("T1", "2025-01-15T10:00:00"),
    ),
    "several_versions": (
        [
            *versions("P1", ("1900-01-01", "10.00")),
            *versions("P1", ("2025-06-01T00:00:00", "12.00")),
        ],
        order("T1", "2025-07-01T10:00:00"),
    ),
    "orphan.fact_sales.customer_id": (
        versions("P1", ("1900-01-01", "10.00")),
        order("T1", "2025-07-01T10:00:00", customer="C9"),
    ),
}


@pytest.mark.parametrize("kind", BROKEN_ORDERS)
def test_an_order_without_exactly_one_version_and_one_customer_fails_before_publishing(
    silver_lake, kind
):
    catalog, broken = BROKEN_ORDERS[kind]
    silver_lake.customers(customer("C1"))
    silver_lake.products(*catalog)
    silver_lake.orders(J1, broken, order("T2", "2025-07-02T10:00:00"))

    with pytest.raises(GoldCheckError, match=kind):
        silver_lake.run_gold(J1)

    assert silver_lake.gold("fact_sales", FACT_SALES.schema).count() == 0


def test_a_customer_who_moves_changes_the_indicators_not_fact_sales(silver_lake):
    silver_lake.customers(customer("C1", country="FR"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00"))
    silver_lake.run_gold(J1)
    fact = sorted(map(tuple, silver_lake.gold("fact_sales").collect()))

    silver_lake.customers(customer("C1", country="DE"))
    silver_lake.complete(J2)
    run = silver_lake.run_gold(J2)

    assert (run.metrics["dates_rewritten"], run.metrics["dates_unchanged"]) == (0, 1)
    assert sorted(map(tuple, silver_lake.gold("fact_sales").collect())) == fact
    daily = silver_lake.gold("kpi_revenue_daily").collect()
    assert [(r.country, r.revenue_eur) for r in daily] == [("DE", Decimal("10.00"))]


def test_an_order_with_a_null_required_column_fails_the_run(silver_lake):
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00", channel=None))

    with pytest.raises(GoldCheckError, match="null_required.fact_sales"):
        silver_lake.run_gold(J1)


def test_a_late_order_refreshes_its_date_without_dropping_the_published_orders(silver_lake):
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(
        J1,
        order("T1", "2025-12-30T09:00:00"),
        order("T2", "2025-12-30T18:00:00"),
        order("T3", "2025-12-31T12:00:00"),
    )
    silver_lake.run_gold(J1)
    assert rows_per_date(silver_lake) == {"2025-12-30": 2, "2025-12-31": 1}

    silver_lake.orders(
        J2, order("T4", "2025-12-30T23:00:00", day=J2), order("T5", "2026-01-01T08:00:00", day=J2)
    )
    run = silver_lake.run_gold(J2)

    assert rows_per_date(silver_lake) == {"2025-12-30": 3, "2025-12-31": 1, "2026-01-01": 1}
    assert set(sales_by_id(silver_lake)) == {"T1", "T2", "T3", "T4", "T5"}
    changes = {k: run.metrics[k] for k in ("dates_rewritten", "dates_removed", "dates_unchanged")}
    assert changes == {"dates_rewritten": 2, "dates_removed": 0, "dates_unchanged": 1}
    assert (run.counters.rows_in, run.counters.rows_out) == (5, 5)


def test_replaying_a_day_leaves_every_gold_table_unchanged(silver_lake):
    silver_lake.customers(customer("C1"), customer("C2", country="DE", segment="B2B"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00"), ("2025-12-31", "11.00")))
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00"), order("T2", "2025-12-31T10:00:00"))
    silver_lake.weblogs(J1, request("L1", "2025-12-30T09:00:00", "view", product="P1"))
    silver_lake.rejects("orders", J1, reject("orders", J1, "BAD_PRICE"))
    silver_lake.run_gold(J1)
    silver_lake.orders(
        J2,
        order("T3", "2025-12-31T23:00:00", day=J2, customer="C2", status="RETURNED"),
        order("T4", "2026-01-01T08:00:00", day=J2, customer="C2"),
    )
    silver_lake.weblogs(J2, request("L2", "2026-01-01T08:00:00", "purchase", day=J2))
    silver_lake.run_gold(J2)
    before = snapshot(silver_lake)
    silver_lake.run_gold(J2)

    assert snapshot(silver_lake) == before
    assert all(before.values())


def test_a_build_that_breaks_an_invariant_writes_nothing(silver_lake):
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00"))
    silver_lake.run_gold(J1)
    before = snapshot(silver_lake)
    # A new customer, and the same transaction under another date: silver never publishes the
    # latter, gold still checks it.
    silver_lake.customers(customer("C1"), customer("C2", country="DE"))
    silver_lake.orders(J2, order("T1", "2026-01-01T08:00:00", day=J2))

    with pytest.raises(GoldCheckError, match="duplicate_key.fact_sales"):
        silver_lake.run_gold(J2)

    assert snapshot(silver_lake) == before


def test_gold_reads_only_the_days_silver_completed(silver_lake):
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00"), order("T2", "2025-12-30T10:00:00"))
    silver_lake.orders(J2, order("T3", "2025-12-30T23:00:00", day=J2), complete=False)

    with pytest.raises(FileNotFoundError, match="silver has not completed 2026-01-02"):
        silver_lake.run_gold(J2)
    silver_lake.run_gold(J1)
    assert rows_per_date(silver_lake) == {"2025-12-30": 2}

    silver_lake.complete(J2)
    silver_lake.run_gold(J2)
    assert rows_per_date(silver_lake) == {"2025-12-30": 3}


def test_the_calendar_covers_every_day_of_the_orders_and_logs(silver_lake):
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(J1, order("T1", "2025-12-29T09:00:00"))
    silver_lake.weblogs(J1, request("L1", "2026-01-04T23:59:59", "view"))

    silver_lake.run_gold(J1)

    days = silver_lake.gold("dim_date").orderBy("date").collect()
    assert [row.date_key for row in days] == [
        20251229,
        20251230,
        20251231,
        20260101,
        20260102,
        20260103,
        20260104,
    ]
    # ISO weeks: week 1 of 2026 starts on Monday 2025-12-29.
    assert [(row.iso_week, row.day_of_week, row.is_weekend) for row in days[::3]] == [
        (1, 1, False),
        (1, 4, False),
        (1, 7, True),
    ]
    assert (days[0].year, days[0].quarter, days[0].month) == (2025, 4, 12)
