"""KAN-34, KAN-35, KAN-36: the gold indicators, each on a small star built by hand."""

from datetime import date, datetime, time
from decimal import ROUND_HALF_UP, Decimal

import pytest

from gtdp.gold import dimensions, facts, kpis
from gtdp.gold.tables import FACT_SALES
from gtdp.schemas import (
    SILVER_CUSTOMERS,
    SILVER_ORDERS,
    SILVER_PRODUCTS_SCD2,
    SILVER_REJECTS,
    SILVER_WEBLOGS,
)

from .silver_lake import J1, J2, customer, frame, order, reject, request, versions

pytestmark = pytest.mark.slow


def sale(
    transaction_id: str,
    day: str,
    *,
    country: str = "FR",
    product: str = "P1",
    version: str = "19000101000000",
    customer_id: str = "C1",
    channel: str = "web",
    status: str = "COMPLETED",
    quantity: int = 1,
    applicable: str = "10.00",
    invoiced: str | None = None,
) -> tuple:
    """A ``fact_sales`` row without discount."""
    on = date.fromisoformat(day)
    applicable_price, invoiced_price = Decimal(applicable), Decimal(invoiced or applicable)
    return (
        transaction_id,
        datetime.combine(on, time(12)),
        int(on.strftime("%Y%m%d")),
        customer_id,
        f"{product}-{version}",
        product,
        country,
        channel,
        status,
        quantity,
        invoiced_price,
        applicable_price,
        Decimal(0),
        quantity * applicable_price,
        quantity * invoiced_price,
        on,
    )


@pytest.fixture
def star(spark):
    def build(*rows):
        return spark.createDataFrame(list(rows), FACT_SALES.schema)

    return build


@pytest.fixture
def catalog(spark):
    def build(*rows):
        return dimensions.products(frame(spark, SILVER_PRODUCTS_SCD2, list(rows)))

    return build


@pytest.fixture
def customers(spark):
    def build(*rows):
        return dimensions.customers(frame(spark, SILVER_CUSTOMERS, list(rows)))

    return build


@pytest.fixture
def web(spark):
    def build(*rows):
        return facts.web_events(frame(spark, SILVER_WEBLOGS, list(rows)))

    return build


def test_daily_revenue_counts_completed_orders_per_country(star):
    sales = star(
        sale("T1", "2025-03-01", applicable="10.00"),
        sale("T2", "2025-03-01", applicable="20.00", quantity=2),
        sale("T3", "2025-03-01", applicable="500.00", status="CANCELLED"),
        sale("T4", "2025-03-01", country="DE", applicable="7.00"),
    )

    rows = kpis.revenue_daily(sales).collect()

    assert {(r.country, r.orders, r.quantity, r.revenue_eur) for r in rows} == {
        ("FR", 2, 3, Decimal("50.00")),
        ("DE", 1, 1, Decimal("7.00")),
    }


@pytest.mark.parametrize(
    ("n", "expected_fr"),
    [
        (1, [(1, "P1")]),
        (2, [(1, "P1"), (2, "P2"), (2, "P3")]),
        (3, [(1, "P1"), (2, "P2"), (2, "P3"), (3, "P4")]),
    ],
)
def test_top_products_keep_every_product_tied_at_a_kept_rank(star, catalog, n, expected_fr):
    sales = star(
        sale("T1", "2025-03-01", product="P1", applicable="300.00"),
        sale("T2", "2025-03-01", product="P2", applicable="100.00", quantity=2),
        sale("T3", "2025-03-02", product="P3", applicable="200.00"),
        sale("T4", "2025-03-02", product="P4", applicable="100.00"),
        sale("T5", "2025-03-02", product="P5", applicable="50.00"),
        sale("T6", "2025-03-03", product="P5", applicable="999.00", status="CANCELLED"),
        sale("T7", "2025-03-03", product="P5", country="DE", applicable="70.00"),
    )
    products = catalog(
        *versions("P1", ("1900-01-01", "300.00"), ("2025-06-01", "320.00", "Safety")),
        *[row for p in ("P2", "P3", "P4", "P5") for row in versions(p, ("1900-01-01", "1.00"))],
    )

    rows = kpis.top_products_by_country(sales, products, n).collect()

    ranked = sorted((r.country, r.rank, r.product_id) for r in rows)
    assert ranked == sorted([("DE", 1, "P5")] + [("FR", *rank) for rank in expected_fr])
    top = next(r for r in rows if r.product_id == "P1")
    assert (top.revenue_eur, top.quantity, top.orders, top.category) == (
        Decimal("300.00"),
        1,
        1,
        "Safety",
    )


def test_the_moving_average_counts_a_day_without_sales_as_zero(star):
    sales = star(
        sale("T1", "2025-01-01", applicable="70.00"),
        sale("T2", "2025-01-02", applicable="500.00", status="CANCELLED"),
        sale("T3", "2025-01-03", applicable="140.00"),
        sale("T4", "2025-01-09", applicable="7.00"),
        sale("T5", "2025-01-05", country="DE", applicable="35.00"),
    )

    rows = kpis.revenue_moving_avg(sales, 7).collect()

    by_country = {
        country: [
            (r.revenue_eur, r.revenue_moving_avg_eur, r.window_days)
            for r in sorted((r for r in rows if r.country == country), key=lambda r: r.order_date)
        ]
        for country in ("FR", "DE")
    }
    assert by_country["FR"] == [
        (70, 70, 1),
        (0, 35, 2),
        (140, 70, 3),
        (0, Decimal("52.50"), 4),
        (0, 42, 5),
        (0, 35, 6),
        (0, 30, 7),
        (0, 20, 7),
        (7, 21, 7),
    ]
    assert [ma for _, ma, _ in by_country["DE"]] == [0, 0, 0, 0, 7, Decimal("5.83"), 5, 5, 5]
    assert len(rows) == 18


REFERENCE_SALES = {
    "FR": {1: "120.50", 2: "80.25", 5: "300.00", 6: "15.10", 7: "42.42", 11: "99.99", 15: "7.00"},
    "IT": {3: "10.00", 4: "260.40", 9: "33.33", 10: "1.01", 14: "500.00"},
}


def reference_moving_average(days: int) -> dict:
    """The moving average computed with plain Python over a calendar of days 1 to 15."""
    expected = {}
    for country, revenue in REFERENCE_SALES.items():
        daily = [Decimal(revenue.get(day, "0")) for day in range(1, 16)]
        for index, day in enumerate(range(1, 16)):
            window = daily[max(0, index - days + 1) : index + 1]
            mean = (sum(window) / len(window)).quantize(Decimal("0.01"), ROUND_HALF_UP)
            expected[(country, day)] = (daily[index], mean, len(window))
    return expected


@pytest.mark.parametrize("days", [1, 3, 7])
def test_the_moving_average_matches_a_plain_python_computation(star, days):
    sales = star(
        *[
            sale(f"{country}{day}", f"2025-01-{day:02d}", country=country, applicable=revenue)
            for country, revenue_per_day in REFERENCE_SALES.items()
            for day, revenue in revenue_per_day.items()
        ]
    )

    rows = kpis.revenue_moving_avg(sales, days).collect()

    assert {
        (r.country, r.order_date.day): (r.revenue_eur, r.revenue_moving_avg_eur, r.window_days)
        for r in rows
    } == reference_moving_average(days)


def test_price_deviation_is_the_median_ratio_per_month_and_version_category(star, catalog):
    sales = star(
        sale("T1", "2025-03-02", applicable="100.00", invoiced="90.00"),
        sale("T2", "2025-03-05", applicable="100.00", invoiced="100.00", status="CANCELLED"),
        sale("T3", "2025-03-09", applicable="100.00", invoiced="95.00"),
        sale("T4", "2025-03-20", version="20250315000000", applicable="80.00", invoiced="76.00"),
        sale("T5", "2025-04-02", version="20250315000000", applicable="80.00", invoiced="80.00"),
    )
    products = catalog(*versions("P1", ("1900-01-01", "100.00"), ("2025-03-15", "80.00", "Safety")))

    rows = kpis.price_deviation(sales, products).collect()

    assert {(str(r.month), r.category, r.lines): r.median_price_ratio for r in rows} == {
        ("2025-03-01", "Lighting", 3): pytest.approx(0.95),
        ("2025-03-01", "Safety", 1): pytest.approx(0.95),
        ("2025-04-01", "Safety", 1): pytest.approx(1.0),
    }


def test_cancel_and_return_rates_are_shares_of_lines_per_day_country_channel_category(
    star, catalog
):
    sales = star(
        sale("T1", "2025-03-01"),
        sale("T2", "2025-03-01"),
        sale("T3", "2025-03-01", status="CANCELLED"),
        sale("T4", "2025-03-01", status="RETURNED"),
        sale("T5", "2025-03-01", channel="store"),
        sale("T6", "2025-03-02", status="CANCELLED"),
        sale("T7", "2025-03-01", country="DE", status="RETURNED"),
    )

    rows = kpis.cancel_return_rate(sales, catalog(*versions("P1", ("1900-01-01", "1.00"))))

    assert {
        (str(r.order_date), r.country, r.channel, r.category): (
            r.lines,
            r.cancelled,
            r.returned,
            r.cancel_rate,
            r.return_rate,
        )
        for r in rows.collect()
    } == {
        ("2025-03-01", "FR", "web", "Lighting"): (4, 1, 1, 0.25, 0.25),
        ("2025-03-01", "FR", "store", "Lighting"): (1, 0, 0, 0.0, 0.0),
        ("2025-03-02", "FR", "web", "Lighting"): (1, 1, 0, 1.0, 0.0),
        ("2025-03-01", "DE", "web", "Lighting"): (1, 0, 1, 0.0, 1.0),
    }


def test_the_average_basket_is_per_completed_order_day_country_and_segment(star, customers):
    sales = star(
        sale("T1", "2025-03-01", applicable="10.00", invoiced="9.00"),
        sale("T2", "2025-03-01", applicable="30.00", invoiced="27.00"),
        sale("T3", "2025-03-01", applicable="900.00", status="CANCELLED"),
        sale("T4", "2025-03-01", customer_id="C2", applicable="25.00", quantity=2),
        sale("T5", "2025-03-02", applicable="40.00"),
    )
    people = customers(customer("C1", segment="B2C"), customer("C2", segment="B2B"))

    rows = kpis.avg_basket(sales, people).collect()

    assert {
        (str(r.order_date), r.country, r.segment): (
            r.orders,
            r.revenue_eur,
            r.avg_basket_eur,
            r.avg_invoiced_eur,
        )
        for r in rows
    } == {
        ("2025-03-01", "FR", "B2C"): (2, Decimal("40.00"), Decimal("20.00"), Decimal("18.00")),
        ("2025-03-01", "FR", "B2B"): (1, Decimal("50.00"), Decimal("50.00"), Decimal("50.00")),
        ("2025-03-02", "FR", "B2C"): (1, Decimal("40.00"), Decimal("40.00"), Decimal("40.00")),
    }


def test_client_and_server_errors_are_counted_per_hour(web):
    events = web(
        request("L1", "2026-01-01T10:05:00", "view"),
        request("L2", "2026-01-01T10:20:00", "view", status=404),
        request("L3", "2026-01-01T10:30:00", "view", status=403),
        request("L4", "2026-01-01T10:40:00", "purchase", status=500),
        request("L5", "2026-01-01T10:59:59", "search"),
        request("L6", "2026-01-01T11:00:00", "view"),
        request("L7", "2026-01-01T11:10:00", "cart", status=503),
    )

    rows = kpis.http_errors_hourly(events).collect()

    assert {
        r.event_hour: (r.requests, r.http_4xx, r.http_5xx, r.rate_4xx, r.rate_5xx) for r in rows
    } == {
        datetime(2026, 1, 1, 10): (5, 2, 1, 0.4, 0.2),
        datetime(2026, 1, 1, 11): (2, 0, 1, 0.0, 0.5),
    }


def test_a_visit_reaches_a_funnel_step_only_through_the_steps_before(web):
    day1, day2 = "2026-01-01T10:00:00", "2026-01-02T10:00:00"
    mobile = [
        *[request(f"A{e}", day1, e, customer_id="C1") for e in ("view", "cart", "purchase")],
        *[request(f"B{e}", day1, e, customer_id="C2") for e in ("view", "cart")],
        *[request(f"C{e}", day1, e, customer_id="C3") for e in ("cart", "purchase")],
        request("D1", day2, "view", customer_id="C1"),
        request("E1", day1, "view", customer_id="C4", status=404),
        request("E2", day1, "search", customer_id="C4"),
        request("F1", day1, "view", customer_id="C5"),
        request("F2", day1, "purchase", customer_id="C5", status=500),
        request("G1", day1, "view", customer_id=None),
    ]
    desktop = [request("H1", day1, "view", customer_id="C1", device="desktop")]

    rows = {r.device: r for r in kpis.conversion_funnel(web(*mobile, *desktop)).collect()}

    steps = {device: (r.visits, r.product_views, r.carts, r.orders) for device, r in rows.items()}
    assert steps == {"mobile": (6, 4, 2, 1), "desktop": (1, 1, 0, 0)}
    assert all(list(counts) == sorted(counts, reverse=True) for counts in steps.values())
    mobile_rates = rows["mobile"]
    assert (
        mobile_rates.view_rate,
        mobile_rates.cart_rate,
        mobile_rates.order_rate,
        mobile_rates.conversion_rate,
    ) == (pytest.approx(4 / 6), 0.5, 0.5, pytest.approx(1 / 6))
    assert (rows["desktop"].cart_rate, rows["desktop"].order_rate) == (0.0, None)


def test_data_quality_counts_rejects_per_code_and_orders_before_signup(spark, customers):
    rejects = frame(
        spark,
        SILVER_REJECTS,
        [
            reject("orders", J1, "BAD_PRICE"),
            reject("orders", J1, "BAD_QUANTITY", "BAD_PRICE"),
            reject("orders", J2, "MALFORMED_ROW"),
            reject("customers", J1, "MISSING_KEY"),
        ],
    )
    orders = frame(
        spark,
        SILVER_ORDERS,
        [
            order("T1", "2025-05-31T23:59:59"),
            order("T2", "2025-06-01T00:00:00"),
            order("T3", "2025-12-31T10:00:00", day=J2),
        ],
    )
    people = customers(customer("C1", signup="2025-06-01"))

    rows = kpis.data_quality(rejects, orders, people).collect()

    assert {(r.ingestion_date, r.source, r.code, r.disposition): r.rows for r in rows} == {
        (J1, "orders", "BAD_PRICE", "rejected"): 2,
        (J1, "orders", "BAD_QUANTITY", "rejected"): 1,
        (J2, "orders", "MALFORMED_ROW", "rejected"): 1,
        (J1, "customers", "MISSING_KEY", "rejected"): 1,
        (J1, "orders", "ORDER_BEFORE_SIGNUP", "kept"): 1,
    }
