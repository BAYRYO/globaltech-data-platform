"""Adversarial review A (data correctness) of the gold layer: tests that are expected to FAIL.

Each test states the documented behaviour it checks; a failure is a finding of the review.
"""

from collections import defaultdict
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from statistics import median

import pytest
from pyspark.sql import functions as F

from gtdp.gold import kpis, report
from gtdp.gold.kpis import KPIS
from gtdp.gold.tables import FACT_SALES, table_path
from gtdp.io import read_table
from gtdp.jobs.gold import GoldCheckError
from silver.lakes import DAYS, SALT, bronze_lake

from .silver_lake import J1, J2, customer, order, versions
from .test_gold_kpis import sale

pytestmark = pytest.mark.slow

CENT = Decimal("0.01")


def gold_ids(lake) -> set:
    return {row.transaction_id for row in lake.gold("fact_sales").collect()}


def kpi_snapshot(lake) -> dict:
    return {name: sorted(map(tuple, lake.gold(name).collect()), key=repr) for name in KPIS}


# --- F1: fact_sales.country is frozen at the build of each partition -------------------------


def test_fact_sales_country_is_the_current_country_of_dim_customer(silver_lake):
    """Dictionary: ``country`` = "country du client dans dim_customer (SCD 1 : pays actuel)"."""
    silver_lake.customers(customer("C1", country="FR"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00"))
    silver_lake.run_gold(J1)

    # J2: the customer moves to DE (SCD1 update) and orders on another date.
    silver_lake.customers(customer("C1", country="DE"))
    silver_lake.orders(J2, order("T2", "2026-01-01T08:00:00", day=J2))
    silver_lake.run_gold(J2)

    dim = {r.customer_id: r.country for r in silver_lake.gold("dim_customer").collect()}
    fact = {r.transaction_id: r.country for r in silver_lake.gold("fact_sales").collect()}
    assert dim == {"C1": "DE"}
    assert fact == {"T1": "DE", "T2": "DE"}


def test_replaying_an_earlier_day_leaves_the_indicators_unchanged(silver_lake):
    """Runbook: "L'ordre des jours n'a pas d'importance", and a replay rewrites the same rows.
    Silver does not change between the two snapshots."""
    silver_lake.customers(customer("C1", country="FR"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00"))
    silver_lake.run_gold(J1)
    silver_lake.customers(customer("C1", country="DE"))
    silver_lake.orders(J2, order("T2", "2026-01-01T08:00:00", day=J2))
    silver_lake.run_gold(J2)
    before = kpi_snapshot(silver_lake)

    silver_lake.run_gold(J1)

    assert kpi_snapshot(silver_lake) == before


def test_generated_fact_sales_country_matches_dim_customer(generated):
    """The same on the generated ci drops: J2 and J3 update the region of existing customers."""
    gold = generated.cfg.paths.gold
    sales = read_table(generated.spark, table_path(gold, "fact_sales"), FACT_SALES.schema)
    dim = read_table(generated.spark, table_path(gold, "dim_customer")).select(
        "customer_id", F.col("country").alias("current_country")
    )

    stale = sales.join(dim, "customer_id").where(F.col("country") != F.col("current_country"))

    assert stale.count() == 0


# --- F2: a fact_sales partition no silver day names any more is never cleaned ----------------


def test_an_order_silver_no_longer_publishes_leaves_fact_sales_after_the_replay(silver_lake):
    """Runbook, "Rejouer un jour": after a silver replay, replay gold for each replayed day.
    KAN-32: total gold revenue equals the total computed from silver."""
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    kept = (order("T0", "2025-12-29T09:00:00"), order("T2", "2025-12-31T09:00:00"))
    silver_lake.orders(J1, *kept, order("T1", "2025-12-30T09:00:00", quantity=7))
    silver_lake.run_gold(J1)

    # Silver J1 is replayed: T1 is now a reject (or a duplicate), its date has no order left.
    silver_lake.orders(J1, *kept)
    run = silver_lake.run_gold(J1)  # the replay succeeds: no check notices the ghost

    assert set(run.metrics["checks"].values()) == {0}
    daily = {
        str(r.order_date): r.revenue_eur for r in silver_lake.gold("kpi_revenue_daily").collect()
    }
    assert (gold_ids(silver_lake), daily) == (
        {"T0", "T2"},
        {"2025-12-29": Decimal("10.00"), "2025-12-31": Decimal("10.00")},
    )


def test_a_corrected_drop_replayed_through_silver_and_gold_leaves_no_ghost_order(spark, tmp_path):
    """The same through the real jobs on the ci drops: the J2 file is corrected (the late orders
    of one date are withdrawn), bronze, silver and gold of J2 are replayed as the runbooks say."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("GTDP_PSEUDO_SALT", SALT)
        lake = bronze_lake(spark, tmp_path, 42)
        for day in DAYS[:2]:
            lake.run("silver", day)
            lake.run("gold", day)
        late = (
            lake.table("orders")
            .where(F.col("ingestion_date") == J2)
            .select("transaction_id", F.to_date("event_time").alias("order_date"))
            .where(F.col("order_date") < "2026-01-01")
            .collect()
        )
        withdrawn_date = late[0].order_date
        withdrawn = {r.transaction_id for r in late if r.order_date == withdrawn_date}

        lake.change_landed(
            DAYS[1],
            "orders",
            lambda lines: [line for line in lines if line.split(",")[0] not in withdrawn],
        )
        lake.run("silver", DAYS[1])
        run = lake.run("gold", DAYS[1])

    silver_ids = {r.transaction_id for r in lake.table("orders").collect()}
    gold = read_table(spark, table_path(lake.cfg.paths.gold, "fact_sales"), FACT_SALES.schema)
    ghosts = {r.transaction_id for r in gold.collect()} - silver_ids
    assert withdrawn and not withdrawn & silver_ids
    assert set(run.metrics["checks"].values()) == {0}  # the gold replay succeeded
    assert ghosts == set()


def test_gold_recovers_when_a_replayed_order_changes_its_date(silver_lake):
    """Runbook, troubleshooting of ``duplicate_key.fact_sales``: "Rejouez gold pour les jours
    concernés"."""
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(J1, order("T1", "2025-12-30T23:30:00"), order("T2", "2025-12-31T09:00:00"))
    silver_lake.run_gold(J1)

    # Silver J1 is replayed with T1 corrected by one hour: it is now an order of the 31st.
    silver_lake.orders(J1, order("T1", "2025-12-31T00:30:00"), order("T2", "2025-12-31T09:00:00"))
    try:
        silver_lake.run_gold(J1)
    except GoldCheckError:
        silver_lake.run_gold(J1)  # the documented repair: replay the day

    assert gold_ids(silver_lake) == {"T1", "T2"}
    assert silver_lake.gold("fact_sales").count() == 2


# --- F3: an amount that does not fit decimal(12,2) becomes a null revenue, silently ----------


def test_a_line_amount_beyond_decimal_12_2_is_not_published_as_null(silver_lake):
    """``fact_sales.revenue_eur`` is declared non-null; silver accepts quantities of 9 digits
    and prices of 8 digits."""
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "100000.00")))
    silver_lake.orders(
        J1,
        order("T1", "2025-12-30T09:00:00", quantity=100_000, price="100000.00"),
        order("T2", "2025-12-30T10:00:00", quantity=1, price="100000.00"),
    )

    try:
        silver_lake.run_gold(J1)
    except GoldCheckError:
        return  # failing closed would be fine

    revenue = {r.transaction_id: r.revenue_eur for r in silver_lake.gold("fact_sales").collect()}
    daily = silver_lake.gold("kpi_revenue_daily").collect()
    assert revenue["T1"] == Decimal("10000000000.00")
    assert daily[0].revenue_eur == Decimal("10000100000.00")


# --- F4: percentile_approx is not the median on an even number of lines ----------------------


def test_price_deviation_is_the_median_on_an_even_number_of_lines(spark):
    """Dictionary: ``median_price_ratio`` = "Médiane de unit_price_invoiced /
    applicable_unit_price"."""
    from gtdp.gold import dimensions
    from gtdp.schemas import SILVER_PRODUCTS_SCD2

    from .silver_lake import frame

    sales = spark.createDataFrame(
        [
            sale("T1", "2025-03-02", applicable="100.00", invoiced="90.00"),
            sale("T2", "2025-03-05", applicable="100.00", invoiced="100.00"),
        ],
        FACT_SALES.schema,
    )
    products = dimensions.products(
        frame(spark, SILVER_PRODUCTS_SCD2, versions("P1", ("1900-01-01", "100.00")))
    )

    (row,) = kpis.price_deviation(sales, products).collect()

    assert row.median_price_ratio == pytest.approx(median([0.90, 1.00]))


# --- F5: the report's "by hand" top N check rejects a correct indicator with a tie -----------


def test_the_report_top_n_check_accepts_a_tie_inside_the_top(silver_lake):
    """Dictionary: products tied share a rank and are all kept; the report must agree."""
    silver_lake.customers(customer("C1"))
    silver_lake.products(
        *[row for p in ("P1", "P2", "P3", "P4") for row in versions(p, ("1900-01-01", "10.00"))]
    )
    silver_lake.orders(
        J1,
        order("T1", "2025-12-30T09:00:00", product="P1", quantity=5),
        order("T2", "2025-12-30T09:00:00", product="P2", quantity=5),
        order("T3", "2025-12-30T09:00:00", product="P3", quantity=3),
        order("T4", "2025-12-30T09:00:00", product="P4", quantity=1),
    )
    silver_lake.run_gold(J1)
    cfg = silver_lake.cfg
    top = silver_lake.gold("kpi_top_products_country").orderBy("rank", "product_id").collect()
    assert [(r.rank, r.product_id) for r in top] == [(1, "P1"), (1, "P2"), (2, "P3"), (3, "P4")]

    lines = report.top_products_check(silver_lake.spark, _with_top_n(cfg, 2), "FR")

    assert all(line[-1] for line in lines), lines


def _with_top_n(cfg, n):
    from dataclasses import replace

    return replace(cfg, gold=replace(cfg.gold, top_n=n))


# --- Independent recomputation on the generated ci drops (expected to pass, see the report) ---


def _silver(generated, source):
    return generated.table(source).collect()


def _gold(generated, name):
    return read_table(generated.spark, table_path(generated.cfg.paths.gold, name)).collect()


def _amount(quantity, price, discount) -> Decimal:
    return (Decimal(quantity) * price * (1 - discount)).quantize(CENT, ROUND_HALF_UP)


@pytest.fixture(scope="module")
def plain(generated):
    """``fact_sales`` recomputed in plain Python from silver: one dict per order."""
    history = defaultdict(list)
    for v in _silver(generated, "products"):
        history[v.product_id].append(v)
    people = {c.customer_id: c for c in _silver(generated, "customers")}
    rows = {}
    for o in _silver(generated, "orders"):
        valid = [v for v in history[o.product_id] if v.valid_from <= o.event_time < v.valid_to]
        assert len(valid) == 1
        (version,) = valid
        rows[o.transaction_id] = {
            "order_date": o.event_time.date(),
            "country": people[o.customer_id].country,
            "segment": people[o.customer_id].segment,
            "status": o.status,
            "channel": o.channel,
            "product_id": o.product_id,
            "product_sk": version.product_sk,
            "category": version.category,
            "quantity": o.quantity,
            "applicable": version.unit_price_eur,
            "invoiced": o.unit_price_eur,
            "revenue": _amount(o.quantity, version.unit_price_eur, o.discount_pct),
            "invoiced_amount": _amount(o.quantity, o.unit_price_eur, o.discount_pct),
        }
    return rows


def test_plain_python_fact_sales_amounts_and_versions(generated, plain):
    sales = {r.transaction_id: r for r in _gold(generated, "fact_sales")}

    assert sales.keys() == plain.keys()
    wrong = [
        tid
        for tid, r in sales.items()
        if (
            r.product_sk,
            r.applicable_unit_price,
            r.revenue_eur,
            r.invoiced_amount_eur,
            r.order_date,
        )
        != (
            plain[tid]["product_sk"],
            plain[tid]["applicable"],
            plain[tid]["revenue"],
            plain[tid]["invoiced_amount"],
            plain[tid]["order_date"],
        )
    ]
    assert wrong == []
    assert sum(r.revenue_eur for r in sales.values()) == sum(p["revenue"] for p in plain.values())
    assert any(r.discount_pct > 0 for r in sales.values())


def test_plain_python_daily_revenue_with_the_current_country(generated, plain):
    """``kpi_revenue_daily`` per (date, current country of the customer)."""
    expected = defaultdict(lambda: [0, 0, Decimal(0)])
    for p in plain.values():
        if p["status"] == "COMPLETED":
            cell = expected[(p["order_date"], p["country"])]
            cell[0] += 1
            cell[1] += p["quantity"]
            cell[2] += p["revenue"]
    got = {
        (r.order_date, r.country): [r.orders, r.quantity, r.revenue_eur]
        for r in _gold(generated, "kpi_revenue_daily")
    }

    differing = {k for k in expected.keys() | got.keys() if expected.get(k) != got.get(k)}
    assert differing == set()


def test_plain_python_indicators_from_the_published_fact(generated):
    """Top N, moving average, cancel/return, basket, price deviation, recomputed in plain Python
    from the published ``fact_sales`` (its own country column), so that the frozen country of F1
    does not hide another defect."""
    cfg = generated.cfg.gold
    sales = _gold(generated, "fact_sales")
    category = {v.product_sk: v.category for v in _gold(generated, "dim_product")}
    segment = {c.customer_id: c.segment for c in _gold(generated, "dim_customer")}
    completed = [r for r in sales if r.status == "COMPLETED"]

    # Top N: dense rank of revenue per country.
    totals = defaultdict(Decimal)
    for r in completed:
        totals[(r.country, r.product_id)] += r.revenue_eur
    expected_top = set()
    for country in {c for c, _ in totals}:
        revenues = sorted({v for (c, _), v in totals.items() if c == country}, reverse=True)
        kept = revenues[: cfg.top_n]
        expected_top |= {
            (country, kept.index(v) + 1, p, v)
            for (c, p), v in totals.items()
            if c == country and v in kept
        }
    top = {
        (r.country, r.rank, r.product_id, r.revenue_eur)
        for r in _gold(generated, "kpi_top_products_country")
    }
    assert top == expected_top

    # Moving average over the complete calendar, zero-filled.
    daily = defaultdict(Decimal)
    for r in completed:
        daily[(r.country, r.order_date)] += r.revenue_eur
    first, last = min(r.order_date for r in sales), max(r.order_date for r in sales)
    calendar = [date.fromordinal(d) for d in range(first.toordinal(), last.toordinal() + 1)]
    expected_avg = {}
    for country in {r.country for r in sales}:
        series = [daily.get((country, day), Decimal(0)) for day in calendar]
        for i, day in enumerate(calendar):
            window = series[max(0, i - cfg.moving_average_days + 1) : i + 1]
            mean = (sum(window) / len(window)).quantize(CENT, ROUND_HALF_UP)
            expected_avg[(country, day)] = (series[i], mean, len(window))
    got_avg = {
        (r.country, r.order_date): (r.revenue_eur, r.revenue_moving_avg_eur, r.window_days)
        for r in _gold(generated, "kpi_revenue_moving_avg")
    }
    assert got_avg == expected_avg

    # Cancel and return rates.
    lines = defaultdict(lambda: [0, 0, 0])
    for r in sales:
        cell = lines[(r.order_date, r.country, r.channel, category[r.product_sk])]
        cell[0] += 1
        cell[1] += r.status == "CANCELLED"
        cell[2] += r.status == "RETURNED"
    got_rates = {
        (r.order_date, r.country, r.channel, r.category): (
            r.lines,
            r.cancelled,
            r.returned,
            r.cancel_rate,
            r.return_rate,
        )
        for r in _gold(generated, "kpi_cancel_return_rate")
    }
    assert got_rates == {
        k: (n, c, ret, pytest.approx(c / n), pytest.approx(ret / n))
        for k, (n, c, ret) in lines.items()
    }

    # Average basket.
    baskets = defaultdict(list)
    for r in completed:
        baskets[(r.order_date, r.country, segment[r.customer_id])].append(r)
    got_baskets = {
        (r.order_date, r.country, r.segment): (r.orders, r.revenue_eur, r.avg_basket_eur)
        for r in _gold(generated, "kpi_avg_basket")
    }
    assert got_baskets == {
        k: (
            len(rows),
            sum(r.revenue_eur for r in rows),
            (sum(r.revenue_eur for r in rows) / len(rows)).quantize(CENT, ROUND_HALF_UP),
        )
        for k, rows in baskets.items()
    }


def test_plain_python_web_indicators(generated):
    events = _gold(generated, "fact_web_events")

    hours = defaultdict(lambda: [0, 0, 0])
    for e in events:
        cell = hours[e.event_ts.replace(minute=0, second=0, microsecond=0)]
        cell[0] += 1
        cell[1] += 400 <= e.http_status <= 499
        cell[2] += 500 <= e.http_status <= 599
    got = {
        r.event_hour: [r.requests, r.http_4xx, r.http_5xx]
        for r in _gold(generated, "kpi_http_errors_hourly")
    }
    assert got == dict(hours)

    visits = defaultdict(set)
    for e in events:
        if e.http_status < 400 and e.customer_id is not None:
            visits[(e.customer_id, e.event_ts.date(), e.device)].add(e.event_type)
    funnel = defaultdict(lambda: [0, 0, 0, 0])
    for (_, _, device), seen in visits.items():
        cell = funnel[device]
        cell[0] += 1
        cell[1] += "view" in seen
        cell[2] += {"view", "cart"} <= seen
        cell[3] += {"view", "cart", "purchase"} <= seen
    got_funnel = {
        r.device: [r.visits, r.product_views, r.carts, r.orders]
        for r in _gold(generated, "kpi_conversion_funnel")
    }
    assert got_funnel == dict(funnel)


def test_plain_python_price_deviation_and_data_quality(generated):
    sales = _gold(generated, "fact_sales")
    category = {v.product_sk: v.category for v in _gold(generated, "dim_product")}
    ratios = defaultdict(list)
    for r in sales:
        month = r.order_date.replace(day=1)
        ratios[(month, category[r.product_sk])].append(
            float(r.unit_price_invoiced / r.applicable_unit_price)
        )
    got = {
        (r.month, r.category): (r.lines, r.median_price_ratio)
        for r in _gold(generated, "kpi_price_deviation")
    }
    assert got.keys() == ratios.keys()
    assert {k: n for k, (n, _) in got.items()} == {k: len(v) for k, v in ratios.items()}
    # percentile_approx returns an element of the group: within 0.01 of the true median here.
    assert max(abs(got[k][1] - median(v)) for k, v in ratios.items()) < 0.01

    expected = defaultdict(int)
    for source in ("orders", "customers", "products", "weblogs"):
        for r in generated.rejects(source).collect():
            for code in r.reject_reasons:
                expected[(r.ingestion_date, r.source, code, "rejected")] += 1
    signup = {c.customer_id: c.signup_date for c in _silver(generated, "customers")}
    for o in _silver(generated, "orders"):
        if signup[o.customer_id] is not None and o.event_time.date() < signup[o.customer_id]:
            expected[(o.ingestion_date, "orders", "ORDER_BEFORE_SIGNUP", "kept")] += 1
    quality = {
        (r.ingestion_date, r.source, r.code, r.disposition): r.rows
        for r in _gold(generated, "kpi_data_quality")
    }
    assert quality == dict(expected)
