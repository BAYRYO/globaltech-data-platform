"""Review B (operations and robustness) of the gold job: each test states the behaviour the
runbook or the Jira criteria promise and fails on the current code."""

import dataclasses
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from gtdp.config import ConfigError, GoldSettings, load_config
from gtdp.gold.tables import FACT_SALES
from gtdp.io import TableLockedError
from gtdp.silver import days
from gtdp.silver.pipelines import PIPELINES

from .silver_lake import J1, J2, SilverLake, customer, order, request, versions

pytestmark = pytest.mark.slow


def local(uri: str) -> Path:
    return Path(uri.removeprefix("file://"))


def ids(lake: SilverLake) -> set[str]:
    return {row.transaction_id for row in lake.gold("fact_sales", FACT_SALES.schema).collect()}


def published(silver_lake: SilverLake) -> None:
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00"), order("T2", "2025-12-31T10:00:00"))
    silver_lake.run_gold(J1)


def three_dates(silver_lake: SilverLake) -> None:
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(
        J1,
        order("T0", "2025-12-29T09:00:00"),
        order("T1", "2025-12-30T09:00:00"),
        order("T2", "2025-12-31T10:00:00"),
    )
    silver_lake.run_gold(J1)


def test_a_run_that_meets_a_held_table_lock_modifies_nothing(silver_lake):
    """Runbook: 'la seconde échoue sur le verrou de table (TableLockedError) sans rien modifier'."""
    published(silver_lake)
    silver_lake.customers(customer("C1"), customer("C2", country="DE"))
    # Another gold run is writing fact_sales right now: it holds the table's lock.
    lock = local(silver_lake.cfg.paths.gold) / "_fact_sales.lock"
    lock.write_text('{"run_id": "other-gold-run"}')

    with pytest.raises(TableLockedError):
        silver_lake.run_gold(J1)

    assert silver_lake.gold("dim_customer").count() == 1, "dim_customer was replaced anyway"


@pytest.mark.parametrize(
    ("key", "value"), [("moving_average_days", 3_000_000_000), ("top_n", 10**20)]
)
def test_a_gold_setting_spark_cannot_use_is_refused_by_the_configuration(key, value):
    """A ROWS frame boundary is a Java int and a rank bound a Java long: a value beyond them must
    exit 2 at load time, not exit 1 after half of gold is published."""
    with pytest.raises(ConfigError, match=key):
        load_config("local", overrides={f"gold.{key}": str(value)})


def test_a_run_with_an_unusable_window_publishes_nothing(silver_lake):
    published(silver_lake)
    silver_lake.orders(J2, order("T3", "2026-01-01T08:00:00", day=J2))
    silver_lake.cfg = dataclasses.replace(
        silver_lake.cfg, gold=GoldSettings(top_n=5, moving_average_days=3_000_000_000)
    )

    with pytest.raises(Exception) as failure:
        silver_lake.run_gold(J2)

    reason = f"{type(failure.value).__name__}: {str(failure.value).splitlines()[0]}"
    assert ids(silver_lake) == {"T1", "T2"}, f"fact_sales published by a run that failed: {reason}"
    revenue = silver_lake.gold("kpi_revenue_daily").agg(F.sum("revenue_eur")).first()[0]
    assert revenue == 20, "kpi_revenue_daily was rebuilt by a run that failed"


def test_a_silver_replay_never_leaves_gold_with_part_of_an_incomplete_day(silver_lake):
    """Runbook: after a silver replay, rerun gold for each replayed day. Silver replaying J1 makes
    J2 incomplete; gold J1 must then either refuse, or serve J2 wholly or not at all."""
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.weblogs(J1, request("L1", "2026-01-01T12:00:00", "view"))
    silver_lake.orders(
        J1,
        order("T1", "2025-12-30T09:00:00"),
        order("T2", "2025-12-30T18:00:00"),
        order("T3", "2025-12-31T12:00:00"),
    )
    silver_lake.run_gold(J1)
    silver_lake.orders(
        J2, order("T4", "2025-12-30T23:00:00", day=J2), order("T5", "2026-01-01T08:00:00", day=J2)
    )
    silver_lake.run_gold(J2)
    assert ids(silver_lake) == {"T1", "T2", "T3", "T4", "T5"}

    # silver --date J1 --force: J2 and later become incomplete, J1 is published again as is.
    silver = silver_lake.cfg.paths.silver
    days.unmark_after(silver_lake.spark, silver, J1)
    days.unmark(silver_lake.spark, silver, J1, PIPELINES)
    silver_lake.orders(
        J1,
        order("T1", "2025-12-30T09:00:00"),
        order("T2", "2025-12-30T18:00:00"),
        order("T3", "2025-12-31T12:00:00"),
    )

    try:
        silver_lake.run_gold(J1)
    except Exception:
        return  # Failing closed is acceptable.
    assert ids(silver_lake) in ({"T1", "T2", "T3"}, {"T1", "T2", "T3", "T4", "T5"})


def test_the_runbook_repair_of_an_order_moved_to_another_date_works(silver_lake):
    """Runbook, duplicate_key.fact_sales: 'Une commande figure sous deux dates : silver a changé
    une commande déjà publiée en gold. Rejouez gold pour les jours concernés'."""
    three_dates(silver_lake)
    # silver --date J1 replayed on a corrected drop: T1 now happened on 2025-12-31.
    silver_lake.orders(
        J1,
        order("T0", "2025-12-29T09:00:00"),
        order("T1", "2025-12-31T09:00:00"),
        order("T2", "2025-12-31T10:00:00"),
    )
    with pytest.raises(Exception):  # noqa: B017
        silver_lake.run_gold(J1)

    silver_lake.run_gold(J1)  # The repair the runbook gives.

    dates = {r.transaction_id: str(r.order_date) for r in silver_lake.gold("fact_sales").collect()}
    assert dates == {"T0": "2025-12-29", "T1": "2025-12-31", "T2": "2025-12-31"}


def test_gold_notices_an_order_that_silver_no_longer_holds(silver_lake):
    """KAN-32: total gold revenue equals total revenue computed from silver. A replay of silver J1
    that rejects the only order of 2025-12-30 leaves the gold run green and that order served."""
    three_dates(silver_lake)
    silver_lake.orders(J1, order("T0", "2025-12-29T09:00:00"), order("T2", "2025-12-31T10:00:00"))

    try:
        silver_lake.run_gold(J1)
    except Exception:
        return  # Failing closed is acceptable.
    assert ids(silver_lake) == {"T0", "T2"}
    revenue = silver_lake.gold("kpi_revenue_daily").agg(F.sum("revenue_eur")).first()[0]
    assert revenue == 20
