"""The gold report on small lakes: what makes it fail, and what does not."""

import pytest

from gtdp.gold import report
from gtdp.gold.state import State, Status, write_state

from .silver_lake import J1, J2, customer, order, versions

pytestmark = pytest.mark.slow


def built_j1(silver_lake) -> None:
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00"))
    silver_lake.run_gold(J1)


def failed_checks(silver_lake) -> list:
    spark, cfg = silver_lake.spark, silver_lake.cfg
    dates = [row for row in report.order_dates(spark, cfg).collect() if row.status != "ok"]
    totals = [line for line in report.reconciliation(spark, cfg) if line[3] != "ok"]
    return dates + totals


def test_a_day_silver_completed_after_the_last_gold_run_is_pending_not_a_mismatch(silver_lake):
    built_j1(silver_lake)
    silver_lake.orders(J2, order("T2", "2025-12-30T10:00:00", day=J2))

    pending = report.publication(silver_lake.spark, silver_lake.cfg)
    pending_failures = failed_checks(silver_lake)
    silver_lake.run_gold(J2)
    built = report.publication(silver_lake.spark, silver_lake.cfg)

    assert (pending.published, pending.pending, pending.withdrawn) == (True, [J2], [])
    assert pending_failures == []
    assert (built.published, built.pending, built.withdrawn) == (True, [], [])
    assert failed_checks(silver_lake) == []


def test_a_day_silver_changed_under_gold_is_a_mismatch(silver_lake):
    built_j1(silver_lake)
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00"), order("T2", "2025-12-31T10:00:00"))

    statuses = {
        str(r.order_date): r.status
        for r in report.order_dates(silver_lake.spark, silver_lake.cfg).collect()
    }

    assert statuses == {"2025-12-30": "ok", "2025-12-31": "MISMATCH"}


def test_gold_left_publishing_is_not_published(silver_lake):
    built_j1(silver_lake)
    write_state(silver_lake.spark, silver_lake.cfg.paths.gold, State(Status.PUBLISHING, "r", (J1,)))

    assert not report.publication(silver_lake.spark, silver_lake.cfg).published
