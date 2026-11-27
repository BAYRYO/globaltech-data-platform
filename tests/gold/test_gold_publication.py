"""How a gold run publishes: one run at a time, all tables or a run marked as unfinished."""

from pathlib import Path

import pytest

from gtdp.io import TableLockedError

from .silver_lake import J1, customer, order, versions
from .test_gold_star import snapshot


def local(uri: str) -> Path:
    return Path(uri.removeprefix("file://"))


def test_a_run_that_meets_the_lock_of_another_gold_run_modifies_nothing(silver_lake):
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00"))
    silver_lake.run_gold(J1)
    before = snapshot(silver_lake)
    silver_lake.customers(customer("C1"), customer("C2", country="DE"))
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00"), order("T2", "2025-12-31T09:00:00"))
    # Another gold run holds the lock of the whole gold layer, beside the gold directory.
    (local(silver_lake.cfg.paths.gold).parent / "_gold.lock").write_text('{"run_id": "other"}')

    with pytest.raises(TableLockedError, match="_gold.lock"):
        silver_lake.run_gold(J1)

    assert snapshot(silver_lake) == before
