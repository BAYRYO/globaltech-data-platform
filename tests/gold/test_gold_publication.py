"""How a gold run publishes: one run at a time, all tables or a run marked as unfinished."""

import dataclasses
from pathlib import Path

import pytest

from gtdp import publish as publish_module
from gtdp.audit import Run
from gtdp.gold.state import State, Status, read_state, write_state
from gtdp.io import TableLockedError, replace_table
from gtdp.jobs.gold import GoldCheckError

from .silver_lake import J1, J2, SilverLake, customer, order, versions
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


def published_j1(silver_lake) -> Run:
    silver_lake.customers(customer("C1"))
    silver_lake.products(*versions("P1", ("1900-01-01", "10.00")))
    silver_lake.orders(J1, order("T1", "2025-12-30T09:00:00"))
    return silver_lake.run_gold(J1)


def test_a_run_marks_gold_published_with_the_silver_days_it_read(silver_lake):
    run = published_j1(silver_lake)
    silver_lake.orders(J2, order("T2", "2026-01-01T09:00:00", day=J2), complete=False)

    assert read_state(silver_lake.spark, silver_lake.cfg.paths.gold) == State(
        Status.PUBLISHED, run.run_id, (J1,)
    )


def test_a_run_that_dies_while_writing_leaves_publishing_and_the_next_run_converges(
    silver_lake, monkeypatch, tmp_path
):
    published_j1(silver_lake)
    silver_lake.customers(customer("C1"), customer("C2", country="DE"))
    silver_lake.orders(J2, order("T2", "2026-01-01T09:00:00", day=J2, customer="C2"))
    writes = []

    def dies_on_the_third_table(frame, path, *args):
        writes.append(path)
        if len(writes) == 3:
            raise RuntimeError("killed")
        replace_table(frame, path, *args)

    monkeypatch.setattr(publish_module, "replace_table", dies_on_the_third_table)
    with pytest.raises(RuntimeError, match="killed"):
        silver_lake.run_gold(J2)
    monkeypatch.undo()
    killed = read_state(silver_lake.spark, silver_lake.cfg.paths.gold)

    run = silver_lake.run_gold(J2)

    assert killed is not None and killed.status is Status.PUBLISHING
    assert read_state(silver_lake.spark, silver_lake.cfg.paths.gold) == State(
        Status.PUBLISHED, run.run_id, (J1, J2)
    )
    once = dataclasses.replace(
        silver_lake.cfg,
        paths=dataclasses.replace(silver_lake.cfg.paths, gold=(tmp_path / "once").as_uri()),
    )
    fresh = SilverLake(silver_lake.spark, once)
    fresh.run_gold(J2)
    assert snapshot(silver_lake) == snapshot(fresh)


def test_a_run_that_fails_a_check_leaves_the_state_of_the_previous_run(silver_lake):
    run = published_j1(silver_lake)
    silver_lake.orders(J2, order("T1", "2026-01-01T08:00:00", day=J2))

    with pytest.raises(GoldCheckError):
        silver_lake.run_gold(J2)

    assert read_state(silver_lake.spark, silver_lake.cfg.paths.gold) == State(
        Status.PUBLISHED, run.run_id, (J1,)
    )


@pytest.mark.parametrize("checksum", ["kept", "removed"])
def test_an_unreadable_state_reads_as_absent(spark, tmp_path, checksum):
    """ADR-0002: an unreadable ``_state.json`` reads as absent, so gold is not taken for
    published. Damaged on disk, its content no longer matches its checksum (review A, F6); torn
    by an earlier version, it holds part of a JSON object."""
    gold = str(tmp_path / "gold")
    write_state(spark, gold, State(Status.PUBLISHED, "run", (J1,)))
    state = tmp_path / "gold" / "_state.json"
    state.write_text('{"status": "PUBLISHING", "run_id": "other", "silver_days": [')
    if checksum == "removed":
        state.with_name(f".{state.name}.crc").unlink()

    assert read_state(spark, gold) is None
