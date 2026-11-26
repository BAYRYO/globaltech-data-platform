"""Two runs writing one table, and runs stopped or killed in the middle of a write."""

import json
import logging
import os
import shutil
import signal
import threading
import time
from datetime import date

import pytest

import gtdp.io
from gtdp.audit import RunStopped
from gtdp.io import (
    INGESTION_DATE,
    RUN_ID_PROPERTY,
    TableLockedError,
    drop_partitions,
    list_partition_values,
    path_exists,
    read_table,
    replace_table,
    table_lock,
    write_partitions,
)

pytestmark = pytest.mark.slow

SCHEMA = "id int, ingestion_date string"
JAN_1, JAN_2 = date(2026, 1, 1), date(2026, 1, 2)
DAY_OLD = 24 * 3600


def rows(spark, path):
    return sorted(tuple(r) for r in spark.read.parquet(path).collect())


def make_old(path):
    old = time.time() - DAY_OLD
    os.utime(path, (old, old))


@pytest.fixture
def orders(spark, tmp_path):
    """A table with days 1 and 2 published, and a day 2 replay ready to write."""
    path = str(tmp_path / "orders")
    write_partitions(
        spark.createDataFrame([(1, "2026-01-01"), (2, "2026-01-02")], SCHEMA),
        path,
        {INGESTION_DATE: [JAN_1, JAN_2]},
    )
    replay = spark.createDataFrame([(3, "2026-01-02"), (4, "2026-01-02")], SCHEMA)
    return path, replay


def test_a_second_writer_fails_at_once_while_the_first_swaps_and_the_table_stays_readable(
    spark, orders, tmp_path, monkeypatch
):
    path, replay = orders
    real_rename, inside, resume = gtdp.io._rename, threading.Event(), threading.Event()

    def held_rename(fs, source, destination):
        inside.set()  # the published day 2 is deleted, its new copy not yet renamed
        assert resume.wait(60)
        real_rename(fs, source, destination)

    monkeypatch.setattr(gtdp.io, "_rename", held_rename)
    errors = []

    def first_run():
        spark.sparkContext.setLocalProperty(RUN_ID_PROPERTY, "run-a")
        try:
            write_partitions(replay, path, {INGESTION_DATE: [JAN_2]})
        except Exception as exc:
            errors.append(exc)

    first = threading.Thread(target=first_run)
    first.start()
    try:
        assert inside.wait(60)
        lock = json.loads((tmp_path / "_orders.lock").read_text())
        with pytest.raises(TableLockedError, match="run-a"):
            write_partitions(replay.limit(1), path, {INGESTION_DATE: [JAN_2]})
    finally:
        resume.set()
        first.join(60)

    assert errors == []
    assert lock["run_id"] == "run-a"
    assert {"application_id", "locked_at"} <= lock.keys()
    assert rows(spark, path) == [(1, JAN_1), (3, JAN_2), (4, JAN_2)]
    assert sorted(os.listdir(tmp_path)) == ["orders"]


def test_a_partition_is_never_renamed_into_an_existing_one(spark, orders, tmp_path, monkeypatch):
    path, replay = orders
    real_rename = gtdp.io._rename

    def racing_rename(fs, source, destination):
        # Another writer, which took no lock, publishes day 2 between the delete and the rename.
        shutil.copytree(source.toUri().getPath(), destination.toUri().getPath())
        real_rename(fs, source, destination)

    monkeypatch.setattr(gtdp.io, "_rename", racing_rename)

    with pytest.raises(FileExistsError, match="ingestion_date=2026-01-02"):
        write_partitions(replay, path, {INGESTION_DATE: [JAN_2]})

    assert rows(spark, path) == [(1, JAN_1), (3, JAN_2), (4, JAN_2)]
    day_2 = tmp_path / "orders" / "ingestion_date=2026-01-02"
    assert [p.name for p in day_2.iterdir() if p.name.startswith(INGESTION_DATE)] == []
    assert sorted(os.listdir(tmp_path)) == ["orders"]


@pytest.mark.parametrize("write", ["write_partitions", "replace_table"])
def test_a_live_lock_fails_the_writer_at_once_and_stays(spark, orders, tmp_path, write):
    path, replay = orders
    lock = tmp_path / "_orders.lock"
    lock.write_text('{"run_id": "run-b"}')

    with pytest.raises(TableLockedError, match="run-b"):
        if write == "write_partitions":
            write_partitions(replay, path, {INGESTION_DATE: [JAN_2]})
        else:
            replace_table(replay, path)

    assert lock.read_text() == '{"run_id": "run-b"}'
    assert rows(spark, path) == [(1, JAN_1), (2, JAN_2)]


def test_a_lock_left_by_a_dead_run_is_taken_over(spark, orders, tmp_path, caplog):
    path, replay = orders
    lock = tmp_path / "_orders.lock"
    lock.write_text('{"run_id": "run-killed"}')
    make_old(lock)

    with caplog.at_level(logging.WARNING, logger="gtdp.io"):
        write_partitions(replay, path, {INGESTION_DATE: [JAN_2]})

    assert "run-killed" in caplog.text
    assert rows(spark, path) == [(1, JAN_1), (3, JAN_2), (4, JAN_2)]
    assert sorted(os.listdir(tmp_path)) == ["orders"]


def test_staging_left_by_a_killed_run_is_removed_by_the_next_writer(spark, orders, tmp_path):
    path, replay = orders
    dead, live = tmp_path / "_orders.staging-dead", tmp_path / "_orders.staging-live"
    for staging in (dead, live):
        (staging / "ingestion_date=2026-01-02").mkdir(parents=True)
    make_old(dead)

    write_partitions(replay, path, {INGESTION_DATE: [JAN_2]})

    assert sorted(os.listdir(tmp_path)) == ["_orders.staging-live", "orders"]


def test_a_stop_signal_during_the_swap_waits_for_its_end(spark, orders, tmp_path, monkeypatch):
    path, replay = orders
    real_rename = gtdp.io._rename

    def stop_then_rename(fs, source, destination):
        os.kill(os.getpid(), signal.SIGTERM)  # after the delete of the published day 2
        real_rename(fs, source, destination)

    def raise_stopped(signum, _frame):
        raise RunStopped(signal.Signals(signum))

    monkeypatch.setattr(gtdp.io, "_rename", stop_then_rename)
    previous = signal.signal(signal.SIGTERM, raise_stopped)
    try:
        with pytest.raises(RunStopped):
            write_partitions(replay, path, {INGESTION_DATE: [JAN_2]})
    finally:
        signal.signal(signal.SIGTERM, previous)

    assert rows(spark, path) == [(1, JAN_1), (3, JAN_2), (4, JAN_2)]
    assert sorted(os.listdir(tmp_path)) == ["orders"]


def test_drop_partitions_deletes_the_named_partitions_that_exist(spark, orders, tmp_path):
    path, _ = orders

    assert drop_partitions(spark, path, {INGESTION_DATE: [JAN_2, date(2026, 1, 3)]}) == [
        "ingestion_date=2026-01-02"
    ]

    assert list_partition_values(spark, path, INGESTION_DATE) == ["2026-01-01"]
    (tmp_path / "_orders.lock").write_text("{}")
    assert drop_partitions(spark, path, {INGESTION_DATE: [JAN_2]}) == []


def test_a_job_holding_the_table_lock_keeps_writers_out_until_it_releases_it(
    spark, orders, tmp_path
):
    path, replay = orders

    with table_lock(spark, path):
        with pytest.raises(TableLockedError):
            write_partitions(replay, path, {INGESTION_DATE: [JAN_2]})
        assert (tmp_path / "_orders.lock").exists()
    write_partitions(replay, path, {INGESTION_DATE: [JAN_2]})

    assert rows(spark, path) == [(1, JAN_1), (3, JAN_2), (4, JAN_2)]
    assert sorted(os.listdir(tmp_path)) == ["orders"]


def test_a_reader_leaves_a_locked_swap_to_its_writer(spark, tmp_path):
    gold = tmp_path / "gold"
    path = str(gold / "dim")
    replace_table(spark.createDataFrame([(1, "v1")], SCHEMA), path)
    os.rename(gold / "dim", gold / "_dim.old")  # a writer between its two renames
    (gold / "_dim.lock").write_text("{}")

    assert not path_exists(spark, path)

    (gold / "_dim.lock").unlink()
    assert [tuple(r) for r in read_table(spark, path).collect()] == [(1, "v1")]
