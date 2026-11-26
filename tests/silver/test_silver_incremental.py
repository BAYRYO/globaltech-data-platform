"""KAN-28: duplicates within a day and across days, idempotent replays, and the order guard."""

import hashlib

import pytest
from pyspark.sql import functions as F

from gtdp.io import LaterPartitionError
from gtdp.silver.pipelines import PIPELINES, rejects_path, table_path

from .lakes import DAYS, local

pytestmark = pytest.mark.slow

J1, J2, J3 = DAYS


def content(frame) -> str:
    """Checksum of the rows of ``frame``, in any order."""
    rows = sorted(repr(tuple(row)) for row in frame.collect())
    return hashlib.sha256("\n".join(rows).encode()).hexdigest()


def silver_state(lake) -> dict[str, str]:
    """Checksums of every silver table and rejects table, run ids of the rejects aside."""
    state = {source: content(lake.table(source)) for source in PIPELINES}
    for source in PIPELINES:
        state[f"rejects/{source}"] = content(lake.rejects(source).drop("_run_id"))
    return state


def files(lake, path: str, day: str) -> dict[str, int]:
    partition = local(path) / f"ingestion_date={day}"
    return {item.name: item.stat().st_mtime_ns for item in partition.iterdir()}


def test_replaying_a_day_rewrites_the_same_silver_and_only_its_partitions(fresh_lake):
    lake, silver = fresh_lake, fresh_lake.cfg.paths.silver
    lake.run("silver", J1)
    lake.run("silver", J2)
    before = silver_state(lake)
    j1_files = {
        path: files(lake, path, J1)
        for path in (table_path(silver, "orders"), rejects_path(silver, "orders"))
    }

    lake.run("silver", J2)
    lake.run("silver", J2)

    assert silver_state(lake) == before
    assert {path: files(lake, path, J1) for path in j1_files} == j1_files


def test_a_day_before_a_published_day_is_refused_unless_forced(fresh_lake):
    lake = fresh_lake
    for day in DAYS:
        lake.run("silver", day)
    published = silver_state(lake)

    with pytest.raises(LaterPartitionError, match="2026-01-03"):
        lake.run("silver", J2)
    refused = lake.audit()[-1]
    unchanged = silver_state(lake)
    lake.run("silver", J2, force=True)
    lake.run("silver", J3)

    assert (refused.status, refused.ingestion_date.isoformat()) == ("FAILED", J2)
    assert unchanged == published
    assert silver_state(lake) == published


def test_a_resent_order_counts_as_a_cross_day_duplicate_and_appears_once(published):
    orders = published.table("orders")
    j1_ids = orders.where(F.col("ingestion_date") == J1).select("transaction_id")
    later = orders.where(F.col("ingestion_date") > J1).select("transaction_id")
    resends = {
        day: sum(
            f["anomalies"].get("DUPLICATE_CROSS_DAY", 0)
            for f in published.manifest(day)["files"]
            if f["source"] == "orders"
        )
        for day in (J2, J3)
    }

    assert orders.count() == orders.select("transaction_id").distinct().count()
    assert later.join(j1_ids, "transaction_id").count() == 0
    assert resends[J2] > 0
    assert resends[J3] > 0
