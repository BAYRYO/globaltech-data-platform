import hashlib

import pytest
from pyspark.sql import functions as F

pytestmark = pytest.mark.slow

J1, J2 = "2026-01-01", "2026-01-02"
SOURCES = ("orders", "customers", "products", "weblogs")


def content(lake, source, day):
    """Row count and checksum of the sorted record hashes of one partition."""
    hashes = sorted(
        row._record_hash
        for row in lake.table(source)
        .where(F.col("ingestion_date") == day)
        .select("_record_hash")
        .collect()
    )
    return len(hashes), hashlib.sha256("\n".join(hashes).encode()).hexdigest()


def files_and_mtimes(lake, source, day):
    partition = lake.partition(source, day)
    return {path.name: path.stat().st_mtime_ns for path in partition.iterdir()}


def test_replaying_a_day_twice_rewrites_only_its_partition_with_the_same_content(lake):
    lake.land(J1)
    lake.land(J2)
    lake.run(J1)
    first = lake.run(J2)
    once = {source: content(lake, source, J2) for source in SOURCES}
    j1_files = {source: files_and_mtimes(lake, source, J1) for source in SOURCES}

    lake.run(J2)
    last = lake.run(J2)

    for source in SOURCES:
        assert content(lake, source, J2) == once[source], source
        assert files_and_mtimes(lake, source, J1) == j1_files[source], source
        run_ids = lake.table(source).where(F.col("ingestion_date") == J2).select("_run_id")
        assert {row._run_id for row in run_ids.distinct().collect()} == {last.run_id}
    assert last.run_id != first.run_id
    assert [row.status for row in lake.audit()] == ["SUCCESS"] * 4


def test_an_earlier_day_can_be_replayed_after_a_later_one(lake):
    lake.land(J1)
    lake.land(J2)
    lake.run(J1)
    lake.run(J2)
    j2 = {source: content(lake, source, J2) for source in SOURCES}
    j1 = {source: content(lake, source, J1) for source in SOURCES}

    lake.run(J1)

    for source in SOURCES:
        assert content(lake, source, J1) == j1[source], source
        assert content(lake, source, J2) == j2[source], source
