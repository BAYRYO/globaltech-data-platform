"""KAN-26 and KAN-27: rules evaluated in one pass over bronze, and a run that fails, with its
counts in the audit, when the records of a day do not add up."""

import json
import shutil
from datetime import date

import pytest
from pyspark.sql import functions as F

from gtdp.jobs import silver as silver_job
from gtdp.silver.pipelines import PIPELINES, classified, rejects_path
from gtdp.silver.pseudonymize import salt_frame
from gtdp.silver.quality import DISPOSITION, rejects

from .lakes import DAYS, local

pytestmark = pytest.mark.slow

J1, J2 = DAYS[0], DAYS[1]


def leaves(frame) -> list[str]:
    """The leaf nodes of the physical plan of ``frame``: what it reads."""
    found = frame._jdf.queryExecution().sparkPlan().collectLeaves()
    return [found.apply(i).nodeName() for i in range(found.size())]


def test_the_rules_run_in_one_projection_and_the_outputs_read_it_once(published):
    salt = salt_frame(published.spark, published.cfg.pseudonymization.salt)
    records = classified(
        published.spark, published.cfg.paths, "orders", date.fromisoformat(J2), salt
    )
    optimized = records._jdf.queryExecution().optimizedPlan().toString()
    persisted = records.persist()
    try:
        outputs = {
            "rejects": rejects(persisted, ["transaction_id"], source="orders", run_id="r"),
            "clean": PIPELINES["orders"].build(persisted.where(F.col(DISPOSITION) == "clean")),
        }
        read = {name: leaves(frame) for name, frame in outputs.items()}
    finally:
        persisted.unpersist()

    assert optimized.count("AS reject_reasons") == 1
    assert all(optimized.count(f"{rule.code}") == 1 for rule in PIPELINES["orders"].entity.rules)
    assert "Scan parquet" in " ".join(leaves(records))
    assert read == {"rejects": ["InMemoryTableScan"], "clean": ["InMemoryTableScan"]}


def test_a_day_without_bronze_fails_and_publishes_nothing(fresh_lake):
    lake = fresh_lake
    shutil.rmtree(local(lake.cfg.paths.bronze) / "weblogs" / f"ingestion_date={J2}")
    lake.run("silver", J1)

    with pytest.raises(FileNotFoundError, match="run bronze"):
        lake.run("silver", J2, "weblogs")

    assert lake.audit()[-1].status == "FAILED"
    assert not (
        local(rejects_path(lake.cfg.paths.silver, "weblogs")) / f"ingestion_date={J2}"
    ).exists()


def test_records_that_do_not_add_up_fail_the_run_and_keep_their_counts(fresh_lake, monkeypatch):
    write = silver_job.write_partitions

    def losing_one_reject(frame, path, partitions):
        if "/rejects/" in path:
            frame = frame.limit(frame.count() - 1)
        write(frame, path, partitions)

    monkeypatch.setattr(silver_job, "write_partitions", losing_one_reject)

    with pytest.raises(silver_job.SilverCheckError, match="in silver/rejects"):
        fresh_lake.run("silver", J1, "orders")

    failed = fresh_lake.audit()[-1]
    report = json.loads(failed.metrics_json)["sources"]["orders"]
    assert failed.status == "FAILED"
    assert report["rejects_written"] == report["rejected"] - 1
    assert failed.rows_in == failed.rows_out + failed.rows_rejected + failed.rows_duplicated
