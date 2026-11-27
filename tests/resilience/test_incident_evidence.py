"""The numbers of the incident report of 2026-11-27, recomputed from its committed evidence.

Review A of PR #24 found counts the committed files did not hold; each test ties a claim of
``docs/incidents/2026-11-27-perte-datanode.md`` to the files it comes from."""

import csv
import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
REPORT = REPO / "docs" / "incidents" / "2026-11-27-perte-datanode.md"
FIRST = REPORT.with_suffix("")
SECOND = FIRST / "ecriture-sous-panne"


def load(name):
    spec = importlib.util.spec_from_file_location(
        name, REPO / "scripts" / "resilience" / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def allocations():
    return load("allocations")


def retries(allocations, log, address, start, end):
    with log.open() as lines:
        found = allocations.parse(lines)
    return allocations.retries(found, address, allocations.seconds(start), allocations.seconds(end))


def test_the_four_driver_stalls_of_the_first_replay_come_from_the_namenode_log(allocations):
    found = retries(
        allocations,
        FIRST / "namenode-allocations-replay.log",
        "172.22.0.4",
        "07:44:17.856",
        "07:45:49.352",
    )

    stalls = [round(r.delay, 1) for r in found if ".lock" in r.first.path and r.delay >= 1]
    assert len(found) == 44
    assert stalls == [65.0, 2.1, 9.2, 60.1]
    assert round(sum(stalls), 1) == 136.4


def test_the_committed_client_logs_hold_one_exclusion_per_block_asked_again():
    logs = ("gold-replay-driver-hdfs.log", "gold-replay-executors-hdfs.log")
    excluded = [(FIRST / log).read_text().count("Excluding datanode") for log in logs]
    assert excluded == [12, 32]


def test_160_fact_sales_files_of_the_second_exercise_were_written_on_two_datanodes(allocations):
    log = SECOND / "namenode-allocations.log"
    asked_again = [
        r
        for r in retries(allocations, log, "172.22.0.12", "11:44:00.244", "11:45:39.762")
        if r.again
    ]
    with log.open() as lines:
        after_death = {
            a.path
            for a in allocations.parse(lines)
            if "_fact_sales.staging-" in a.path
            and a.seconds >= allocations.seconds("11:45:39.762")
            and len(a.replicas) == 2
        }

    assert len(asked_again) == 72
    assert all("_fact_sales.staging-" in r.first.path for r in asked_again)
    assert all("172.22.0.12" not in r.again.replicas for r in asked_again)
    assert len(after_death - {r.first.path for r in asked_again}) == 88


def test_the_gold_written_during_the_failure_has_the_published_fingerprint():
    prints = [
        (SECOND / f"fingerprint-{name}.json").read_bytes()
        for name in ("scratch", "datalake", "baseline")
    ]

    assert prints[0] == prints[1] == prints[2]
    fingerprint = json.loads(prints[0])
    assert len(fingerprint["tables"]) == 14
    assert len(fingerprint["fact_sales_partitions"]) == 367
    assert sum(t["rows"] for t in fingerprint["tables"].values()) == 1_128_995


def test_the_spare_received_5571_replicas_and_3_erasure_coded_units():
    with (FIRST / "timeline.csv").open() as file:
        done = next(r for r in csv.DictReader(file) if r["time"] == "2026-11-27T08:57:26Z")

    assert int(done["datanode4_blocks"]) == 5571 + 3
    assert "5 571 répliques et 3 unités" in REPORT.read_text(encoding="utf-8")


def test_the_final_nominal_report_does_not_trip_the_dead_node_alert():
    """Action 2 alerts on ``NumDeadDataNodes - NumDecomDeadDataNodes > 0``."""
    nodes = load("hdfs_watch").parse_report((FIRST / "dfsadmin-nominal.txt").read_text()).nodes

    assert [node.hostname for node in nodes if node.live] == ["datanode1", "datanode2", "datanode3"]
    assert [node for node in nodes if not node.live] == []
