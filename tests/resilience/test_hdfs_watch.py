import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DATA = Path(__file__).parent / "data"
HOSTS = ("datanode1", "datanode2", "datanode3", "datanode4")
NAMENODE = {"PendingReconstructionBlocks": 4, "ExcessBlocks": 0}


@pytest.fixture(scope="module")
def watch():
    spec = importlib.util.spec_from_file_location(
        "hdfs_watch", REPO / "scripts" / "resilience" / "hdfs_watch.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def sample(watch, name):
    report = watch.parse_report((DATA / name).read_text())
    return watch.row(report, NAMENODE, HOSTS)


def test_a_nominal_report_gives_three_live_nodes_and_no_low_redundancy_block(watch):
    assert sample(watch, "report-nominal.txt") == {
        "live": 3,
        "dead": 0,
        "decommissioning": 0,
        "under_replicated": 0,
        "missing": 0,
        "corrupt": 0,
        "pending_deletion": 0,
        "ec_low_redundancy": 0,
        "ec_missing": 0,
        "ec_corrupt": 0,
        "pending_reconstruction": 4,
        "excess": 0,
        "datanode1": "live",
        "datanode1_blocks": 5556,
        "datanode2": "live",
        "datanode2_blocks": 5556,
        "datanode3": "live",
        "datanode3_blocks": 5555,
        "datanode4": "absent",
        "datanode4_blocks": "",
    }


def test_the_header_names_every_column_of_a_row(watch):
    row = sample(watch, "report-nominal.txt")
    assert watch.header(HOSTS) == ["time", *row, "error"]


def at(seconds: int) -> str:
    return f"2026-11-27T10:{seconds // 60:02d}:{seconds % 60:02d}Z"


def timeline(*samples: dict[str, str]) -> list[dict[str, str]]:
    base = {
        "error": "",
        "under_replicated": "0",
        "missing": "0",
        "ec_low_redundancy": "0",
        "ec_missing": "0",
        "excess": "0",
        "pending_deletion": "0",
        "datanode1": "live",
        "datanode1_blocks": "100",
        "datanode4": "absent",
        "datanode4_blocks": "",
    }
    return [base | s for s in samples]


def test_milestones_count_from_their_event_to_the_first_sample_that_reaches_them(watch):
    rows = timeline(
        {"time": at(0)},
        {"time": at(95), "datanode1": "dead", "under_replicated": "3"},
        {"time": at(100), "datanode1": "dead", "error": "IOException: refused"},
        {"time": at(120), "datanode1": "dead", "datanode1_blocks": "0", "under_replicated": "90"},
        {"time": at(300), "datanode1": "dead", "datanode1_blocks": "0", "under_replicated": "0"},
    )
    events = {"victim_stopped": at(5), "spare_started": at(200)}

    lines = watch.summarize(rows, events, "datanode1", "datanode4")

    assert lines == [
        ("victim dead in the report", "victim_stopped", at(95), 90),
        ("victim replicas removed", "victim_stopped", at(120), 115),
        ("spare live", "spare_started", "", None),
        ("no low-redundancy block left", "spare_started", at(300), 100),
    ]


def test_no_excess_replica_counts_only_once_the_victim_reported_its_blocks(watch):
    rows = timeline(
        {"time": at(10), "datanode1": "dead", "datanode1_blocks": "0"},
        {"time": at(15), "datanode1": "live", "datanode1_blocks": "0"},
        {"time": at(20), "datanode1": "live", "excess": "70"},
        {"time": at(40), "datanode1": "live"},
    )

    lines = watch.summarize(rows, {"victim_started": at(8)}, "datanode1", "datanode4")

    assert lines == [
        ("victim live again", "victim_started", at(15), 7),
        ("excess replicas deleted", "victim_started", at(40), 32),
    ]


def test_peak_gives_the_first_sample_of_the_highest_value(watch):
    rows = timeline(
        {"time": at(0), "under_replicated": "4"},
        {"time": at(5), "under_replicated": "12"},
        {"time": at(10), "under_replicated": "12"},
        {"time": at(15), "under_replicated": "", "error": "TimeoutExpired"},
    )
    assert watch.peak(rows, "under_replicated") == (at(5), 12)
