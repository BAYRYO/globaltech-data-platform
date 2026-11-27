import importlib.util
import io
import json
import re
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


def test_a_dead_node_counts_dead_and_its_lost_replicas_under_replicated(watch):
    row = sample(watch, "report-dead.txt")

    assert (row["live"], row["dead"]) == (2, 1)
    assert (row["datanode1"], row["datanode1_blocks"]) == ("dead", 0)
    assert (row["datanode2"], row["datanode3"]) == ("live", "live")
    assert row["under_replicated"] == 5551
    assert row["pending_deletion"] == 102
    assert (row["ec_low_redundancy"], row["missing"], row["ec_missing"]) == (3, 0, 0)


def test_a_decommissioning_node_counts_once_though_the_report_lists_it_twice(watch):
    row = sample(watch, "report-decommissioning.txt")

    assert (row["live"], row["dead"], row["decommissioning"]) == (4, 0, 1)
    assert (row["datanode4"], row["datanode4_blocks"]) == ("live/decommissioning", 5574)
    assert (row["datanode3"], row["datanode3_blocks"]) == ("live", 5575)


def test_a_stopped_decommissioned_spare_is_dead_and_decommissioned(watch):
    row = sample(watch, "report-spare-decommissioned.txt")

    assert (row["live"], row["dead"], row["decommissioning"]) == (3, 1, 0)
    assert row["datanode4"] == "dead/decommissioned"
    assert [row[f"datanode{n}"] for n in (1, 2, 3)] == ["live", "live", "live"]
    assert row["under_replicated"] == row["ec_low_redundancy"] == row["missing"] == 0


def test_a_host_listed_live_and_dead_counts_as_live(watch):
    dead_section = (DATA / "report-dead.txt").read_text().split("Dead datanodes")[1]
    text = (DATA / "report-nominal.txt").read_text() + "Dead datanodes" + dead_section

    row = watch.row(watch.parse_report(text), NAMENODE, HOSTS)

    assert (row["live"], row["dead"]) == (3, 1)
    assert (row["datanode1"], row["datanode1_blocks"]) == ("live", 5556)


def numbered(text: str) -> str:
    """The report with each counter line set to a value of its own: 101, 102... under
    ``Replicated Blocks``, 201, 202... under ``Erasure Coded Block Groups``."""
    lines, group, position = [], 0, 0
    for raw in text.splitlines():
        line = raw
        if raw.strip() in ("Replicated Blocks:", "Erasure Coded Block Groups:"):
            group, position = group + 1, 0
        elif raw.startswith("-----"):
            group = 0
        elif group and raw.startswith("\t"):
            position += 1
            line = f"{raw.partition(':')[0]}: {100 * group + position}"
        lines.append(line)
    return "\n".join(lines) + "\n"


def test_each_counter_reads_its_own_line_of_the_report(watch):
    text = numbered((DATA / "report-dead.txt").read_text())

    row = watch.row(watch.parse_report(text), NAMENODE, HOSTS)

    assert {column: row[column] for column in watch.COUNTERS} == {
        "under_replicated": 101,
        "corrupt": 102,
        "missing": 103,
        "pending_deletion": 106,
        "ec_low_redundancy": 201,
        "ec_corrupt": 202,
        "ec_missing": 203,
    }


def test_a_report_without_a_counter_line_gives_an_error_sample(watch, monkeypatch):
    text = re.sub(r"(?m)^\tMissing blocks: 0\n", "", (DATA / "report-nominal.txt").read_text())
    monkeypatch.setattr(watch, "dfsadmin_report", lambda: watch.parse_report(text))
    metrics = json.dumps({"beans": [NAMENODE]}).encode()
    monkeypatch.setattr(watch.urllib.request, "urlopen", lambda url, timeout: io.BytesIO(metrics))

    assert watch.sample(HOSTS) == {"error": "KeyError: 'Missing blocks'"}


def test_state_prints_one_host_from_one_report(watch, monkeypatch, capsys):
    report = watch.parse_report((DATA / "report-spare-decommissioned.txt").read_text())
    monkeypatch.setattr(watch, "dfsadmin_report", lambda: report)

    for host in ("datanode4", "datanode1", "datanode9"):
        assert watch.main(["state", host]) == 0

    assert capsys.readouterr().out.split() == ["dead/decommissioned", "live", "absent"]


def test_the_header_names_every_column_of_a_row(watch):
    row = sample(watch, "report-nominal.txt")
    assert watch.header(HOSTS) == ["time", *row, "error"]


def test_poll_ends_lines_with_lf_so_shell_tools_read_the_last_column(watch, tmp_path, monkeypatch):
    stop = tmp_path / "stop"

    def one_sample(hosts):
        stop.touch()
        return sample(watch, "report-nominal.txt")

    monkeypatch.setattr(watch, "sample", one_sample)
    out = tmp_path / "timeline.csv"

    watch.poll(out, HOSTS, interval=0, stop=stop, max_seconds=60)

    lines = out.read_bytes().split(b"\n")
    assert b"\r" not in out.read_bytes()
    assert lines[0].endswith(b",error") and lines[1].endswith(b",")
    assert lines[2] == b""


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


def test_a_sample_taken_in_the_second_of_its_event_counts(watch):
    rows = timeline({"time": at(30)})

    lines = watch.summarize(rows, {"victim_started": at(30)}, "datanode1", "datanode4")

    assert lines[0] == ("victim live again", "victim_started", at(30), 0)


def test_the_spare_milestones_wait_for_the_decommission_to_end_and_a_decommissioned_death(watch):
    rows = timeline(
        {"time": at(0), "datanode4": "live/decommissioning"},
        {"time": at(60), "datanode4": "live/decommissioned"},
        {"time": at(130), "datanode4": "live/decommissioned"},
        {"time": at(220), "datanode4": "dead/decommissioned"},
    )
    events = {"decommission_started": at(0), "spare_stopped": at(125)}

    lines = watch.summarize(rows, events, "datanode1", "datanode4")

    assert lines == [
        ("spare decommissioned", "decommission_started", at(60), 60),
        ("spare dead", "spare_stopped", at(220), 95),
    ]


def test_a_spare_dead_before_its_decommission_ended_is_not_a_retired_spare(watch):
    rows = timeline(
        {"time": at(10), "datanode4": "live/decommissioning"},
        {"time": at(100), "datanode4": "dead"},
    )

    lines = watch.summarize(rows, {"spare_stopped": at(5)}, "datanode1", "datanode4")

    assert lines == [("spare dead", "spare_stopped", "", None)]
