"""Watch HDFS through a DataNode failure: one CSV row of ``hdfs dfsadmin -report`` and NameNode
counters every few seconds, a wait on a JSON state file, and the milestones of a run.

    hdfs_watch.py poll --out timeline.csv --hosts datanode1,datanode2 --stop-file stop
    hdfs_watch.py wait-state /datalake/gold/_state.json PUBLISHING --timeout 600
    hdfs_watch.py summarize timeline.csv events.csv --victim datanode1 --spare datanode4

Runs in the cluster image (``scripts/cluster/client.sh``) with ``HADOOP_USER_NAME=hadoop``.

``poll`` writes a ``time`` column (UTC), the DataNode counts, the block counters of the report,
two NameNode counters the report does not show (``pending_reconstruction``: replicas asked of a
DataNode and not yet confirmed, ``excess``: replicas above the replication factor, waiting for
deletion), then for each host its state (``live``, ``dead``, ``live/decommissioning``,
``dead/decommissioned``, ``absent``...) and its block count. A sample that fails fills ``error``
only. It stops when the stop file exists or after ``--max-seconds``.

``summarize`` reads that timeline and the ``time,event`` lines of the scenario
(``scripts/resilience/datanode-failure.sh``) and prints when each milestone was first sampled.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

NAMENODE_HTTP = "http://namenode:9870"
FSNAMESYSTEM = "Hadoop:service=NameNode,name=FSNamesystem"
SECTION = re.compile(r"(?P<kind>Live|Dead) datanodes \(\d+\):")
GROUPS = {"Replicated Blocks:": "replicated", "Erasure Coded Block Groups:": "erasure_coded"}
ADMIN_STATES = {
    "Normal": "",
    "Decommission in progress": "decommissioning",
    "Decommissioned": "decommissioned",
    "Entering Maintenance": "entering-maintenance",
    "In Maintenance": "in-maintenance",
}
COUNTERS = {
    "under_replicated": ("replicated", "Under replicated blocks"),
    "missing": ("replicated", "Missing blocks"),
    "corrupt": ("replicated", "Blocks with corrupt replicas"),
    "pending_deletion": ("replicated", "Pending deletion blocks"),
    "ec_low_redundancy": ("erasure_coded", "Low redundancy block groups"),
    "ec_missing": ("erasure_coded", "Missing block groups"),
    "ec_corrupt": ("erasure_coded", "Block groups with corrupt internal blocks"),
}
NAMENODE_METRICS = {
    "pending_reconstruction": "PendingReconstructionBlocks",
    "excess": "ExcessBlocks",
}
COLUMNS = ("live", "dead", "decommissioning", *COUNTERS, *NAMENODE_METRICS)


@dataclass(frozen=True)
class Node:
    hostname: str
    live: bool
    admin_state: str
    """``Decommission Status`` of the report: ``Normal``, ``Decommissioned``..."""
    blocks: int

    @property
    def state(self) -> str:
        admin = ADMIN_STATES.get(self.admin_state, self.admin_state.lower().replace(" ", "-"))
        return ("live" if self.live else "dead") + (f"/{admin}" if admin else "")


@dataclass(frozen=True)
class Report:
    counters: dict[str, dict[str, int]]
    """Group (``replicated``, ``erasure_coded``) to report line to value."""
    nodes: tuple[Node, ...]


def parse_report(text: str) -> Report:
    """The block counters and the DataNodes of ``hdfs dfsadmin -report``."""
    counters: dict[str, dict[str, int]] = {group: {} for group in GROUPS.values()}
    fields: list[tuple[bool, dict[str, str]]] = []
    group = live = None
    for raw in text.splitlines():
        line = raw.strip()
        if line in GROUPS:
            group = GROUPS[line]
        elif section := SECTION.fullmatch(line):
            group, live = None, section["kind"] == "Live"
        elif ":" in line:
            key, _, value = (part.strip() for part in line.partition(":"))
            if group and raw.startswith("\t"):
                counters[group][key] = int(value)
            elif key == "Name" and live is not None:
                fields.append((live, {}))
            elif fields:
                fields[-1][1][key] = value
    nodes = tuple(
        Node(f["Hostname"], is_live, f["Decommission Status"], int(f["Num of Blocks"]))
        for is_live, f in fields
    )
    return Report(counters, nodes)


def row(report: Report, metrics: dict[str, int], hosts: Sequence[str]) -> dict[str, object]:
    """The timeline columns of one sample, ``hosts`` in that order. A host listed live and dead,
    re-registered under a new address, counts as live."""
    nodes = {node.hostname: node for node in sorted(report.nodes, key=lambda n: n.live)}
    values: dict[str, object] = {
        "live": sum(node.live for node in report.nodes),
        "dead": sum(not node.live for node in report.nodes),
        "decommissioning": sum(n.admin_state == "Decommission in progress" for n in report.nodes),
        **{column: report.counters[g][line] for column, (g, line) in COUNTERS.items()},
        **{column: metrics[name] for column, name in NAMENODE_METRICS.items()},
    }
    for host in hosts:
        node = nodes.get(host)
        values[host] = node.state if node else "absent"
        values[f"{host}_blocks"] = node.blocks if node else ""
    return values


def header(hosts: Sequence[str]) -> list[str]:
    return ["time", *COLUMNS, *(f for host in hosts for f in (host, f"{host}_blocks")), "error"]


def now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def sample(hosts: Sequence[str]) -> dict[str, object]:
    try:
        report = subprocess.run(
            ["hdfs", "dfsadmin", "-report"], capture_output=True, text=True, check=True, timeout=60
        ).stdout
        url = f"{NAMENODE_HTTP}/jmx?qry={FSNAMESYSTEM}"
        with urllib.request.urlopen(url, timeout=10) as resp:
            metrics = json.load(resp)["beans"][0]
        return row(parse_report(report), metrics, hosts)
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, IndexError) as exc:
        return {"error": f"{type(exc).__name__}: {exc}"[:300]}


def poll(out: Path, hosts: Sequence[str], interval: float, stop: Path, max_seconds: float) -> int:
    deadline = time.monotonic() + max_seconds
    with out.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=header(hosts))
        writer.writeheader()
        while not stop.exists() and time.monotonic() < deadline:
            started = time.monotonic()
            writer.writerow({"time": now(), **sample(hosts)})
            file.flush()
            time.sleep(max(0.0, interval - (time.monotonic() - started)))
    return 0


def wait_state(path: str, status: str, timeout: float) -> int:
    """Wait until the JSON file at ``path`` says ``"status": status``; 1 on timeout."""
    user = os.environ.get("HADOOP_USER_NAME", "etl")
    url = f"{NAMENODE_HTTP}/webhdfs/v1{path}?op=OPEN&user.name={user}"
    print(f"{now()} watching {path} for {status}", flush=True)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                state = json.load(resp)
        except (OSError, ValueError):
            state = {}
        if state.get("status") == status:
            print(f"{now()} {path} is {status}", flush=True)
            return 0
        time.sleep(0.2)
    print(f"{now()} {path} not {status} after {timeout:.0f} s", flush=True)
    return 1


@dataclass(frozen=True)
class Milestone:
    name: str
    after: str
    """The event the delay counts from."""
    reached: Callable[[dict[str, str]], bool]


def milestones(victim: str, spare: str) -> tuple[Milestone, ...]:
    def state(host: str, *values: str) -> Callable[[dict[str, str]], bool]:
        return lambda r: r[host] in values

    def zero(*columns: str) -> Callable[[dict[str, str]], bool]:
        return lambda r: all(r[c] == "0" for c in columns)

    def reported_back(r: dict[str, str]) -> bool:
        """The victim is live and its block report is in: before it, no replica is excess."""
        return r[victim] == "live" and r[f"{victim}_blocks"] not in ("", "0")

    return (
        Milestone("victim dead in the report", "victim_stopped", state(victim, "dead")),
        Milestone(
            "victim replicas removed",
            "victim_stopped",
            lambda r: r[victim] == "dead" and r[f"{victim}_blocks"] == "0",
        ),
        Milestone("spare live", "spare_started", state(spare, "live")),
        Milestone(
            "no low-redundancy block left",
            "spare_started",
            zero("under_replicated", "ec_low_redundancy", "missing", "ec_missing"),
        ),
        Milestone("victim live again", "victim_started", state(victim, "live")),
        Milestone(
            "excess replicas deleted",
            "victim_started",
            lambda r: reported_back(r) and zero("excess", "pending_deletion")(r),
        ),
        Milestone(
            "spare decommissioned", "decommission_started", state(spare, "live/decommissioned")
        ),
        Milestone("spare dead", "spare_stopped", state(spare, "dead/decommissioned")),
    )


def _seconds(start: str, end: str) -> int:
    return round((datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds())


def summarize(
    rows: list[dict[str, str]], events: dict[str, str], victim: str, spare: str
) -> list[tuple[str, str, str, int | None]]:
    """``(milestone, event, first sample time, seconds after the event)`` for each milestone
    whose event happened; time and seconds empty when no later sample reached it."""
    sampled = [r for r in rows if not r["error"]]
    lines: list[tuple[str, str, str, int | None]] = []
    for milestone in milestones(victim, spare):
        start = events.get(milestone.after)
        if start is None:
            continue
        later = (r for r in sampled if r["time"] >= start and milestone.reached(r))
        first = next(later, None)
        lines.append(
            (
                milestone.name,
                milestone.after,
                first["time"] if first else "",
                _seconds(start, first["time"]) if first else None,
            )
        )
    return lines


def peak(rows: list[dict[str, str]], column: str) -> tuple[str, int] | None:
    """The first sample time of the highest value of ``column``, and that value."""
    best = max((r for r in rows if not r["error"]), key=lambda r: int(r[column]), default=None)
    return (best["time"], int(best[column])) if best else None


def _read_events(path: Path) -> dict[str, str]:
    events: dict[str, str] = {}
    with path.open() as file:
        for when, name in csv.reader(file):
            events.setdefault(name, when)
    return events


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("poll", help="write one timeline row every --interval seconds")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--hosts", required=True, help="DataNode hostnames, comma-separated")
    p.add_argument("--interval", type=float, default=5.0)
    p.add_argument("--stop-file", type=Path, required=True)
    p.add_argument("--max-seconds", type=float, default=4 * 3600)
    w = commands.add_parser("wait-state", help="wait for a status in a JSON file of HDFS")
    w.add_argument("path")
    w.add_argument("status")
    w.add_argument("--timeout", type=float, default=600)
    s = commands.add_parser("summarize", help="print the milestones of a scenario run")
    s.add_argument("timeline", type=Path)
    s.add_argument("events", type=Path)
    s.add_argument("--victim", required=True)
    s.add_argument("--spare", required=True)
    args = parser.parse_args(argv)
    if args.command == "poll":
        hosts = args.hosts.split(",")
        return poll(args.out, hosts, args.interval, args.stop_file, args.max_seconds)
    if args.command == "wait-state":
        return wait_state(args.path, args.status, args.timeout)
    with args.timeline.open(newline="") as file:
        rows = list(csv.DictReader(file))
    events = _read_events(args.events)
    for name, event, when, seconds in summarize(rows, events, args.victim, args.spare):
        delay = f"{seconds} s" if seconds is not None else "not reached"
        print(f"{name:<32} {when or '-':<21} {delay} after {event} ({events[event]})")
    for column in ("under_replicated", "ec_low_redundancy", "excess"):
        if highest := peak(rows, column):
            print(f"peak {column:<27} {highest[0]:<21} {highest[1]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
