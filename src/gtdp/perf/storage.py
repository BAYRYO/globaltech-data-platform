"""KAN-47: the same files stored with replication 3 and with erasure coding XOR-2-1-1024k.

    python3 -m gtdp.perf.storage --source HDFS_FILE [--small HDFS_FILE] [--runs N]
                                 [--root HDFS_DIR] [--run NAME] [--out DIR]
    python3 -m gtdp.perf.storage --report DIR
    (scripts/perf/storage.sh runs it in the cluster image)

Each run writes to its own area, ``<root>/<run>/rep3`` and ``<root>/<run>/ec`` (``--root``
defaults to ``[perf] storage``), and refuses an area that already exists: it never overwrites a
file, such as the copies kept for the DataNode failure test (KAN-52). It sets the policy
``XOR-2-1-1024k`` on ``ec`` and checks it, and that ``rep3`` has none, before the first copy.

The tool copies the source into each folder ``--runs`` times, alternating the modes, and times
each copy; a copy is removed before the next one, and the last stays. Then, for each mode, it
records the folder's policy (``hdfs ec -getPolicy``), the logical and physical bytes
(``hdfs dfs -du -s``), the blocks and their locations (``hdfs fsck -files -blocks -locations``)
and a composite CRC (``hdfs dfs -checksum``), equal in both modes when the bytes are equal. It
times the full read of each copy (``hdfs dfs -cat``), one warm-up read then ``--runs`` reads per
mode, alternating; the read of the small file gives the fixed cost of the client JVM. It writes
``result.json``, ``report.md`` (with the exact two-sided Mann-Whitney p of each time, rep3
against ec) and the fsck outputs to ``--out``. ``--report`` rebuilds ``report.md`` from the
``result.json`` of a directory.
"""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import re
import statistics
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from gtdp.config import ConfigError, load_config
from gtdp.perf.mr_compare import mann_whitney

POLICY = "XOR-2-1-1024k"
MODES = ("rep3", "ec")
NO_POLICY = "is unspecified"
BLOCK = re.compile(r"^\d+\. \S+ len=(\d+) Live_repl=(\d+)\s+\[(.*)\]\s*$", re.MULTILINE)
NODE = re.compile(r"DatanodeInfoWithStorage\[([^,\]]+)")


@dataclass(frozen=True)
class Block:
    """A block, or an erasure-coded block group, as ``hdfs fsck -blocks -locations`` lists it:
    its length, its live replicas or internal blocks, and the DataNode of each."""

    length: int
    live: int
    nodes: tuple[str, ...]


def blocks(fsck: str) -> list[Block]:
    """The blocks or block groups of the file of an ``hdfs fsck -files -blocks -locations``."""
    return [
        Block(int(length), int(live), tuple(NODE.findall(locations)))
        for length, live, locations in BLOCK.findall(fsck)
    ]


def overhead(logical: int, physical: int) -> float:
    """Bytes stored beyond the data, in percent of the data: 200 for 3 replicas."""
    return 100 * (physical - logical) / logical


def hdfs(*args: str, admin: bool = False) -> str:
    """Run ``hdfs args``; ``admin`` runs it as the HDFS superuser (fsck, ec)."""
    env = os.environ | {"HADOOP_USER_NAME": "hadoop"} if admin else None
    done = subprocess.run(["hdfs", *args], capture_output=True, text=True, check=True, env=env)
    return done.stdout


def timed(*args: str) -> float:
    start = time.perf_counter()
    subprocess.run(
        ["hdfs", *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True
    )
    return time.perf_counter() - start


def copy_of(source: str, folder: str) -> str:
    return posixpath.join(folder, posixpath.basename(source))


class Refused(Exception):
    """The run area cannot be used: it exists, or a folder does not have the expected policy."""


def run_area(root: str, run: str) -> dict[str, str]:
    """Create ``<root>/<run>/rep3`` and ``<root>/<run>/ec``, set ``POLICY`` on ``ec``, and check
    both policies; refuse an area that already exists."""
    area = posixpath.join(root, run)
    if subprocess.run(["hdfs", "dfs", "-test", "-e", area], check=False).returncode == 0:
        raise Refused(f"{area} exists: each run writes to a new area, pass another --run")
    folders = {mode: posixpath.join(area, mode) for mode in MODES}
    hdfs("dfs", "-mkdir", "-p", *folders.values())
    hdfs("ec", "-setPolicy", "-path", folders["ec"], "-policy", POLICY, admin=True)
    for mode, folder in folders.items():
        policy = hdfs("ec", "-getPolicy", "-path", folder, admin=True).strip()
        if not (policy == POLICY if mode == "ec" else policy.endswith(NO_POLICY)):
            raise Refused(f"{folder} has the policy {policy!r}")
    return folders


def measure(sources: Sequence[str], runs: int, folders: Mapping[str, str], out: Path) -> dict:
    source, smalls = sources[0], sources[1:]
    copies: dict[str, list[float]] = {mode: [] for mode in folders}
    for run in range(runs):
        for mode, folder in folders.items():
            if run:
                hdfs("dfs", "-rm", "-skipTrash", copy_of(source, folder))
            copies[mode].append(timed("dfs", "-cp", source, folder))
            for small in smalls if not run else ():
                hdfs("dfs", "-cp", small, folder)
    reads: dict[str, list[float]] = {mode: [] for mode in folders}
    small_reads: dict[str, list[float]] = {mode: [] for mode in folders}
    for run in range(runs + 1):
        for mode, folder in folders.items():
            seconds = timed("dfs", "-cat", copy_of(source, folder))
            small = [timed("dfs", "-cat", copy_of(path, folder)) for path in smalls[:1]]
            if run:
                reads[mode].append(seconds)
                small_reads[mode] += small
    files = {}
    for mode, folder in folders.items():
        policy = hdfs("ec", "-getPolicy", "-path", folder, admin=True).strip()
        for path in sources:
            copy = copy_of(path, folder)
            fsck = hdfs("fsck", copy, "-files", "-blocks", "-locations", admin=True)
            (out / f"fsck-{mode}-{posixpath.basename(path)}.txt").write_text(fsck)
            logical, physical, _ = hdfs("dfs", "-du", "-s", copy).split(maxsplit=2)
            checksum = hdfs(
                "dfs", "-Ddfs.checksum.combine.mode=COMPOSITE_CRC", "-checksum", copy
            ).split()[1:]
            files[copy] = {
                "mode": mode,
                "policy": policy,
                "logical": int(logical),
                "physical": int(physical),
                "overhead_pct": overhead(int(logical), int(physical)),
                "blocks": [asdict(block) for block in blocks(fsck)],
                "checksum": checksum,
            }
    return {
        "folders": dict(folders),
        "files": files,
        "copy_s": {mode: {"runs": values} for mode, values in copies.items()},
        "read_s": {mode: {"runs": values} for mode, values in reads.items()},
        "small_read_s": {mode: {"runs": values} for mode, values in small_reads.items() if values},
    }


TIMES = (("copy_s", "copie"), ("read_s", "lecture"), ("small_read_s", "lecture du petit fichier"))


def markdown(result: Mapping) -> str:
    """The volumes per file, and per mode the median, min and max of each time, with the exact
    two-sided Mann-Whitney p of rep3 against ec."""
    lines = [
        "| Fichier | Mode | Logique | Physique | Surcoût | Blocs ou groupes | Unités par bloc |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for path, f in result["files"].items():
        units = sorted({block["live"] for block in f["blocks"]})
        lines.append(
            f"| `{path}` | {f['mode']} | {f['logical']:,} | {f['physical']:,} "
            f"| {f['overhead_pct']:.1f} % | {len(f['blocks'])} | {units} |".replace(",", " ")
        )
    times = [(key, label) for key, label in TIMES if key in result]
    lines += [
        "",
        "| Mode | " + " | ".join(f"{label} : médiane, min à max (s)" for _, label in times) + " |",
        "| --- |" + " ---: |" * len(times),
    ]
    for mode in MODES:
        cells = []
        for key, _ in times:
            values = result[key][mode]["runs"]
            cells.append(f"{statistics.median(values):.2f}, {min(values):.2f} à {max(values):.2f}")
        lines.append(f"| {mode} | " + " | ".join(cells) + " |")
    p = [
        mann_whitney(result[key]["rep3"]["runs"], result[key]["ec"]["runs"])[2] for key, _ in times
    ]
    lines.append("| p bilatéral (rep3 contre ec) | " + " | ".join(f"{v:.3f}" for v in p) + " |")
    return "\n".join(lines)


def write_report(out: Path, result: Mapping) -> str:
    report = markdown(result)
    (out / "report.md").write_text(report + "\n")
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="storage", description=__doc__.split("\n\n")[0])
    parser.add_argument("--source", help="HDFS file measured in both modes")
    parser.add_argument("--small", help="HDFS file smaller than a 1 MiB cell")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--root", help="HDFS root of the run areas (default: [perf] storage)")
    parser.add_argument("--run", default=f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}", help="run area")
    parser.add_argument("--out", type=Path, default=Path("build/perf/storage"))
    parser.add_argument("--report", type=Path, help="rebuild DIR/report.md from DIR/result.json")
    args = parser.parse_args(argv)
    if args.report:
        print(write_report(args.report, json.loads((args.report / "result.json").read_text())))
        return 0
    if not args.source:
        parser.error("--source is required unless --report is given")
    root = args.root or perf_storage()
    try:
        folders = run_area(root, args.run)
    except Refused as refused:
        print(f"error: {refused}", file=sys.stderr)
        return 2
    args.out.mkdir(parents=True, exist_ok=True)
    sources = [args.source, *([args.small] if args.small else [])]
    result = measure(sources, args.runs, folders, args.out)
    (args.out / "result.json").write_text(json.dumps(result, indent=2))
    print(write_report(args.out, result))
    checksums: dict[str, set[str]] = {}
    for path, f in result["files"].items():
        checksums.setdefault(posixpath.basename(path), set()).add(f["checksum"][-1])
    differ = sorted(name for name, sums in checksums.items() if len(sums) > 1)
    if differ:
        print(f"CHECK FAILED: the two copies of {differ} differ", file=sys.stderr)
    return 1 if differ else 0


def perf_storage() -> str:
    perf = load_config("cluster").perf
    if perf is None:
        raise ConfigError("the configuration has no [perf] section")
    return perf.storage


if __name__ == "__main__":
    sys.exit(main())
