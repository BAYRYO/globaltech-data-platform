"""KAN-47: the same files stored with replication 3 and with erasure coding XOR-2-1-1024k.

    python3 -m gtdp.perf.storage --source HDFS_FILE [--small HDFS_FILE] [--runs N] [--out DIR]
    (scripts/perf/storage.sh runs it in the cluster image)

The tool copies each file into ``/benchmarks/rep3`` (replication 3) and ``/benchmarks/ec``
(erasure coding), alternating the modes, ``--runs`` times, and times each copy. Then, for each
mode, it records the directory's policy (``hdfs ec -getPolicy``), the logical and physical bytes
(``hdfs dfs -du -s``), the blocks and their locations (``hdfs fsck -files -blocks -locations``)
and a composite CRC (``hdfs dfs -checksum``), equal in both modes when the bytes are equal. It
times the full read of each copy (``hdfs dfs -cat``), one warm-up read then ``--runs`` reads per
mode, alternating; the read of the small file gives the fixed cost of the client JVM. It writes
``result.json``, ``report.md`` and the fsck outputs to ``--out``. The copies stay in place for
the DataNode failure test (KAN-52).
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
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

MODES = {"rep3": "/benchmarks/rep3", "ec": "/benchmarks/ec"}
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


def spread(values: Sequence[float]) -> dict[str, float]:
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def measure(sources: Sequence[str], runs: int, out: Path) -> dict:
    copies = {mode: [] for mode in MODES}
    for _ in range(runs):
        for mode, folder in MODES.items():
            copies[mode].append(timed("dfs", "-cp", "-f", sources[0], folder))
            for small in sources[1:]:
                hdfs("dfs", "-cp", "-f", small, folder)
    reads = {mode: [] for mode in MODES}
    small_reads = {mode: [] for mode in MODES}
    for run in range(runs + 1):
        for mode, folder in MODES.items():
            seconds = timed("dfs", "-cat", copy_of(sources[0], folder))
            small = [timed("dfs", "-cat", copy_of(source, folder)) for source in sources[1:2]]
            if run:
                reads[mode].append(seconds)
                small_reads[mode] += small
    files = {}
    for mode, folder in MODES.items():
        policy = hdfs("ec", "-getPolicy", "-path", folder, admin=True).strip()
        for source in sources:
            path = copy_of(source, folder)
            fsck = hdfs("fsck", path, "-files", "-blocks", "-locations", admin=True)
            (out / f"fsck-{mode}-{posixpath.basename(source)}.txt").write_text(fsck)
            logical, physical, _ = hdfs("dfs", "-du", "-s", path).split(maxsplit=2)
            checksum = hdfs(
                "dfs", "-Ddfs.checksum.combine.mode=COMPOSITE_CRC", "-checksum", path
            ).split()[1:]
            files[path] = {
                "mode": mode,
                "policy": policy,
                "logical": int(logical),
                "physical": int(physical),
                "overhead_pct": overhead(int(logical), int(physical)),
                "blocks": [asdict(block) for block in blocks(fsck)],
                "checksum": checksum,
            }
    return {
        "files": files,
        "copy_s": {mode: spread(values) | {"runs": values} for mode, values in copies.items()},
        "read_s": {mode: spread(values) | {"runs": values} for mode, values in reads.items()},
        "small_read_s": {
            mode: spread(values) | {"runs": values}
            for mode, values in small_reads.items()
            if values
        },
    }


def markdown(result: dict) -> str:
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
    lines += ["", "| Mode | Copie médiane (s) | Lecture médiane (s) | Min à max (s) |"]
    lines.append("| --- | ---: | ---: | ---: |")
    for mode in MODES:
        read, copy = result["read_s"][mode], result["copy_s"][mode]
        lines.append(
            f"| {mode} | {copy['median']:.2f} | {read['median']:.2f} "
            f"| {read['min']:.2f} à {read['max']:.2f} |"
        )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="storage", description=__doc__.split("\n\n")[0])
    parser.add_argument("--source", required=True, help="HDFS file measured in both modes")
    parser.add_argument("--small", help="HDFS file smaller than a 1 MiB cell")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--out", type=Path, default=Path("build/perf/storage"))
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    sources = [args.source, *([args.small] if args.small else [])]
    result = measure(sources, args.runs, args.out)
    (args.out / "result.json").write_text(json.dumps(result, indent=2))
    (args.out / "report.md").write_text(markdown(result) + "\n")
    print(markdown(result))
    checksums: dict[str, set[str]] = {}
    for path, f in result["files"].items():
        checksums.setdefault(posixpath.basename(path), set()).add(f["checksum"][-1])
    differ = sorted(name for name, sums in checksums.items() if len(sums) > 1)
    if differ:
        print(f"CHECK FAILED: the two copies of {differ} differ", file=sys.stderr)
    return 1 if differ else 0


if __name__ == "__main__":
    sys.exit(main())
