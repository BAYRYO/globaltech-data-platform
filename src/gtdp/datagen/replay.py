"""Replay pre-generated sensor files into the directory Spark Structured Streaming watches.

A file appears in the destination in one atomic step: a rename on the local file system, an
HDFS `-mv` from a sibling staging directory. Files already in the destination are skipped, so
a replay that stopped half-way resumes where it left off.
"""

import os
import shutil
import subprocess
import time
from collections.abc import Callable, Iterator
from pathlib import Path

Publisher = Callable[[Path], None]


def local_publisher(destination: Path) -> tuple[set[str], Publisher]:
    destination.mkdir(parents=True, exist_ok=True)

    def publish(source: Path) -> None:
        tmp = destination / f".{source.name}.tmp"
        shutil.copyfile(source, tmp)
        os.replace(tmp, destination / source.name)

    return {p.name for p in destination.iterdir() if not p.name.startswith(".")}, publish


def hdfs_publisher(
    destination: str, run: Callable[..., subprocess.CompletedProcess] = subprocess.run
) -> tuple[set[str], Publisher]:
    destination = destination.rstrip("/")
    staging = f"{destination}_staging"
    run(["hdfs", "dfs", "-mkdir", "-p", staging, destination], check=True)
    listing = run(
        ["hdfs", "dfs", "-ls", "-C", destination], check=True, capture_output=True, text=True
    )
    present = {line.rsplit("/", 1)[-1] for line in listing.stdout.splitlines() if line}

    def publish(source: Path) -> None:
        staged = f"{staging}/{source.name}"
        run(["hdfs", "dfs", "-put", "-f", str(source), staged], check=True)
        run(["hdfs", "dfs", "-mv", staged, f"{destination}/{source.name}"], check=True)

    return present, publish


def replay(
    source_dir: Path,
    destination: str,
    interval: float,
    limit: int | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    run: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> Iterator[Path]:
    """Publish one file every `interval` seconds, in name order; yield each file once published."""
    if destination.startswith("hdfs://"):
        present, publish = hdfs_publisher(destination, run)
    else:
        present, publish = local_publisher(Path(destination))
    files = sorted(source_dir.glob("sensors_*.jsonl"))[:limit]
    pending = [path for path in files if path.name not in present]
    start = clock()
    for index, path in enumerate(pending):
        delay = start + index * interval - clock()
        if delay > 0:
            sleep(delay)
        publish(path)
        yield path


def count_lines(path: Path) -> int:
    """Lines of a sensor file: one event each, unreadable ones included."""
    with path.open("rb") as handle:
        return sum(1 for _ in handle)
