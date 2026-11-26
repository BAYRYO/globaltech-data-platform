"""Helpers of the streaming tests: a local lake, sensor files and a finite run of the stream."""

import json
import os
import time
from dataclasses import fields, replace
from pathlib import Path

from pyspark.errors import StreamingQueryException

from gtdp.config import LakePaths, load_config
from gtdp.streaming.stream import (
    SensorPaths,
    Written,
    collect_progress,
    query_metrics,
    start_queries,
)

JOB = "streaming-sensors"
SUFFIX = {"temperature": "TEMP", "vibration": "VIB", "pressure": "PRES"}


def local_path(uri: str) -> Path:
    return Path(uri.removeprefix("file://"))


def lake(root: Path):
    """Local configuration whose lake areas live under ``root``, one micro-batch per file."""
    areas = {f"paths.{area.name}": str(root / area.name) for area in fields(LakePaths)}
    cfg = load_config("local", overrides=areas)
    settings = replace(cfg.streaming, max_files_per_trigger=1, state_partitions=2)
    return SensorPaths.of(cfg.paths, JOB), settings


def event(event_id: str, at: str, value: float | None, metric="temperature", machine="FR-LYON-M01"):
    """A contract sensor event at ``2026-01-01T<at>``, as one JSON line."""
    stamp = f"2026-01-01T{at}.000Z"
    return json.dumps(
        {
            "event_id": event_id,
            "sensor_id": f"{machine}-{SUFFIX[metric]}",
            "site": machine.rsplit("-", 1)[0],
            "machine_id": machine,
            "metric": metric,
            "value": value,
            "unit": "C",
            "event_time": stamp,
            "emitted_at": stamp,
        }
    )


def drop_files(paths: SensorPaths, files: list[list[str]], first: int = 0) -> None:
    """Write each list of lines as one sensor file, each file one second newer than the last.

    The file source reads new files oldest first, so with one file per micro-batch, file ``k``
    is micro-batch ``k``.
    """
    incoming = local_path(paths.incoming)
    incoming.mkdir(parents=True, exist_ok=True)
    origin = time.time() - 3600
    for index, lines in enumerate(files, start=first):
        path = incoming / f"sensors_{index:04d}.jsonl"
        path.write_text("".join(f"{line}\n" for line in lines), encoding="utf-8")
        os.utime(path, (origin + index, origin + index))


def run_stream(spark, paths: SensorPaths, settings) -> dict[str, dict[str, int]]:
    """Run the three queries over every file present, then return their metrics."""
    written, progress = Written(), {}
    queries = start_queries(spark, paths, settings, written, {"availableNow": True})
    errors = []
    for query in queries.values():
        try:
            query.awaitTermination()
        except StreamingQueryException as error:
            errors.append(error)
    if errors:
        raise errors[0]
    collect_progress(queries, progress)
    return query_metrics(progress, written)
