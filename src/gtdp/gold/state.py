"""``_state.json`` at the root of gold: whether the gold tables form one published whole.

A gold run writes ``PUBLISHING`` before its first write and ``PUBLISHED`` after its last. Trust
gold only while the status is ``PUBLISHED``: a run that failed or was killed while writing leaves
``PUBLISHING`` and a mix of tables, until a later run succeeds (docs/runbook/gold.md).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Any

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from gtdp.io import write_file

STATE = "_state.json"


class Status(StrEnum):
    PUBLISHING = "PUBLISHING"
    PUBLISHED = "PUBLISHED"


@dataclass(frozen=True)
class State:
    status: Status
    run_id: str
    """The audited gold run that wrote the state."""
    silver_days: tuple[date, ...]
    """The days silver had completed when that run read it: gold is built from them alone."""


def write_state(spark: SparkSession, gold: str, state: State) -> None:
    """Replace the state of ``gold`` with ``state``, whole (``gtdp.io.write_file``)."""
    record = {
        "status": state.status.value,
        "run_id": state.run_id,
        "silver_days": [str(day) for day in state.silver_days],
    }
    write_file(spark, f"{gold}/{STATE}", json.dumps(record))


def read_state(spark: SparkSession, gold: str) -> State | None:
    """The state of ``gold``; ``None`` when no run wrote one, or when it cannot be read: either
    way, gold is not known to be published."""
    fs, path = _filesystem(spark, f"{gold}/{STATE}")
    if not fs.exists(path):
        return None
    try:
        stream = fs.open(path)
        try:
            record = json.loads(bytes(stream.readAllBytes()).decode("utf-8"))
        finally:
            stream.close()
        days = tuple(date.fromisoformat(day) for day in record["silver_days"])
        return State(Status(record["status"]), record["run_id"], days)
    except (Py4JJavaError, ValueError, KeyError, TypeError):
        return None


def _filesystem(spark: SparkSession, path: str) -> tuple[Any, Any]:
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hadoop_path
