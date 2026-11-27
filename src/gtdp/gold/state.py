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

from pyspark.sql import SparkSession

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
    record = {
        "status": state.status.value,
        "run_id": state.run_id,
        "silver_days": [str(day) for day in state.silver_days],
    }
    fs, path = _filesystem(spark, f"{gold}/{STATE}")
    stream = fs.create(path, True)
    try:
        stream.write(bytearray(json.dumps(record).encode()))
    finally:
        stream.close()


def read_state(spark: SparkSession, gold: str) -> State | None:
    """The state of ``gold``; ``None`` when no run wrote one, or a run died writing it."""
    fs, path = _filesystem(spark, f"{gold}/{STATE}")
    if not fs.exists(path):
        return None
    stream = fs.open(path)
    try:
        record = json.loads(bytes(stream.readAllBytes()).decode("utf-8"))
        days = tuple(date.fromisoformat(day) for day in record["silver_days"])
        return State(Status(record["status"]), record["run_id"], days)
    except (ValueError, KeyError, TypeError):
        return None
    finally:
        stream.close()


def _filesystem(spark: SparkSession, path: str) -> tuple[Any, Any]:
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hadoop_path
