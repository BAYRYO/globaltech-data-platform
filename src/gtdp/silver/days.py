"""The days silver has published, per source, and the complete days readers can trust.

``silver/_days/ingestion_date=D/<source>.json`` names the run that published source ``D`` and
verified it. ``_SUCCESS`` beside them appears once every source of ``D`` has its marker: the
day is complete. Later days deduplicate against complete days only, and a reader that must see
whole days reads the partitions of ``complete_days`` alone.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, date, datetime
from typing import Any

from pyspark.sql import SparkSession

from gtdp.io import INGESTION_DATE, list_partition_values

DAYS = "_days"
COMPLETE = "_SUCCESS"


def days_path(silver: str) -> str:
    return f"{silver}/{DAYS}"


def complete_days(spark: SparkSession, silver: str) -> list[date]:
    """The days whose every source silver published and verified, in order."""
    return [day for day in _marked_days(spark, silver) if _exists(spark, _complete(silver, day))]


def published_sources(spark: SparkSession, silver: str, day: date) -> dict[str, str]:
    """Each source published for ``day``, with the run that published it."""
    directory = _day_path(silver, day)
    fs, path = _fs(spark, directory)
    if not fs.exists(path):
        return {}
    markers = [s.getPath().getName() for s in fs.listStatus(path)]
    return {
        name.removesuffix(".json"): json.loads(_read(spark, f"{directory}/{name}"))["run_id"]
        for name in markers
        if name.endswith(".json")
    }


def latest_day(
    spark: SparkSession, silver: str, source: str, before: date | None = None
) -> date | None:
    """The latest day, before ``before`` if given, for which ``source`` is published."""
    days = [
        day
        for day in _marked_days(spark, silver)
        if (before is None or day < before) and source in published_sources(spark, silver, day)
    ]
    return days[-1] if days else None


def mark_source(spark: SparkSession, silver: str, day: date, source: str, run_id: str) -> None:
    record = {
        "source": source,
        "run_id": run_id,
        "published_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    _write(spark, f"{_day_path(silver, day)}/{source}.json", record)


def mark_complete(
    spark: SparkSession, silver: str, day: date, sources: Iterable[str], run_id: str
) -> bool:
    """Write the ``_SUCCESS`` of ``day`` if every one of ``sources`` is published for it."""
    published = published_sources(spark, silver, day)
    if set(sources) - published.keys():
        return False
    _write(spark, _complete(silver, day), {"run_id": run_id, "sources": published})
    return True


def unmark(spark: SparkSession, silver: str, day: date, sources: Iterable[str]) -> None:
    """Make ``day`` incomplete and ``sources`` unpublished for it."""
    for path in (_complete(silver, day), *[f"{_day_path(silver, day)}/{s}.json" for s in sources]):
        fs, hadoop_path = _fs(spark, path)
        fs.delete(hadoop_path, False)


def unmark_after(spark: SparkSession, silver: str, day: date) -> list[date]:
    """Drop the markers of every day after ``day``; return those days."""
    later = [marked for marked in _marked_days(spark, silver) if marked > day]
    for marked in later:
        fs, path = _fs(spark, _day_path(silver, marked))
        fs.delete(path, True)
    return later


def _marked_days(spark: SparkSession, silver: str) -> list[date]:
    return [
        date.fromisoformat(value)
        for value in list_partition_values(spark, days_path(silver), INGESTION_DATE)
    ]


def _day_path(silver: str, day: date) -> str:
    return f"{days_path(silver)}/{INGESTION_DATE}={day}"


def _complete(silver: str, day: date) -> str:
    return f"{_day_path(silver, day)}/{COMPLETE}"


def _fs(spark: SparkSession, path: str) -> tuple[Any, Any]:
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hadoop_path


def _exists(spark: SparkSession, path: str) -> bool:
    fs, hadoop_path = _fs(spark, path)
    return bool(fs.exists(hadoop_path))


def _write(spark: SparkSession, path: str, record: dict[str, Any]) -> None:
    fs, hadoop_path = _fs(spark, path)
    stream = fs.create(hadoop_path, True)
    try:
        stream.write(bytearray(json.dumps(record, default=str).encode()))
    finally:
        stream.close()


def _read(spark: SparkSession, path: str) -> str:
    fs, hadoop_path = _fs(spark, path)
    stream = fs.open(hadoop_path)
    try:
        return bytes(stream.readAllBytes()).decode("utf-8")
    finally:
        stream.close()
