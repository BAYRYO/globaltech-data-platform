"""The days silver has published, per source, and the complete days readers can trust.

``silver/_days/ingestion_date=D/<source>.json`` names the run that published source ``D`` and
verified it. ``_SUCCESS`` beside them appears once every source of ``D`` has its marker: the
day is complete. Later days deduplicate against complete days only, and a reader that must see
whole days reads the partitions of ``complete_days`` alone.

A table rebuilt whole (customers, products) holds no partition to leave out: each row carries the
day it arrived, and ``<table>/_built_for.json`` names the day the table was rebuilt for, written
into the new table before the swap so that it moves in with it. A reader that must see complete
days only reads such a table while both name complete days.

Each file of ``_days`` is written whole, then renamed into place (``gtdp.io.write_file``).
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, date, datetime
from typing import Any

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from gtdp.io import INGESTION_DATE, list_partition_values, write_file

DAYS = "_days"
COMPLETE = "_SUCCESS"
BUILT_FOR = "_built_for.json"


class MarkerError(RuntimeError):
    """A file of ``silver/_days``, or the ``_built_for.json`` of a table, cannot be read."""


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
    redo = f"run silver --date {day} again to rewrite the markers of the day"
    return {
        name.removesuffix(".json"): _field(spark, f"{directory}/{name}", "run_id", redo)
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


def built_for(day: date | None, run_id: str) -> dict[str, str]:
    """The ``files`` of ``gtdp.publish.publish`` for a table rebuilt whole with the rows of
    ``day``, or emptied (``None``), by the run ``run_id``."""
    return {BUILT_FOR: json.dumps({"day": day, "run_id": run_id}, default=str)}


def rebuilt_day(spark: SparkSession, table: str) -> date | None:
    """The day the table at ``table``, rebuilt whole, was rebuilt for; ``None`` if it was
    emptied, or if it has no ``_built_for.json`` (written before silver wrote one)."""
    path = f"{table}/{BUILT_FOR}"
    if not _exists(spark, path):
        return None
    redo = "run silver --date <the latest complete day> again to rebuild the table"
    day = _field(spark, path, "day", redo)
    try:
        return None if day is None else date.fromisoformat(day)
    except (TypeError, ValueError) as exc:
        raise MarkerError(f"{path} is unreadable ({exc!r}): {redo}") from None


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
    write_file(spark, path, json.dumps(record, default=str))


def _field(spark: SparkSession, path: str, name: str, redo: str) -> Any:
    """Field ``name`` of the JSON object in the file ``path``; ``MarkerError`` saying ``redo``
    if the file cannot be read or is not such an object."""
    fs, hadoop_path = _fs(spark, path)
    try:
        stream = fs.open(hadoop_path)
        try:
            return json.loads(bytes(stream.readAllBytes()).decode("utf-8"))[name]
        finally:
            stream.close()
    except Py4JJavaError as exc:
        reason = exc.java_exception.toString()
    except (ValueError, KeyError, TypeError) as exc:
        reason = repr(exc)
    raise MarkerError(f"{path} is unreadable ({reason}): {redo}")
