"""The three queries of the sensor stream, their idempotent sinks, and how the job stops them.

Each output is its own query with its own checkpoint directory: ``aggregates`` and ``alerts`` in
gold, ``rejects`` in the streaming area. A ``foreachBatch`` sink writes micro-batch ``n`` to the
partition ``batch_id=n`` with ``write_partitions``, which replaces that partition: Spark replays an
uncommitted batch with the same id and the same files after a restart, so the replay overwrites
the partial result instead of adding to it (ADR-0008). The alerts sink writes the first breach
of each window that no earlier micro-batch alerted.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from gtdp.config import LakePaths, StreamingSettings
from gtdp.io import read_table, write_partitions
from gtdp.streaming.sensors import (
    AGGREGATES_SCHEMA,
    ALERTS_SCHEMA,
    BATCH_ID,
    REJECTS_SCHEMA,
    events,
    first_alerts,
    parse,
    rejects,
    threshold_breaches,
    thresholds_frame,
    window_aggregates,
)

logger = logging.getLogger(__name__)

FILE_PATTERN = "sensors_" + "[0-9]" * 8 + "T" + "[0-9]" * 6 + ".jsonl"
"""Files the stream reads in ``incoming``: ``sensors_<yyyyMMdd>T<HHmmss>.jsonl`` (data contract)."""
AGGREGATES, ALERTS, REJECTS = "aggregates", "alerts", "rejects"
SCHEMAS = {AGGREGATES: AGGREGATES_SCHEMA, ALERTS: ALERTS_SCHEMA, REJECTS: REJECTS_SCHEMA}
_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
_STOP_GRACE_S = 60.0
_WATERMARK_KEY = "spark.watermarkDelayMs"
_METRICS = (
    "batches",
    "input_rows",
    "rows_written",
    "rows_dropped_by_watermark",
    "rows_deduplicated",
)

Progress = dict[tuple[str, int], dict[str, Any]]
"""Progress report of each executed micro-batch, keyed by (query, batch id)."""


@dataclass(frozen=True)
class SensorPaths:
    """Where the stream reads, writes, keeps its checkpoints and looks for its stop marker."""

    incoming: str
    tables: Mapping[str, str]
    checkpoints: str
    stop_marker: str

    @classmethod
    def of(cls, paths: LakePaths, job: str) -> SensorPaths:
        return cls(
            incoming=f"{paths.landing}/sensors/incoming",
            tables={
                AGGREGATES: f"{paths.gold}/sensor_aggregates",
                ALERTS: f"{paths.gold}/sensor_alerts",
                REJECTS: f"{paths.streaming}/rejects",
            },
            checkpoints=paths.checkpoints,
            stop_marker=f"{paths.streaming}/control/stop-{job}",
        )

    def checkpoint(self, query: str) -> str:
        return f"{self.checkpoints}/{query}"


@dataclass
class Written:
    """Batches and rows each sink wrote in this run. The ``foreachBatch`` threads update it."""

    batches: Counter[str] = field(default_factory=Counter)
    rows: Counter[str] = field(default_factory=Counter)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, query: str, rows: int) -> None:
        with self._lock:
            self.batches[query] += 1
            self.rows[query] += rows

    def total_batches(self) -> int:
        with self._lock:
            return sum(self.batches.values())


def start_queries(
    spark: SparkSession,
    paths: SensorPaths,
    settings: StreamingSettings,
    written: Written,
    trigger: Mapping[str, Any],
) -> dict[str, StreamingQuery]:
    """Start the aggregates, alerts and rejects queries; ``trigger`` goes to ``writeStream``.

    The source lists the ``incoming`` directory and keeps the names that match ``FILE_PATTERN``.
    A pattern in the path itself would be expanded to one path per file, which Spark lists with
    one task per file in every micro-batch of every query.
    """
    lines = (
        spark.readStream.option("maxFilesPerTrigger", settings.max_files_per_trigger)
        .option("pathGlobFilter", FILE_PATTERN)
        .text(paths.incoming)
        .select(F.col("value").alias("raw_line"), F.input_file_name().alias("source_file"))
    )
    parsed = parse(lines, settings.future_tolerance_s)
    outputs = {
        AGGREGATES: window_aggregates(events(parsed)),
        ALERTS: threshold_breaches(events(parsed), thresholds_frame(spark, settings.thresholds)),
        REJECTS: rejects(parsed),
    }
    # A query copies the session configuration when it starts, and a new checkpoint keeps the
    # number of state partitions for good.
    previous = spark.conf.get(_SHUFFLE_PARTITIONS)
    spark.conf.set(_SHUFFLE_PARTITIONS, str(settings.state_partitions))
    try:
        return {
            name: output.writeStream.queryName(f"sensor_{name}")
            .outputMode("append")
            .trigger(**trigger)
            .option("checkpointLocation", paths.checkpoint(name))
            .foreachBatch(_batch_writer(name, paths.tables[name], written, _SELECT.get(name)))
            .start()
            for name, output in outputs.items()
        }
    finally:
        spark.conf.set(_SHUFFLE_PARTITIONS, previous)


def _new_alerts(batch: DataFrame, batch_id: int, table: str) -> DataFrame:
    """The first breach of each window of ``batch`` that no earlier micro-batch alerted.

    Batch ``n`` reads the alerts of batches below ``n`` only, so its replay finds the same ones.
    """
    alerted = read_table(batch.sparkSession, table, ALERTS_SCHEMA)
    return first_alerts(batch, alerted.where(F.col(BATCH_ID) < batch_id))


_SELECT: dict[str, Callable[[DataFrame, int, str], DataFrame]] = {ALERTS: _new_alerts}
"""Rows of a micro-batch to write, for the queries that do not write the whole batch."""


def _batch_writer(
    query: str,
    table: str,
    written: Written,
    select: Callable[[DataFrame, int, str], DataFrame] | None,
) -> Callable[[DataFrame, int], None]:
    def write(batch: DataFrame, batch_id: int) -> None:
        rows_of_batch = select(batch, batch_id, table) if select else batch
        # Cached: the count and the write must not compute the stateful plan of the batch twice.
        output = _as_table(rows_of_batch, query, batch_id).persist()
        try:
            rows = output.count()
            if rows:
                write_partitions(output, table, {BATCH_ID: [str(batch_id)]})
        finally:
            output.unpersist()
        written.add(query, rows)
        if rows:
            logger.info(
                "batch written",
                extra={"query": query, "batch_id": batch_id, "rows": rows, "table": table},
            )

    return write


def _as_table(batch: DataFrame, query: str, batch_id: int) -> DataFrame:
    """The rows of ``batch`` with the columns, order and types of their table.

    The watermark column keeps the stream's watermark marker in its metadata through any alias
    or cast the optimizer can remove, and a table must not keep it: it is rebuilt from its
    microseconds instead.
    """
    columns = []
    for column in SCHEMAS[query].fields:
        if column.name == BATCH_ID:
            value = F.lit(batch_id)
        elif _WATERMARK_KEY in batch.schema[column.name].metadata:
            value = F.timestamp_micros(F.unix_micros(column.name))
        else:
            value = F.col(column.name)
        columns.append(value.cast(column.dataType).alias(column.name))
    return batch.select(*columns)


def wait_for_stop(
    queries: Mapping[str, StreamingQuery],
    written: Written,
    settings: StreamingSettings,
    stop_requested: Callable[[], bool],
    progress: Progress,
) -> str:
    """Block until a stop condition holds and return it: ``"stop marker"`` or ``"idle"``.

    Every trigger interval (1 s at least) it checks the stop marker, then counts an idle trigger
    when no sink ran and no query has data or a batch in progress. ``stop_after_idle_triggers``
    idle triggers in a row stop the stream. A failed query raises its exception here.
    """
    poll_s = max(settings.trigger_interval_s, 1)
    idle, batches = 0, written.total_batches()
    while True:
        time.sleep(poll_s)
        for name, query in queries.items():
            if not query.isActive:
                raise query.exception() or RuntimeError(f"query {name} stopped unrequested")
        collect_progress(queries, progress)
        if stop_requested():
            return "stop marker"
        busy = any(
            q.status["isDataAvailable"] or q.status["isTriggerActive"] for q in queries.values()
        )
        now = written.total_batches()
        idle = 0 if busy or now != batches else idle + 1
        batches = now
        if settings.stop_after_idle_triggers and idle >= settings.stop_after_idle_triggers:
            return "idle"


def stop_queries(queries: Mapping[str, StreamingQuery], grace_s: float = _STOP_GRACE_S) -> None:
    """Stop each query between two micro-batches when it can; an interrupted batch is replayed."""
    for query in queries.values():
        deadline = time.monotonic() + grace_s
        while query.isActive and query.status["isTriggerActive"] and time.monotonic() < deadline:
            time.sleep(0.2)
        query.stop()


def collect_progress(queries: Mapping[str, StreamingQuery], progress: Progress) -> None:
    """Add the progress of each executed micro-batch to ``progress``.

    An idle trigger also reports a progress, with the id of the next batch and no ``addBatch``
    duration: it is left out.
    """
    for name, query in queries.items():
        for entry in query.recentProgress:
            if "addBatch" in entry["durationMs"]:
                progress[(name, entry["batchId"])] = entry


def query_metrics(progress: Progress, written: Written) -> dict[str, dict[str, int]]:
    """Per query: batches, rows read, rows written, rows dropped as late and as duplicates."""
    metrics = {name: dict.fromkeys(_METRICS, 0) for name in (AGGREGATES, ALERTS, REJECTS)}
    for (name, _), entry in progress.items():
        counts = metrics[name]
        counts["input_rows"] += entry["numInputRows"]
        for operator in entry["stateOperators"]:
            counts["rows_dropped_by_watermark"] += operator["numRowsDroppedByWatermark"]
            custom = operator.get("customMetrics", {})
            counts["rows_deduplicated"] += custom.get("numDroppedDuplicateRows", 0)
    for name, counts in metrics.items():
        counts["batches"] = written.batches[name]
        counts["rows_written"] = written.rows[name]
    return metrics


def checkpoint_state(spark: SparkSession, checkpoint: str) -> dict[str, int | None]:
    """Last planned (``offsets/``) and last committed (``commits/``) batch of a checkpoint.

    A planned batch without its commit is the one Spark replays when the query restarts.
    """
    return {
        f"last_{kind}_batch": max(_numbered_files(spark, f"{checkpoint}/{folder}"), default=None)
        for kind, folder in (("planned", "offsets"), ("committed", "commits"))
    }


def make_dirs(spark: SparkSession, path: str) -> None:
    fs, hadoop_path = _filesystem(spark, path)
    fs.mkdirs(hadoop_path)


def delete_path(spark: SparkSession, path: str) -> bool:
    """Delete ``path`` if it exists; return whether it did."""
    fs, hadoop_path = _filesystem(spark, path)
    return bool(fs.delete(hadoop_path, True))


def _numbered_files(spark: SparkSession, directory: str) -> list[int]:
    fs, hadoop_path = _filesystem(spark, directory)
    if not fs.exists(hadoop_path):
        return []
    names = [status.getPath().getName() for status in fs.listStatus(hadoop_path)]
    return [int(name) for name in names if name.isdigit()]


def _filesystem(spark: SparkSession, path: str) -> tuple[Any, Any]:
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hadoop_path
