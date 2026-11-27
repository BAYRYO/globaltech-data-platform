"""The three queries of the sensor stream, their idempotent sinks, and how the job stops them.

Each output is its own query with its own checkpoint directory: ``aggregates`` and ``alerts`` in
gold, ``rejects`` in the streaming area. A ``foreachBatch`` sink writes micro-batch ``n`` to the
partitions ``<date>=<d>/batch_id=n`` with ``write_partitions``, which replaces them: Spark replays
an uncommitted batch with the same id and the same files after a restart, so the replay
overwrites the partial result instead of adding to it (ADR-0008). The alerts sink writes the
first breach of each window that no earlier micro-batch alerted.
"""

from __future__ import annotations

import functools
import logging
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from gtdp.config import LakePaths, StreamingSettings
from gtdp.io import (
    STALE_AFTER_CONF,
    TableLockedError,
    list_partition_values,
    read_table,
    write_partitions,
)
from gtdp.streaming.sensors import (
    AGGREGATES_SCHEMA,
    ALERTS_SCHEMA,
    ARRIVAL_DATE,
    BATCH_ID,
    REJECTS_SCHEMA,
    WINDOW_DATE,
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
METRICS = (
    "batches",
    "input_rows",
    "rows_written",
    "rows_dropped_by_watermark",
    "rows_deduplicated",
)
_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
_STOP_GRACE_S = 60.0
_LOCK_POLL_S = 2.0
_LOCK_MARGIN_S = 60.0
_WATERMARK_KEY = "spark.watermarkDelayMs"
_ALERTED_SCHEMA = StructType(
    [StructField("alert_id", StringType()), StructField(BATCH_ID, IntegerType())]
)


@dataclass(frozen=True)
class Output:
    """The table a query writes, partitioned by ``date_column`` then ``batch_id``.

    ``closes_with_watermark``: no row of a date arrives once the watermark passes its end.
    """

    schema: StructType
    date_column: str
    closes_with_watermark: bool


OUTPUTS: Mapping[str, Output] = MappingProxyType(
    {
        AGGREGATES: Output(AGGREGATES_SCHEMA, WINDOW_DATE, closes_with_watermark=True),
        ALERTS: Output(ALERTS_SCHEMA, WINDOW_DATE, closes_with_watermark=True),
        REJECTS: Output(REJECTS_SCHEMA, ARRIVAL_DATE, closes_with_watermark=False),
    }
)
SCHEMAS = MappingProxyType({name: output.schema for name, output in OUTPUTS.items()})


@dataclass(frozen=True)
class SensorPaths:
    """Where the stream reads, writes, keeps its checkpoints, archives and finds its stop marker."""

    incoming: str
    tables: Mapping[str, str]
    checkpoints: str
    archive: str
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
            archive=f"{paths.streaming}/archive/sensors",
            stop_marker=f"{paths.streaming}/control/stop-{job}",
        )

    def checkpoint(self, query: str) -> str:
        return f"{self.checkpoints}/{query}"


class Totals:
    """Running counts of each query in this run: their memory does not grow with the run.

    The ``foreachBatch`` threads add what each sink wrote; the driver adds each micro-batch's
    progress report once, however many times it reads the recent reports.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts = {name: dict.fromkeys(METRICS, 0) for name in OUTPUTS}
        self._last_batch = dict.fromkeys(OUTPUTS, -1)

    def add_written(self, query: str, rows: int) -> None:
        with self._lock:
            self._counts[query]["batches"] += 1
            self._counts[query]["rows_written"] += rows

    def add_progress(self, query: str, progress: Mapping[str, Any]) -> None:
        """Count the progress report of one executed micro-batch, unless already counted."""
        with self._lock:
            if progress["batchId"] <= self._last_batch[query]:
                return
            self._last_batch[query] = progress["batchId"]
            counts = self._counts[query]
            counts["input_rows"] += progress["numInputRows"]
            for operator in progress["stateOperators"]:
                counts["rows_dropped_by_watermark"] += operator["numRowsDroppedByWatermark"]
                custom = operator.get("customMetrics", {})
                counts["rows_deduplicated"] += custom.get("numDroppedDuplicateRows", 0)

    def batches_written(self) -> int:
        with self._lock:
            return sum(counts["batches"] for counts in self._counts.values())

    def snapshot(self) -> dict[str, dict[str, int]]:
        """Per query: batches, rows read, rows written, rows dropped as late and as duplicates."""
        with self._lock:
            return {name: dict(counts) for name, counts in self._counts.items()}


def start_queries(
    spark: SparkSession,
    paths: SensorPaths,
    settings: StreamingSettings,
    totals: Totals,
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
        .select(
            F.col("value").alias("raw_line"),
            F.input_file_name().alias("source_file"),
            F.to_date("_metadata.file_modification_time").alias(ARRIVAL_DATE),
        )
    )
    parsed = parse(lines, settings.future_tolerance_s)
    outputs = {
        AGGREGATES: window_aggregates(events(parsed)),
        ALERTS: threshold_breaches(events(parsed), thresholds_frame(spark, settings.thresholds)),
        REJECTS: rejects(parsed),
    }
    # A query copies the session configuration when it starts, and a new checkpoint keeps the
    # number of state partitions for good.
    session = {
        _SHUFFLE_PARTITIONS: str(settings.state_partitions),
        STALE_AFTER_CONF: str(settings.stale_after_s),
    }
    previous = {key: spark.conf.get(key, None) for key in session}
    for key, value in session.items():
        spark.conf.set(key, value)
    try:
        return {
            name: output.writeStream.queryName(f"sensor_{name}")
            .outputMode("append")
            .trigger(**trigger)
            .option("checkpointLocation", paths.checkpoint(name))
            .foreachBatch(_batch_writer(name, paths.tables[name], totals, settings.stale_after_s))
            .start()
            for name, output in outputs.items()
        }
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def _new_alerts(batch: DataFrame, batch_id: int, table: str) -> DataFrame:
    """The first breach of each window of ``batch`` that no earlier micro-batch alerted.

    Batch ``n`` reads the alerts of batches below ``n`` only, so its replay finds the same ones.
    It reads only the dates from its earliest window on: a breach is never older than the
    watermark, and the housekeeping compacts only the dates that the watermark has passed.
    """
    since: date | None = batch.agg(F.min(WINDOW_DATE)).first()[0]
    if since is None:
        return batch
    dates = list_partition_values(batch.sparkSession, table, WINDOW_DATE)
    alerted = [
        read_table(batch.sparkSession, f"{table}/{WINDOW_DATE}={d}", _ALERTED_SCHEMA)
        .where(F.col(BATCH_ID) < batch_id)
        .select("alert_id")
        for d in dates
        if d >= since.isoformat()
    ]
    none = batch.sparkSession.createDataFrame([], _ALERTED_SCHEMA).select("alert_id")
    return first_alerts(batch, functools.reduce(DataFrame.unionByName, alerted, none))


_SELECT: dict[str, Callable[[DataFrame, int, str], DataFrame]] = {ALERTS: _new_alerts}
"""Rows of a micro-batch to write, for the queries that do not write the whole batch."""


def _batch_writer(
    query: str, table: str, totals: Totals, stale_after_s: int
) -> Callable[[DataFrame, int], None]:
    output, select = OUTPUTS[query], _SELECT.get(query)

    def write(batch: DataFrame, batch_id: int) -> None:
        # Cached: the stateful plan of the batch must run once, whatever reads it afterwards.
        batch.persist()
        frame = None
        try:
            selected = select(batch, batch_id, table) if select else batch
            frame = _as_table(selected, output.schema, batch_id).persist()
            dates = dict(frame.groupBy(output.date_column).count().collect())
            rows = sum(dates.values())
            if rows:
                partitions = {output.date_column: sorted(dates), BATCH_ID: [str(batch_id)]}
                _write_waiting(frame.coalesce(1), table, partitions, stale_after_s)
        finally:
            if frame is not None:
                frame.unpersist()
            batch.unpersist()
        totals.add_written(query, rows)
        if rows:
            logger.info(
                "batch written",
                extra={"query": query, "batch_id": batch_id, "rows": rows, "table": table},
            )

    return write


def _write_waiting(
    frame: DataFrame, table: str, partitions: Mapping[str, list[Any]], stale_after_s: int
) -> None:
    """``write_partitions``, retried while another run holds the table's lock.

    The holder is the housekeeping, or a run that died holding it, whose lock ``write_partitions``
    takes over once YARN reports it ended, or after ``stale_after_s`` without a state from YARN.
    ``TableLockedError`` once ``stale_after_s`` and a margin have passed, whatever YARN says of the
    holder: a housekeeping step longer than that fails the query.
    """
    start, warned = time.monotonic(), False
    while True:
        try:
            write_partitions(frame, table, partitions)
        except TableLockedError:
            if time.monotonic() - start > stale_after_s + _LOCK_MARGIN_S:
                raise
            if not warned:
                logger.warning("table locked, waiting for its lock", extra={"table": table})
                warned = True
            time.sleep(_LOCK_POLL_S)
            continue
        if warned:
            waited_s = round(time.monotonic() - start, 1)
            logger.info("table lock obtained", extra={"table": table, "waited_s": waited_s})
        return


def _as_table(batch: DataFrame, schema: StructType, batch_id: int) -> DataFrame:
    """The rows of ``batch`` with the columns, order and types of ``schema``.

    The watermark column keeps the stream's watermark marker in its metadata through any alias
    or cast the optimizer can remove, and a table must not keep it: it is rebuilt from its
    microseconds instead.
    """
    columns = []
    for column in schema.fields:
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
    totals: Totals,
    settings: StreamingSettings,
    stop_requested: Callable[[], bool],
) -> str:
    """Block until a stop condition holds and return it: ``"stop marker"`` or ``"idle"``.

    Every trigger interval (1 s at least) it adds the new progress reports to ``totals``, checks
    the stop marker, then counts an idle trigger when no sink ran and no query has data or a
    batch in progress. ``stop_after_idle_triggers`` idle triggers in a row stop the stream. A
    failed query raises its exception here.
    """
    poll_s = max(settings.trigger_interval_s, 1)
    idle, batches = 0, totals.batches_written()
    while True:
        time.sleep(poll_s)
        for name, query in queries.items():
            if not query.isActive:
                raise query.exception() or RuntimeError(f"query {name} stopped unrequested")
        collect_progress(queries, totals)
        if stop_requested():
            return "stop marker"
        busy = any(
            q.status["isDataAvailable"] or q.status["isTriggerActive"] for q in queries.values()
        )
        now = totals.batches_written()
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


def collect_progress(queries: Mapping[str, StreamingQuery], totals: Totals) -> None:
    """Add the progress of each executed micro-batch still in ``recentProgress`` to ``totals``.

    An idle trigger also reports a progress, with the id of the next batch and no ``addBatch``
    duration: it is left out.
    """
    for name, query in queries.items():
        for entry in query.recentProgress:
            if "addBatch" in entry["durationMs"]:
                totals.add_progress(name, entry)


def checkpoint_state(spark: SparkSession, checkpoint: str) -> dict[str, int | None]:
    """Last planned (``offsets/``) and last committed (``commits/``) batch of a checkpoint.

    A planned batch without its commit is the one Spark replays when the query restarts.
    """
    return {
        f"last_{kind}_batch": max(numbered_files(spark, f"{checkpoint}/{folder}"), default=None)
        for kind, folder in (("planned", "offsets"), ("committed", "commits"))
    }


def make_dirs(spark: SparkSession, path: str) -> None:
    fs, hadoop_path = filesystem(spark, path)
    fs.mkdirs(hadoop_path)


def delete_path(spark: SparkSession, path: str) -> bool:
    """Delete ``path`` if it exists; return whether it did."""
    fs, hadoop_path = filesystem(spark, path)
    return bool(fs.delete(hadoop_path, True))


def numbered_files(spark: SparkSession, directory: str) -> list[int]:
    """The numbers of the files named by a number in ``directory`` (a checkpoint log)."""
    fs, hadoop_path = filesystem(spark, directory)
    if not fs.exists(hadoop_path):
        return []
    names = [status.getPath().getName() for status in fs.listStatus(hadoop_path)]
    return [int(name) for name in names if name.isdigit()]


def filesystem(spark: SparkSession, path: str) -> tuple[Any, Any]:
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hadoop_path
