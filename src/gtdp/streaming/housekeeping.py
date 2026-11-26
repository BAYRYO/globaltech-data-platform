"""What keeps a 24/7 sensor stream within its HDFS quotas: archive, purge and compaction.

- ``archive_inputs`` moves the input files that every query has committed out of ``incoming``,
  to ``<archive>/arrival_date=<d>/``. A file is committed by a query once a committed micro-batch
  lists it in the query's file source log (``<checkpoint>/sources/0``).
- ``purge_archive`` deletes the archived days older than the retention.
- ``compact_table`` merges the ``batch_id`` partitions of a date into one, the partition of the
  last batch merged. It merges only batches older than the oldest one the checkpoint retains
  (Spark never replays them), and for the aggregates and alerts only dates the watermark of the
  last committed batch has passed (no micro-batch writes or reads them any more).

A compaction stages the merged rows, then writes a journal of the partitions it replaces, then
swaps: it deletes those partitions and moves the merged one in. The run that finds a journal
finishes that swap first, so a run killed at any point is completed by the next one.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from gtdp.io import TableLockedError, list_partition_values, table_lock
from gtdp.streaming.sensors import ARRIVAL_DATE, BATCH_ID
from gtdp.streaming.stream import (
    OUTPUTS,
    Output,
    SensorPaths,
    filesystem,
    numbered_files,
)

logger = logging.getLogger(__name__)

_JOURNAL = "_journal.json"
_MERGED = "merged"
_SOURCE_LOG = "sources/0"
_LOCK_POLL_S = 2.0
_LOCK_MARGIN_S = 60.0


@dataclass(frozen=True)
class CheckpointState:
    """What a query's checkpoint says about the batches the housekeeping may touch.

    ``oldest_batch``: the oldest batch whose offsets Spark still keeps; it never replays an older
    one. ``watermark_ms``: the watermark of the last committed batch, below which every later
    micro-batch drops its events.
    """

    oldest_batch: int
    last_committed: int
    watermark_ms: int


@dataclass(frozen=True)
class Compaction:
    """What ``compact_table`` did: dates compacted, partitions before and after, rows moved."""

    dates: tuple[str, ...] = ()
    partitions_before: int = 0
    partitions_after: int = 0
    rows: int = 0

    def __add__(self, other: Compaction) -> Compaction:
        return Compaction(
            self.dates + other.dates,
            self.partitions_before + other.partitions_before,
            self.partitions_after + other.partitions_after,
            self.rows + other.rows,
        )


def read_checkpoint(spark: SparkSession, checkpoint: str) -> CheckpointState | None:
    """The state of ``checkpoint``, ``None`` before its first committed batch."""
    offsets = numbered_files(spark, f"{checkpoint}/offsets")
    commits = numbered_files(spark, f"{checkpoint}/commits")
    if not offsets or not commits:
        return None
    last = max(commits)
    metadata = json.loads(_read(spark, f"{checkpoint}/offsets/{last}").splitlines()[1])
    return CheckpointState(min(offsets), last, int(metadata["batchWatermarkMs"]))


def archive_inputs(spark: SparkSession, paths: SensorPaths) -> list[str]:
    """Move to the archive the files of ``incoming`` that every query committed; return them."""
    fs, incoming = filesystem(spark, paths.incoming)
    if not fs.exists(incoming):
        return []
    present = {status.getPath().getName(): status for status in fs.listStatus(incoming)}
    committed = set(present)
    for query in OUTPUTS:
        committed &= _committed_names(spark, paths.checkpoint(query), committed)
    moved = []
    for name in sorted(committed):
        status = present[name]
        day = datetime.fromtimestamp(status.getModificationTime() / 1000, UTC).date()
        target = _path(spark, f"{paths.archive}/{ARRIVAL_DATE}={day.isoformat()}/{name}")
        fs.mkdirs(target.getParent())
        if fs.rename(status.getPath(), target):
            moved.append(name)
        else:
            logger.warning("not archived, its archive path exists: %s", status.getPath())
    return moved


def _committed_names(spark: SparkSession, checkpoint: str, names: set[str]) -> set[str]:
    """The ``names`` of files that a committed micro-batch of ``checkpoint``'s query has read."""
    state = read_checkpoint(spark, checkpoint)
    if state is None or not names:
        return set()
    # Spark deletes the log files a compacted one replaces: a vanished file is in a newer one.
    entries = (
        spark.read.option("ignoreMissingFiles", "true")
        .text(f"{checkpoint}/{_SOURCE_LOG}")
        .where(F.col("value").startswith("{"))
        .select(F.from_json("value", "path string, batchId long").alias("entry"))
        .where(F.col("entry.batchId") <= state.last_committed)
        .select(F.element_at(F.split("entry.path", "/"), -1).alias("name"))
        .where(F.col("name").isin(sorted(names)))
        .distinct()
    )
    return {row.name for row in entries.collect()}


def purge_archive(
    spark: SparkSession, paths: SensorPaths, keep_days: int, today: date
) -> list[str]:
    """Delete the archived days before ``today - keep_days``; return them."""
    oldest_kept = (today - timedelta(days=keep_days)).isoformat()
    purged = []
    for day in list_partition_values(spark, paths.archive, ARRIVAL_DATE):
        if day < oldest_kept:
            fs, directory = filesystem(spark, f"{paths.archive}/{ARRIVAL_DATE}={day}")
            fs.delete(directory, True)
            purged.append(day)
    return purged


def compact_table(
    spark: SparkSession, table: str, output: Output, state: CheckpointState, stale_after_s: int
) -> Compaction:
    """Merge the old ``batch_id`` partitions of each eligible date of ``table`` into one.

    Each date is compacted under the table's lock, once any compaction a killed run left is
    finished. The streaming job holds the lock during each write and waits for it in between;
    the compaction waits for it too, ``stale_after_s`` and a margin at most.
    """
    fs, target = filesystem(spark, table)
    if not fs.exists(target):
        return Compaction()
    with _waiting_lock(spark, table, stale_after_s):
        finish_interrupted(spark, table, output)
    done = Compaction()
    for day in list_partition_values(spark, table, output.date_column):
        if output.closes_with_watermark and _day_end_ms(day) > state.watermark_ms:
            continue
        with _waiting_lock(spark, table, stale_after_s):
            done += _compact_date(spark, table, output, day, state.oldest_batch)
    return done


@contextmanager
def _waiting_lock(spark: SparkSession, table: str, stale_after_s: int) -> Iterator[None]:
    """``table_lock``, retried while a live run holds it; a dead run's lock turns stale."""
    deadline = time.monotonic() + stale_after_s + _LOCK_MARGIN_S
    while True:
        with ExitStack() as held:
            try:
                held.enter_context(table_lock(spark, table))
            except TableLockedError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(_LOCK_POLL_S)
                continue
            yield
            return


def finish_interrupted(spark: SparkSession, table: str, output: Output) -> list[str]:
    """Finish the compactions of ``table`` a killed run left; return their staging directories.

    A staging directory without journal was still being written: it is deleted. With a journal,
    the swap is completed. The caller holds the table's lock.
    """
    fs, target = filesystem(spark, table)
    prefix = f"_{target.getName()}.compaction-"
    parent = target.getParent()
    if not fs.exists(parent):
        return []
    found = [s.getPath() for s in fs.listStatus(parent) if s.getPath().getName().startswith(prefix)]
    for stage in found:
        if fs.exists(stage.suffix(f"/{_JOURNAL}")):
            logger.warning("finishing the compaction a killed run left: %s", stage)
            _swap(spark, table, output, stage.toString())
        else:
            logger.warning("removing the compaction a killed run was staging: %s", stage)
            fs.delete(stage, True)
    return [stage.toString() for stage in found]


def _compact_date(
    spark: SparkSession, table: str, output: Output, day: str, oldest_batch: int
) -> Compaction:
    date_dir = f"{table}/{output.date_column}={day}"
    batches = sorted(int(b) for b in list_partition_values(spark, date_dir, BATCH_ID))
    old = [batch for batch in batches if batch < oldest_batch]
    if len(old) < 2:
        return Compaction()
    fs, target = filesystem(spark, table)
    stage = f"{target.getParent()}/_{target.getName()}.compaction-{output.date_column}={day}"
    rows = spark.read.schema(_data_schema(output)).parquet(
        *(f"{date_dir}/{BATCH_ID}={batch}" for batch in old)
    )
    expected = rows.count()
    rows.coalesce(1).write.parquet(f"{stage}/{_MERGED}")
    merged = spark.read.parquet(f"{stage}/{_MERGED}").count()
    if merged != expected:
        fs.delete(_path(spark, stage), True)
        raise RuntimeError(f"{stage}: {merged} rows merged from {expected}")
    _write(spark, f"{stage}/{_JOURNAL}", json.dumps({"batches": old}))
    _swap(spark, table, output, stage)
    logger.info(
        "date compacted",
        extra={"table": table, "date": day, "batches": len(old), "into": old[-1], "rows": merged},
    )
    return Compaction((day,), len(old), 1, merged)


def _swap(spark: SparkSession, table: str, output: Output, stage: str) -> None:
    """Replace the partitions the journal of ``stage`` names by its merged partition.

    The merged partition takes the id of the last batch merged. Once it has moved out of the
    staging directory, the partition of that id is the merged one and is kept.
    """
    fs, _ = filesystem(spark, stage)
    journal = json.loads(_read(spark, f"{stage}/{_JOURNAL}"))
    day = stage.rsplit(f".compaction-{output.date_column}=", 1)[1]
    date_dir = f"{table}/{output.date_column}={day}"
    into = journal["batches"][-1]
    merged = _path(spark, f"{stage}/{_MERGED}")
    moved = not fs.exists(merged)
    for batch in journal["batches"]:
        if not (moved and batch == into):
            fs.delete(_path(spark, f"{date_dir}/{BATCH_ID}={batch}"), True)
    if not moved and not fs.rename(merged, _path(spark, f"{date_dir}/{BATCH_ID}={into}")):
        raise OSError(f"could not move {merged} into {date_dir}")
    fs.delete(_path(spark, stage), True)


def _data_schema(output: Output) -> StructType:
    """The columns stored in the files: the partition columns are directory names."""
    partition = {output.date_column, BATCH_ID}
    return StructType([f for f in output.schema.fields if f.name not in partition])


def _day_end_ms(day: str) -> int:
    start = datetime.fromisoformat(day).replace(tzinfo=UTC)
    return int((start + timedelta(days=1)).timestamp() * 1000)


def _path(spark: SparkSession, path: str) -> Any:
    return filesystem(spark, path)[1]


def _read(spark: SparkSession, path: str) -> str:
    fs, hadoop_path = filesystem(spark, path)
    stream = fs.open(hadoop_path)
    try:
        return bytes(stream.readAllBytes()).decode("utf-8")
    finally:
        stream.close()


def _write(spark: SparkSession, path: str, text: str) -> None:
    fs, hadoop_path = filesystem(spark, path)
    stream = fs.create(hadoop_path, True)
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()
