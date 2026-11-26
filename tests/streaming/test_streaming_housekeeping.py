"""streaming-housekeeping: compaction keeps rows, bounds partitions, survives kills; archive."""

import json
import shutil
import threading
import time
from collections import Counter
from datetime import date

import pytest
from pyspark.sql import functions as F
from stream_helpers import drop_files, event, file_name, lake, local_path, run_stream

from gtdp.io import list_partition_values, read_table
from gtdp.streaming import housekeeping
from gtdp.streaming.housekeeping import (
    archive_inputs,
    compact_table,
    finish_interrupted,
    purge_archive,
    read_checkpoint,
)
from gtdp.streaming.sensors import ARRIVAL_DATE, BATCH_ID, WINDOW_DATE
from gtdp.streaming.stream import AGGREGATES, ALERTS, OUTPUTS, REJECTS, SCHEMAS

RETAINED = 3
STALE_AFTER_S = 300


def at(event_id: str, stamp: str, value: float | None) -> str:
    """A contract event at ``stamp`` (``YYYY-MM-DDTHH:MM:SS``), emitted at the same time."""
    line = json.loads(event(event_id, "00:00:00", value))
    return json.dumps({**line, "event_time": f"{stamp}.000Z", "emitted_at": f"{stamp}.000Z"})


def files_across_midnight(first: int, count: int) -> list[list[str]]:
    """``count`` files of 20 s each from 2025-12-31 23:57:00 + ``first`` files; a breach in two."""
    files = []
    for index in range(first, first + count):
        seconds = 23 * 3600 + 57 * 60 + 20 * index
        day = "2025-12-31" if seconds < 86400 else "2026-01-01"
        stamp = f"{day}T{seconds % 86400 // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"
        lines = [at(f"E{index}", stamp, 90.0 if index % 7 == 0 else 60.0), "not json"]
        files.append(lines)
    return files


@pytest.fixture
def retained_batches(spark):
    """Spark keeps the offsets of the last ``RETAINED`` batches, instead of 100."""
    key = "spark.sql.streaming.minBatchesToRetain"
    previous = spark.conf.get(key)
    spark.conf.set(key, str(RETAINED))
    yield
    spark.conf.set(key, previous)


def build(spark, root):
    """A stream of 30 files, one micro-batch each, across midnight; Spark keeps few batches."""
    key = "spark.sql.streaming.minBatchesToRetain"
    previous = spark.conf.get(key)
    spark.conf.set(key, str(RETAINED))
    paths, settings = lake(root)
    drop_files(paths, files_across_midnight(0, 30))
    run_stream(spark, paths, settings)
    spark.conf.set(key, previous)
    return paths, settings


@pytest.fixture(scope="module")
def built_stream(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("built") / "lake"
    build(spark, root)
    return root


@pytest.fixture
def stream(built_stream, tmp_path):
    """A copy of the built stream's tables and checkpoints: enough to compact, not to restart."""
    root = tmp_path / "lake"
    shutil.copytree(built_stream, root)
    return lake(root)


@pytest.fixture
def own_stream(spark, tmp_path):
    """A stream built in this test's directory, which the stream can restart from."""
    return build(spark, tmp_path / "lake")


def content(spark, paths) -> dict[str, Counter]:
    """Every row of each table, without its ``batch_id``, with file names instead of paths."""
    found = {}
    for name in OUTPUTS:
        frame = read_table(spark, paths.tables[name], SCHEMAS[name]).drop(BATCH_ID)
        if "source_file" in frame.columns:
            frame = frame.withColumn("source_file", F.element_at(F.split("source_file", "/"), -1))
        found[name] = Counter(tuple(row) for row in frame.collect())
    return found


def batch_partitions(spark, paths, name) -> dict[str, list[int]]:
    table, column = paths.tables[name], OUTPUTS[name].date_column
    return {
        day: sorted(
            int(b) for b in list_partition_values(spark, f"{table}/{column}={day}", BATCH_ID)
        )
        for day in list_partition_values(spark, table, column)
    }


def compact_all(spark, paths) -> dict:
    done = {}
    for name, output in OUTPUTS.items():
        state = read_checkpoint(spark, paths.checkpoint(name))
        done[name] = compact_table(spark, paths.tables[name], output, state, STALE_AFTER_S)
    return done


def test_compaction_keeps_every_row_and_leaves_one_partition_per_closed_date(spark, stream):
    paths, _ = stream
    before = content(spark, paths)
    partitions = {name: batch_partitions(spark, paths, name) for name in OUTPUTS}
    oldest = read_checkpoint(spark, paths.checkpoint(REJECTS)).oldest_batch
    assert len(partitions[AGGREGATES]["2025-12-31"]) == 3  # 23:57, 23:58 and 23:59

    done = compact_all(spark, paths)

    assert content(spark, paths) == before
    after = {name: batch_partitions(spark, paths, name) for name in OUTPUTS}
    # 2025-12-31 is behind the watermark: one partition, that of the last batch merged.
    assert after[AGGREGATES]["2025-12-31"] == [partitions[AGGREGATES]["2025-12-31"][-1]]
    assert done[AGGREGATES].dates == ("2025-12-31",)
    # 2026-01-01 is still open: the stream may still write and read it, it is left as is.
    assert after[AGGREGATES]["2026-01-01"] == partitions[AGGREGATES]["2026-01-01"]
    # Rejects: in each arrival day, the batches older than the checkpoint keeps become one.
    for day, batches in partitions[REJECTS].items():
        old = [b for b in batches if b < oldest]
        recent = [b for b in batches if b >= oldest]
        assert after[REJECTS][day] == (old[-1:] if len(old) > 1 else old) + recent
    assert done[REJECTS].partitions_before > RETAINED


def test_compaction_waits_for_the_lock_a_streaming_write_holds(spark, stream):
    paths, _ = stream
    before = content(spark, paths)
    lock = local_path(paths.tables[AGGREGATES]).parent / "_sensor_aggregates.lock"
    lock.write_text('{"run_id": "a micro-batch writing"}')
    threading.Timer(3.0, lock.unlink).start()
    started = time.monotonic()

    done = compact_all(spark, paths)

    assert time.monotonic() - started >= 3.0
    assert done[AGGREGATES].dates == ("2025-12-31",)
    assert content(spark, paths) == before


def test_compaction_run_again_changes_nothing(spark, stream):
    paths, _ = stream
    compact_all(spark, paths)
    partitions = {name: batch_partitions(spark, paths, name) for name in OUTPUTS}
    before = content(spark, paths)

    done = compact_all(spark, paths)

    assert {name: d.partitions_before for name, d in done.items()} == dict.fromkeys(OUTPUTS, 0)
    assert {name: batch_partitions(spark, paths, name) for name in OUTPUTS} == partitions
    assert content(spark, paths) == before


class KilledHousekeeping(BaseException):
    """Stands for a SIGKILL: nothing after it runs."""


class KillAfter:
    """A Hadoop FileSystem that dies when its ``kill_at``-th delete or rename starts.

    A compaction of 2025-12-31 moves its journal in, deletes its 3 partitions, moves the merged
    one in, then deletes its staging directory: 6 steps.
    """

    def __init__(self, fs, kill_at: int):
        self._fs, self._kill_at, self.calls = fs, kill_at, 0

    def __getattr__(self, name):
        return getattr(self._fs, name)

    def _step(self):
        if self.calls == self._kill_at:
            raise KilledHousekeeping
        self.calls += 1

    def delete(self, path, recursive):
        self._step()
        return self._fs.delete(path, recursive)

    def rename(self, source, target):
        self._step()
        return self._fs.rename(source, target)


@pytest.mark.parametrize("kill_at", range(6), ids=lambda n: f"step{n}")
def test_a_compaction_killed_at_any_step_is_finished_by_the_next_run(
    spark, stream, monkeypatch, kill_at
):
    paths, _ = stream
    before = content(spark, paths)
    real = housekeeping.filesystem
    dying = []

    def filesystem(spark_, path):
        fs, hadoop_path = real(spark_, path)
        if not dying:
            dying.append(KillAfter(fs, kill_at))
        return dying[0], hadoop_path

    monkeypatch.setattr(housekeeping, "filesystem", filesystem)
    with pytest.raises(KilledHousekeeping):
        compact_table(
            spark,
            paths.tables[AGGREGATES],
            OUTPUTS[AGGREGATES],
            read_checkpoint(spark, paths.checkpoint(AGGREGATES)),
            STALE_AFTER_S,
        )
    monkeypatch.undo()

    compact_all(spark, paths)

    assert content(spark, paths) == before
    assert len(batch_partitions(spark, paths, AGGREGATES)["2025-12-31"]) == 1
    parent = local_path(paths.tables[AGGREGATES]).parent
    assert not list(parent.glob("_sensor_aggregates.compaction-*"))


def test_a_compaction_killed_while_staging_is_discarded_and_redone(spark, stream):
    paths, _ = stream
    before = content(spark, paths)
    parent = local_path(paths.tables[AGGREGATES]).parent
    stage = parent / f"_sensor_aggregates.compaction-{WINDOW_DATE}=2025-12-31" / "merged"
    stage.mkdir(parents=True)
    (stage / "part-00000.parquet").write_bytes(b"half a file")

    assert finish_interrupted(spark, paths.tables[AGGREGATES], OUTPUTS[AGGREGATES])
    compact_all(spark, paths)

    assert content(spark, paths) == before
    assert len(batch_partitions(spark, paths, AGGREGATES)["2025-12-31"]) == 1


class KilledWhileWritingTheJournal:
    """A Hadoop FileSystem that dies once it has created a journal file, before writing it."""

    def __init__(self, fs):
        self._fs = fs

    def __getattr__(self, name):
        return getattr(self._fs, name)

    def create(self, path, *args):
        stream = self._fs.create(path, *args)
        if "_journal" in path.getName():
            stream.close()
            raise KilledHousekeeping
        return stream


def test_a_compaction_killed_while_writing_its_journal_is_discarded_and_redone(
    spark, stream, monkeypatch
):
    paths, _ = stream
    before = content(spark, paths)
    real = housekeeping.filesystem
    monkeypatch.setattr(
        housekeeping,
        "filesystem",
        lambda spark_, path: (
            KilledWhileWritingTheJournal(real(spark_, path)[0]),
            real(spark_, path)[1],
        ),
    )
    with pytest.raises(KilledHousekeeping):
        compact_table(
            spark,
            paths.tables[AGGREGATES],
            OUTPUTS[AGGREGATES],
            read_checkpoint(spark, paths.checkpoint(AGGREGATES)),
            STALE_AFTER_S,
        )
    monkeypatch.undo()

    compact_all(spark, paths)

    assert content(spark, paths) == before
    assert len(batch_partitions(spark, paths, AGGREGATES)["2025-12-31"]) == 1


def test_the_stream_goes_on_after_compaction_as_if_it_had_not_happened(
    spark, own_stream, retained_batches, tmp_path
):
    paths, settings = own_stream
    compact_all(spark, paths)
    archive_inputs(spark, paths)
    more = files_across_midnight(30, 20)
    drop_files(paths, more, first=30)
    run_stream(spark, paths, settings)
    reference, _ = lake(tmp_path / "reference")
    drop_files(reference, files_across_midnight(0, 50))

    run_stream(spark, reference, settings)

    assert content(spark, paths) == content(spark, reference)
    alerts = read_table(spark, paths.tables[ALERTS], SCHEMAS[ALERTS]).collect()
    assert len({a.alert_id for a in alerts}) == len(alerts) > 0


def test_only_the_files_every_query_committed_are_archived(spark, own_stream):
    paths, settings = own_stream
    incoming = local_path(paths.incoming)
    drop_files(paths, [[event("LATE", "00:30:00", 60.0)]], first=30)

    archived = archive_inputs(spark, paths)

    assert sorted(archived) == sorted(file_name(i) for i in range(30))
    assert [p.name for p in incoming.iterdir() if not p.name.startswith(".")] == [file_name(30)]
    [day] = list_partition_values(spark, paths.archive, ARRIVAL_DATE)
    assert len(list((local_path(paths.archive) / f"{ARRIVAL_DATE}={day}").glob("sensors_*"))) == 30
    assert archive_inputs(spark, paths) == []

    metrics = run_stream(spark, paths, settings)

    assert metrics[AGGREGATES]["input_rows"] == 1
    assert sorted(archive_inputs(spark, paths)) == [file_name(30)]


def test_the_archive_keeps_the_configured_number_of_days(spark, stream):
    paths, _ = stream
    archive = local_path(paths.archive)
    for day in ("2026-11-20", "2026-11-24", "2026-11-25", "2026-11-26"):
        (archive / f"{ARRIVAL_DATE}={day}").mkdir(parents=True)
        (archive / f"{ARRIVAL_DATE}={day}" / file_name(0)).write_text("x\n")

    purged = purge_archive(spark, paths, keep_days=1, today=date(2026, 11, 26))

    assert purged == ["2026-11-20", "2026-11-24"]
    assert list_partition_values(spark, paths.archive, ARRIVAL_DATE) == ["2026-11-25", "2026-11-26"]


def test_a_checkpoint_says_which_batches_the_housekeeping_may_touch(spark, stream):
    paths, _ = stream

    state = read_checkpoint(spark, paths.checkpoint(AGGREGATES))

    assert 0 < state.oldest_batch <= state.last_committed - RETAINED
    # The last file holds 2026-01-01 00:06:40: the watermark is 2 minutes earlier.
    assert state.watermark_ms == (1767225600 + 4 * 60 + 40) * 1000
