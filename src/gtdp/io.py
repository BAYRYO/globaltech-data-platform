"""Idempotent Parquet writes and path checks on any Hadoop filesystem (HDFS or local)."""

from __future__ import annotations

import itertools
import json
import logging
import signal
import threading
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime
from typing import Any

from py4j.protocol import Py4JJavaError
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

# Partition column of landing, bronze and incremental silver: ingestion_date=YYYY-MM-DD.
INGESTION_DATE = "ingestion_date"
# Spark local property naming the audited run of the current thread, written into table locks.
RUN_ID_PROPERTY = "gtdp.run_id"
# Age in seconds from which a table lock or a staging directory counts as left by a dead run.
STALE_AFTER_CONF = "spark.gtdp.io.staleAfterSeconds"
STALE_AFTER_DEFAULT_S = 6 * 3600
_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)
_FILE_EXISTS = "org.apache.hadoop.fs.FileAlreadyExistsException"

PartitionValue = str | date


class LaterPartitionError(RuntimeError):
    """The target already holds a partition later than the one a job is about to produce."""


class TableLockedError(RuntimeError):
    """Another run holds the lock of the table: it is writing the table right now."""


def write_partitions(
    df: DataFrame, path: str, partitions: Mapping[str, Sequence[PartitionValue]]
) -> None:
    """Replace exactly the named partitions with the rows of ``df``; leave the others as is.

    ``partitions`` maps each partition column, in directory order, to the values this run
    (re)produces, for example ``{"source": ["orders"], "ingestion_date": [date(2026, 1, 2)]}``.
    The run first takes the table's lock (``TableLockedError`` while another run holds it). ``df``
    is written in full to a staging directory of its own, the hidden sibling
    ``_<name>.staging-<id>``. Only after that write succeeds is each named partition deleted and
    replaced by its staged copy, so a failed write leaves the table as it was. SIGTERM and SIGINT
    wait for the end of that swap: a stopped run leaves each partition old or new, never missing.
    A named partition without rows becomes an empty directory holding a ``_SUCCESS`` marker: it
    still counts as produced for ``ensure_no_later_partition``, and ``read_table`` reads it as
    empty. A row outside the named partitions raises ``ValueError`` and publishes nothing. Writing
    the same ``df`` again gives the same table (ADR-0002). A process killed outright between the
    delete and the rename of a partition leaves that partition missing until the day is replayed.
    """
    spark = df.sparkSession
    named = [
        _partition_dir(spark, partitions, values)
        for values in itertools.product(*_value_lists(partitions))
    ]
    fs, target = _filesystem(spark, path)
    staging = f"{target.getParent().toString()}/_{target.getName()}.staging-{uuid.uuid4().hex}"
    with _locked(spark, fs, target):
        try:
            df.write.partitionBy(*partitions).parquet(staging)
            staged = _partition_dirs(fs, _hadoop_path(spark, staging), len(partitions))
            if stray := sorted(staged - set(named)):
                raise ValueError(f"df has rows outside the partitions named for {path}: {stray}")
            with _stop_signals_deferred():
                for directory in named:
                    source = _hadoop_path(spark, f"{staging}/{directory}")
                    if directory not in staged:
                        fs.mkdirs(source)
                        fs.create(_hadoop_path(spark, f"{staging}/{directory}/_SUCCESS")).close()
                    destination = _hadoop_path(spark, f"{path}/{directory}")
                    fs.delete(destination, True)
                    fs.mkdirs(destination.getParent())
                    _rename(fs, source, destination)
        finally:
            fs.delete(_hadoop_path(spark, staging), True)
    # A cached read of the table would keep serving the partitions that were just replaced.
    spark.catalog.refreshByPath(path)


def drop_partitions(
    spark: SparkSession, path: str, partitions: Mapping[str, Sequence[PartitionValue]]
) -> list[str]:
    """Delete the named partitions of ``path``; return those that existed, as ``col=value``.

    ``partitions`` names them as for ``write_partitions``. The table's lock is taken only when one
    of them exists.
    """
    named = [
        _partition_dir(spark, partitions, values)
        for values in itertools.product(*_value_lists(partitions))
    ]
    fs, target = _filesystem(spark, path)
    present = [name for name in named if fs.exists(_hadoop_path(spark, f"{path}/{name}"))]
    if present:
        with _locked(spark, fs, target), _stop_signals_deferred():
            present = [n for n in present if fs.delete(_hadoop_path(spark, f"{path}/{n}"), True)]
        spark.catalog.refreshByPath(path)
    return present


def replace_table(df: DataFrame, path: str) -> None:
    """Replace the whole table at ``path`` with ``df``, which may read ``path`` itself.

    The run holds the table's lock throughout, as ``write_partitions`` does. ``df`` is written in
    full to the hidden sibling ``_<name>.tmp``. Then the old table moves aside to ``_<name>.old``,
    the new one moves into place, and the old one is deleted; stop signals wait for the end of the
    two moves. If a crash left the table missing, ``replace_table``, ``read_table`` and
    ``path_exists`` first put back the latest complete table: ``_<name>.tmp`` when it holds
    ``_SUCCESS``, else ``_<name>.old``.
    """
    spark = df.sparkSession
    fs, target = _filesystem(spark, path)
    with _locked(spark, fs, target):
        _recover(fs, target)
        staging, aside = _siblings(target)
        fs.delete(aside, True)
        df.write.mode("overwrite").parquet(staging.toString())
        if not fs.exists(staging.suffix("/_SUCCESS")):
            fs.create(staging.suffix("/_SUCCESS")).close()
        with _stop_signals_deferred():
            if fs.exists(target):
                _rename(fs, target, aside)
            _rename(fs, staging, target)
        fs.delete(aside, True)
    spark.catalog.refreshByPath(path)


def read_table(
    spark: SparkSession,
    path: str,
    schema: StructType | None = None,
    *,
    merge_schema: bool = False,
) -> DataFrame:
    """Read the Parquet table at ``path``; a missing table or one without data reads as empty.

    With ``schema``, the table is read with it: a column of ``schema`` that a file lacks reads as
    null, and a file column outside ``schema`` is left out. A table holds no data file before its
    first write, or when every day written so far produced no row: it then reads as an empty
    DataFrame of ``schema``. Without ``schema``, the columns are those of the files, the union of
    their schemas with ``merge_schema`` (ADR-0007), and a table without data file raises
    ``FileNotFoundError``.
    """
    fs, hadoop_path = _recovered(spark, path)
    if not fs.exists(hadoop_path) or not _has_data_file(fs, hadoop_path):
        if schema is None:
            raise FileNotFoundError(f"{path} holds no data file: pass its schema to read it empty")
        return spark.createDataFrame([], schema)
    reader = spark.read.option("mergeSchema", merge_schema)
    if schema is not None:
        reader = reader.schema(schema)
    return reader.parquet(path)


def path_exists(spark: SparkSession, path: str) -> bool:
    """Whether ``path`` exists, once an interrupted ``replace_table`` of it is finished."""
    fs, hadoop_path = _recovered(spark, path)
    return bool(fs.exists(hadoop_path))


def list_partition_values(spark: SparkSession, path: str, column: str) -> list[str]:
    """Sorted values of the ``<column>=<value>`` directories under ``path``; ``[]`` if absent."""
    fs, hadoop_path = _filesystem(spark, path)
    if not fs.exists(hadoop_path):
        return []
    prefix = f"{column}="
    names = [status.getPath().getName() for status in fs.listStatus(hadoop_path)]
    return sorted(
        _catalog_utils(spark).unescapePathName(name.removeprefix(prefix))
        for name in names
        if name.startswith(prefix)
    )


def latest_partition_after(
    spark: SparkSession, path: str, column: str, value: PartitionValue
) -> str | None:
    """The greatest ``column`` partition of ``path`` above ``value``, or ``None``."""
    later = [v for v in list_partition_values(spark, path, column) if v > str(value)]
    return later[-1] if later else None


def ensure_no_later_partition(
    spark: SparkSession, path: str, column: str, value: PartitionValue, *, force: bool = False
) -> None:
    """Refuse to produce partition ``value`` while ``path`` holds a later one.

    Silver deduplicates day D against the days before it (ADR-0005): processing D after D+1
    leaves keys duplicated in D+1. With ``force``, log a warning and let the caller replay the
    later days afterwards.
    """
    later = latest_partition_after(spark, path, column, value)
    if later is None:
        return
    message = f"{path} already holds {column}={later}, later than {value}"
    if not force:
        raise LaterPartitionError(f"{message}; replay the days in order or force it")
    logger.warning("%s; forced: replay the later days afterwards", message)


def _partition_dir(
    spark: SparkSession, partitions: Mapping[str, object], values: Sequence[PartitionValue]
) -> str:
    """``col1=v1/col2=v2``, escaped the way Spark names partition directories."""
    escape = _catalog_utils(spark).escapePathName
    pairs = zip(partitions, values, strict=True)
    return "/".join(f"{column}={escape(str(value))}" for column, value in pairs)


def _siblings(target: Any) -> tuple[Any, Any]:
    """``_<name>.tmp`` and ``_<name>.old``: the hidden siblings of a ``replace_table`` swap."""
    parent, name = target.getParent(), target.getName()
    return parent.suffix(f"/_{name}.tmp"), parent.suffix(f"/_{name}.old")


def _recovered(spark: SparkSession, path: str) -> tuple[Any, Any]:
    """``_filesystem(spark, path)``, after putting back a table that a crash left missing.

    A table missing while a live run holds its lock is that run's swap in progress: left alone.
    """
    fs, target = _filesystem(spark, path)
    if not fs.exists(target):
        age = _age_s(fs, _lock_path(target))
        if age is None or age >= _stale_after_s(spark):
            _recover(fs, target)
    return fs, target


def _recover(fs: Any, target: Any) -> None:
    """Put back the table ``target`` if a crash left it missing (see ``replace_table``)."""
    if fs.exists(target):
        return
    staging, aside = _siblings(target)
    source = staging if fs.exists(staging.suffix("/_SUCCESS")) else aside
    if not fs.exists(source):
        return
    try:
        _rename(fs, source, target)
    except (FileExistsError, Py4JJavaError):
        if not fs.exists(target):
            raise
        return  # A concurrent reader put the table back first.
    logger.warning("finished an interrupted replace_table: %s became %s", source, target)


def _rename(fs: Any, source: Any, destination: Any) -> None:
    """Rename ``source`` to ``destination``; ``FileExistsError`` if ``destination`` exists.

    ``FileSystem.rename`` moves a directory into an existing destination directory, which nests a
    partition inside itself. ``FileContext.rename`` refuses instead, in one NameNode call on HDFS.
    """
    jvm = SparkContext._jvm
    context = jvm.org.apache.hadoop.fs.FileContext.getFileContext(fs.getUri(), fs.getConf())
    no_overwrite = SparkContext._gateway.new_array(jvm.org.apache.hadoop.fs.Options.Rename, 0)
    try:
        context.rename(fs.makeQualified(source), fs.makeQualified(destination), no_overwrite)
    except Py4JJavaError as exc:
        if exc.java_exception.getClass().getName() == _FILE_EXISTS:
            raise FileExistsError(f"not renaming {source} onto {destination}: it exists") from None
        raise


def _lock_path(target: Any) -> Any:
    """``_<name>.lock``, beside the table ``target``: ``replace_table`` moves the table itself."""
    return target.getParent().suffix(f"/_{target.getName()}.lock")


@contextmanager
def _locked(spark: SparkSession, fs: Any, target: Any) -> Iterator[None]:
    """Hold the lock of the table ``target`` for the block, then remove dead runs' staging.

    The lock is a file created only if absent, which HDFS decides atomically. It holds the run id,
    the application id and the time it was taken. A lock younger than ``STALE_AFTER_CONF`` seconds
    belongs to a live run: ``TableLockedError`` at once, no wait. An older one was left by a run
    that died: it is taken over with a warning.
    """
    lock, stale_after = _lock_path(target), _stale_after_s(spark)
    holder = json.dumps(
        {
            "run_id": spark.sparkContext.getLocalProperty(RUN_ID_PROPERTY),
            "application_id": spark.sparkContext.applicationId,
            "locked_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "token": uuid.uuid4().hex,
        }
    )
    with _stop_signals_deferred():
        if not _create(fs, lock, holder):
            age = _age_s(fs, lock)
            if age is not None and age < stale_after:
                raise TableLockedError(_locked_message(fs, target, lock))
            if age is not None:
                logger.warning(
                    "taking over the lock of %s, %.0f s old: %s", target, age, _read(fs, lock)
                )
                fs.delete(lock, False)
            if not _create(fs, lock, holder):
                raise TableLockedError(_locked_message(fs, target, lock))
    try:
        _remove_stale_staging(fs, target, stale_after)
        yield
    finally:
        with _stop_signals_deferred():
            if _read(fs, lock) == holder:
                fs.delete(lock, False)


def _locked_message(fs: Any, target: Any, lock: Any) -> str:
    return (
        f"{target} is being written by another run, which holds {lock}: {_read(fs, lock)}. "
        f"Wait for that run to end; if no run is active, delete {lock}"
    )


def _create(fs: Any, path: Any, text: str) -> bool:
    """Create the file ``path`` holding ``text``; ``False`` if it already exists."""
    try:
        stream = fs.create(path, False)
    except Py4JJavaError:
        if fs.exists(path):
            return False
        raise
    try:
        stream.write(bytearray(text.encode()))
    finally:
        stream.close()
    return True


def _read(fs: Any, path: Any) -> str | None:
    """The UTF-8 text of the small file ``path``, ``None`` if it cannot be read."""
    try:
        stream = fs.open(path)
    except Py4JJavaError:
        return None
    try:
        return bytes(stream.readAllBytes()).decode("utf-8", "replace")
    finally:
        stream.close()


def _age_s(fs: Any, path: Any) -> float | None:
    """Seconds since ``path`` was last modified, ``None`` if it does not exist."""
    try:
        modified_ms = fs.getFileStatus(path).getModificationTime()
    except Py4JJavaError:
        return None
    return time.time() - modified_ms / 1000


def _stale_after_s(spark: SparkSession) -> float:
    return float(spark.conf.get(STALE_AFTER_CONF, str(STALE_AFTER_DEFAULT_S)))


def _remove_stale_staging(fs: Any, target: Any, stale_after: float) -> None:
    """Delete the ``_<name>.staging-*`` directories of ``target`` older than ``stale_after``.

    The holder of the lock calls it: such a directory belongs to a run that was killed outright.
    """
    prefix = f"_{target.getName()}.staging-"
    for status in fs.listStatus(target.getParent()):
        path = status.getPath()
        age = time.time() - status.getModificationTime() / 1000
        if path.getName().startswith(prefix) and age >= stale_after:
            logger.warning("removing %s, left %.0f s ago by a run that died", path, age)
            fs.delete(path, True)


@contextmanager
def _stop_signals_deferred() -> Iterator[None]:
    """Hold SIGTERM and SIGINT until the block ends, then raise the first one received.

    The handler that was in place (run.py's stops the run) then runs as if the signal had just
    arrived. Only the main thread receives signals: in another thread the block runs as is.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    received: list[int] = []
    previous = {
        sig: signal.signal(sig, lambda signum, _frame: received.append(signum))
        for sig in _STOP_SIGNALS
    }
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, signal.SIG_DFL if handler is None else handler)
        if received:
            signal.raise_signal(received[0])


def _has_data_file(fs: Any, directory: Any) -> bool:
    """Whether ``directory`` holds, at any depth, a file that Spark reads (not ``_*`` or ``.*``)."""
    for status in fs.listStatus(directory):
        if status.getPath().getName().startswith(("_", ".")):
            continue
        if status.isFile() or _has_data_file(fs, status.getPath()):
            return True
    return False


def _value_lists(
    partitions: Mapping[str, Sequence[PartitionValue]],
) -> list[Sequence[PartitionValue]]:
    """The value lists of ``partitions``; ``TypeError`` for values that name other directories."""
    if not partitions:
        raise ValueError("write_partitions needs at least one partition column")
    for column, values in partitions.items():
        if isinstance(values, str) or not isinstance(values, Sequence):
            raise TypeError(f"partitions[{column!r}] must be a list of values, got {values!r}")
        if datetimes := [value for value in values if isinstance(value, datetime)]:
            raise TypeError(f"partitions[{column!r}] must hold dates, not datetimes: {datetimes}")
    return list(partitions.values())


def _partition_dirs(fs: Any, root: Any, depth: int) -> set[str]:
    """The ``col1=v1/col2=v2`` directories found ``depth`` levels under ``root``."""
    level: list[tuple[tuple[str, ...], Any]] = [((), root)]
    for _ in range(depth):
        level = [
            ((*names, status.getPath().getName()), status.getPath())
            for names, directory in level
            for status in fs.listStatus(directory)
            if status.isDirectory() and not status.getPath().getName().startswith(("_", "."))
        ]
    return {"/".join(names) for names, _ in level}


def _catalog_utils(spark: SparkSession) -> Any:
    return spark.sparkContext._jvm.org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils


def _filesystem(spark: SparkSession, path: str) -> tuple[Any, Any]:
    """The Hadoop FileSystem that owns ``path``, and ``path`` as a Hadoop Path (py4j objects)."""
    hadoop_path = _hadoop_path(spark, path)
    return hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hadoop_path


def _hadoop_path(spark: SparkSession, path: str) -> Any:
    return spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
