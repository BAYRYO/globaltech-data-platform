"""Idempotent Parquet writes and path checks on any Hadoop filesystem (HDFS or local)."""

from __future__ import annotations

import hashlib
import itertools
import json
import logging
import re
import signal
import threading
import time
import urllib.request
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
# Age in seconds from which a staging directory counts as left by a dead run, and a table lock
# too when YARN cannot say whether the run that holds it ended.
STALE_AFTER_CONF = "spark.gtdp.io.staleAfterSeconds"
STALE_AFTER_DEFAULT_S = 6 * 3600
_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)
_FILE_EXISTS = "org.apache.hadoop.fs.FileAlreadyExistsException"
_YARN_APPLICATION = re.compile(r"application_\d+_\d+")
_YARN_ENDED = frozenset({"FINISHED", "FAILED", "KILLED"})
_RM_WEBAPP = "yarn.resourcemanager.webapp.address"
_RM_TIMEOUT_S = 5

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
    A named partition without rows becomes an empty directory holding a ``_SUCCESS`` marker,
    which ``read_table`` reads as empty. A row outside the named partitions raises ``ValueError``
    and publishes nothing. Writing the same ``df`` again gives the same table (ADR-0002). A
    process killed outright between the delete and the rename of a partition leaves that
    partition missing until the day is replayed.
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


@contextmanager
def table_lock(spark: SparkSession, path: str) -> Iterator[None]:
    """Hold the lock of the table at ``path`` for the block, the lock ``write_partitions`` takes.

    For a job that changes a table in steps of its own. ``TableLockedError`` at once while
    another run holds it. Taking it also removes the staging directories of dead runs.
    """
    fs, target = _filesystem(spark, path)
    with _locked(spark, fs, target):
        yield


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
        lock = _lock_path(target)
        held = _read(fs, lock)
        if held is None or _stale_reason(spark, fs, lock, held) is not None:
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

    The lock is a file created only if absent (``_create``). It holds the run id, the application
    id, the time it was taken and a token, so that no two takings write the same content. While
    the run that holds it may be alive: ``TableLockedError`` at once, no wait. A lock left by a
    run that died is taken over (``_acquire``). A stop signal received while the lock is taken
    is raised once it is, and the lock is released.
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
    acquired = False
    try:
        with _stop_signals_deferred():
            acquired = _acquire(spark, fs, lock, holder)
            if not acquired:
                raise TableLockedError(_locked_message(fs, target, lock))
        _remove_leftovers(fs, target, stale_after)
        yield
    finally:
        if acquired:
            with _stop_signals_deferred():
                _release(spark, fs, lock, holder)


def _locked_message(fs: Any, target: Any, lock: Any) -> str:
    return (
        f"{target} is being written by another run, which holds {lock}: {_read(fs, lock)}. "
        "Wait for that run to end: the lock frees itself once YARN reports its application "
        f"ended. Delete {lock} by hand only if no run is active and YARN cannot tell"
    )


def _acquire(spark: SparkSession, fs: Any, path: Any, holder: str) -> bool:
    """Create the lock file ``path`` holding ``holder``; ``False`` while a live run holds it.

    A file left by a run that died (``_stale_reason``) is deleted (``_delete_if_holds``) with a
    warning, then created again; ``False`` if another run deletes or creates it first.
    """
    if _create(fs, path, holder):
        return True
    held = _read(fs, path)
    if held is None:
        return _create(fs, path, holder)
    reason = _stale_reason(spark, fs, path, held)
    if reason is None or not _delete_if_holds(spark, fs, path, held, holder):
        return False
    logger.warning("taking over the lock %s, left by a run that died (%s): %s", path, reason, held)
    return _create(fs, path, holder)


def _release(spark: SparkSession, fs: Any, lock: Any, holder: str) -> None:
    """Delete the lock file ``lock`` if it still holds ``holder`` (``_delete_if_holds``).

    A lock that another run took over, the age rule having taken this run for dead, is left to it.
    """
    held = _read(fs, lock)
    if held is None:
        return
    if held != holder or not _delete_if_holds(spark, fs, lock, holder, holder):
        logger.warning("leaving the lock %s to the run that took it over from this one", lock)


def _delete_if_holds(spark: SparkSession, fs: Any, path: Any, held: str, claimant: str) -> bool:
    """Delete the lock file ``path`` if it still holds ``held``; ``False`` if not, or if claimed.

    Every delete of a lock file, by the run that holds it or by a taker, goes through here. The run
    first creates the claim ``<path>.takeover-<digest of held>`` holding ``claimant`` with
    ``_acquire``, so a claim left by a run that died is taken over in turn. Then it reads ``path``
    again and deletes it if it still holds ``held``. Only the run that holds that claim deletes a
    file holding ``held``, and no content is written twice, so ``path`` cannot change between that
    read and the delete.
    """
    claim = path.suffix(f".takeover-{hashlib.sha256(held.encode()).hexdigest()[:16]}")
    if not _acquire(spark, fs, claim, claimant):
        return False
    try:
        if _read(fs, path) != held:
            return False
        fs.delete(path, False)
        return True
    finally:
        if _read(fs, claim) == claimant:
            fs.delete(claim, False)


def _stale_reason(spark: SparkSession, fs: Any, path: Any, held: str) -> str | None:
    """Why the lock file ``path``, which holds ``held``, was left by a run that died, or ``None``.

    For a YARN application, the ResourceManager decides: ended (FINISHED, FAILED, KILLED) is dead
    whatever the age of the lock, any other state alive whatever its age. Without a state from
    YARN, the lock is dead once older than ``STALE_AFTER_CONF`` seconds.
    """
    application_id = _application_id(held)
    if application_id is None or not _YARN_APPLICATION.fullmatch(application_id):
        logger.info("%s names no YARN application (%s): judged by its age", path, application_id)
    else:
        try:
            state = _yarn_state(spark, application_id)
        except Exception as exc:
            logger.warning(
                "no state from YARN for %s (%r): %s judged by its age", application_id, exc, path
            )
        else:
            return f"{application_id} is {state}" if state in _YARN_ENDED else None
    age = _age_s(fs, path)
    if age is not None and age >= _stale_after_s(spark):
        return f"{age:.0f} s old"
    return None


def _application_id(held: str) -> str | None:
    """The ``application_id`` that the lock content ``held`` names, if any."""
    try:
        value = json.loads(held).get("application_id")
    except (ValueError, AttributeError):
        return None
    return value if isinstance(value, str) else None


def _yarn_state(spark: SparkSession, application_id: str) -> str:
    """The state of ``application_id`` that the ResourceManager's REST API reports."""
    address = spark.sparkContext._jsc.hadoopConfiguration().get(_RM_WEBAPP)
    if not address:
        raise LookupError(f"{_RM_WEBAPP} is not set")
    url = f"http://{address}/ws/v1/cluster/apps/{application_id}"
    with urllib.request.urlopen(url, timeout=_RM_TIMEOUT_S) as response:
        return str(json.load(response)["app"]["state"])


def _create(fs: Any, path: Any, text: str) -> bool:
    """Create the file ``path`` holding ``text``; ``False`` if it already exists.

    ``text`` is written to a file of its own, ``<path>.tmp-<id>``, then renamed onto ``path``
    only if ``path`` is absent. A file at ``path`` therefore always holds all of ``text``, even if
    the run dies while it writes.
    """
    staged = path.suffix(f".tmp-{uuid.uuid4().hex}")
    stream = fs.create(staged, False)
    try:
        stream.write(bytearray(text.encode()))
    finally:
        stream.close()
    try:
        _rename(fs, staged, path)
    except FileExistsError:
        fs.delete(staged, False)
        return False
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


def _remove_leftovers(fs: Any, target: Any, stale_after: float) -> None:
    """Delete the staging directories, lock claims and lock files being written of ``target``
    older than ``stale_after``.

    The holder of the lock calls it: they belong to runs that were killed outright.
    """
    name = target.getName()
    prefixes = (f"_{name}.staging-", f"_{name}.lock.")
    for status in fs.listStatus(target.getParent()):
        path = status.getPath()
        age = time.time() - status.getModificationTime() / 1000
        if path.getName().startswith(prefixes) and age >= stale_after:
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
