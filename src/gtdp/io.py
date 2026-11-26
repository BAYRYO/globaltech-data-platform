"""Idempotent Parquet writes and path checks on any Hadoop filesystem (HDFS or local)."""

from __future__ import annotations

import itertools
import logging
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

# Partition column of landing, bronze and incremental silver: ingestion_date=YYYY-MM-DD.
INGESTION_DATE = "ingestion_date"

PartitionValue = str | date


class LaterPartitionError(RuntimeError):
    """The target already holds a partition later than the one a job is about to produce."""


def write_partitions(
    df: DataFrame, path: str, partitions: Mapping[str, Sequence[PartitionValue]]
) -> None:
    """Replace exactly the named partitions with the rows of ``df``; leave the others as is.

    ``partitions`` maps each partition column, in directory order, to the values this run
    (re)produces, for example ``{"source": ["orders"], "ingestion_date": [date(2026, 1, 2)]}``.
    ``df`` is written in full to a staging directory of its own, the hidden sibling
    ``_<name>.staging-<id>``. Only after that write succeeds is each named partition deleted and
    replaced by its staged copy, so a failed write leaves the table as it was. A named partition
    without rows becomes an empty directory holding a ``_SUCCESS`` marker: it still counts as
    produced for ``ensure_no_later_partition``, and ``read_table`` reads it as empty. A row outside
    the named partitions raises ``ValueError`` and publishes nothing. Writing the same ``df`` again
    gives the same table (ADR-0002). A crash between the delete and the rename of a partition
    leaves that partition missing until the day is replayed.
    """
    spark = df.sparkSession
    named = [
        _partition_dir(spark, partitions, values)
        for values in itertools.product(*_value_lists(partitions))
    ]
    fs, target = _filesystem(spark, path)
    staging = f"{target.getParent().toString()}/_{target.getName()}.staging-{uuid.uuid4().hex}"
    try:
        df.write.partitionBy(*partitions).parquet(staging)
        staged = _partition_dirs(fs, _hadoop_path(spark, staging), len(partitions))
        if stray := sorted(staged - set(named)):
            raise ValueError(f"df has rows outside the partitions named for {path}: {stray}")
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


def replace_table(df: DataFrame, path: str) -> None:
    """Replace the whole table at ``path`` with ``df``, which may read ``path`` itself.

    ``df`` is written in full to the hidden sibling ``_<name>.tmp``. Then the old table moves
    aside to ``_<name>.old``, the new one moves into place, and the old one is deleted. If a crash
    left the table missing, ``replace_table``, ``read_table`` and ``path_exists`` first put back the
    latest complete table: ``_<name>.tmp`` when it holds ``_SUCCESS``, else ``_<name>.old``.
    """
    spark = df.sparkSession
    fs, target = _recovered(spark, path)
    staging, aside = _siblings(target)
    fs.delete(aside, True)
    df.write.mode("overwrite").parquet(staging.toString())
    if not fs.exists(staging.suffix("/_SUCCESS")):
        fs.create(staging.suffix("/_SUCCESS")).close()
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
    """``_filesystem(spark, path)``, after putting back a table that a crash left missing."""
    fs, target = _filesystem(spark, path)
    if not fs.exists(target):
        staging, aside = _siblings(target)
        source = staging if fs.exists(staging.suffix("/_SUCCESS")) else aside
        # A concurrent reader may win the rename: the table is back all the same.
        if fs.exists(source) and fs.rename(source, target):
            logger.warning("finished an interrupted replace_table: %s became %s", source, target)
    return fs, target


def _rename(fs: Any, source: Any, destination: Any) -> None:
    if not fs.rename(source, destination):
        raise OSError(f"could not rename {source} to {destination}")


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
