"""Idempotent Parquet writes and path checks on any Hadoop filesystem (HDFS or local)."""

from __future__ import annotations

import itertools
import logging
from collections.abc import Mapping, Sequence
from datetime import date
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
    Every named partition is deleted first, so one that ``df`` no longer has a row for does not
    keep stale rows. ``df`` must only hold rows of the named partitions. Writing the same ``df``
    again gives the same table (ADR-0002).
    """
    if not partitions:
        raise ValueError("write_partitions needs at least one partition column")
    spark = df.sparkSession
    for values in itertools.product(*partitions.values()):
        fs, directory = _filesystem(spark, f"{path}/{_partition_dir(spark, partitions, values)}")
        fs.delete(directory, True)
    # Set on the writer, not only in the session: static mode would wipe every other partition.
    (
        df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(*partitions)
        .parquet(path)
    )


def replace_table(df: DataFrame, path: str) -> None:
    """Replace the whole table at ``path`` with ``df``, which may read ``path`` itself.

    ``df`` is written in full to the hidden sibling ``_<name>.tmp``, then renamed over the old
    table. The next run overwrites a sibling that a crashed run left behind. A crash between the
    delete and the rename leaves no table until the next run.
    """
    fs, target = _filesystem(df.sparkSession, path)
    staging_uri = f"{target.getParent().toString()}/_{target.getName()}.tmp"
    _, staging = _filesystem(df.sparkSession, staging_uri)
    df.write.mode("overwrite").parquet(staging_uri)
    if fs.exists(target):
        fs.delete(target, True)
    if not fs.rename(staging, target):
        raise OSError(f"could not rename {staging_uri} to {path}")


def read_table(
    spark: SparkSession,
    path: str,
    schema: StructType | None = None,
    *,
    merge_schema: bool = False,
) -> DataFrame:
    """Read the Parquet table at ``path``; a missing table or one without data reads as empty.

    A table holds no data file before its first write, or when every day written so far produced
    no row. Its empty DataFrame has ``schema``, partition columns included; without ``schema`` such
    a table raises ``FileNotFoundError``. ``merge_schema`` reads the union of the schemas of the
    files (ADR-0007).
    """
    fs, hadoop_path = _filesystem(spark, path)
    if not fs.exists(hadoop_path) or not _has_data_file(fs, hadoop_path):
        if schema is None:
            raise FileNotFoundError(f"{path} holds no data file: pass its schema to read it empty")
        return spark.createDataFrame([], schema)
    return spark.read.option("mergeSchema", merge_schema).parquet(path)


def path_exists(spark: SparkSession, path: str) -> bool:
    fs, hadoop_path = _filesystem(spark, path)
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


def _has_data_file(fs: Any, directory: Any) -> bool:
    """Whether ``directory`` holds, at any depth, a file that Spark reads (not ``_*`` or ``.*``)."""
    for status in fs.listStatus(directory):
        if status.getPath().getName().startswith(("_", ".")):
            continue
        if status.isFile() or _has_data_file(fs, status.getPath()):
            return True
    return False


def _catalog_utils(spark: SparkSession) -> Any:
    return spark.sparkContext._jvm.org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils


def _filesystem(spark: SparkSession, path: str) -> tuple[Any, Any]:
    """The Hadoop FileSystem that owns ``path``, and ``path`` as a Hadoop Path (py4j objects)."""
    context = spark.sparkContext
    hadoop_path = context._jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(context._jsc.hadoopConfiguration()), hadoop_path
