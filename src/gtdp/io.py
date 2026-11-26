"""Idempotent Parquet writes and path checks on any Hadoop filesystem (HDFS or local)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from urllib.parse import unquote

from pyspark.sql import DataFrame, SparkSession


def write_partitions(df: DataFrame, path: str, partition_cols: Sequence[str]) -> None:
    """Rewrite exactly the partitions present in ``df``; leave every other partition as is.

    Writing the same ``df`` again gives the same table (ADR-0002).
    """
    if not partition_cols:
        raise ValueError("write_partitions needs at least one partition column")
    (
        df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(*partition_cols)
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


def path_exists(spark: SparkSession, path: str) -> bool:
    fs, hadoop_path = _filesystem(spark, path)
    return bool(fs.exists(hadoop_path))


def list_partition_values(spark: SparkSession, path: str, column: str) -> list[str]:
    """Sorted values of the ``<column>=<value>`` directories directly under ``path``."""
    fs, hadoop_path = _filesystem(spark, path)
    if not fs.exists(hadoop_path):
        return []
    prefix = f"{column}="
    names = [status.getPath().getName() for status in fs.listStatus(hadoop_path)]
    # Spark percent-encodes special characters in partition directory names.
    return sorted(unquote(name.removeprefix(prefix)) for name in names if name.startswith(prefix))


def _filesystem(spark: SparkSession, path: str) -> tuple[Any, Any]:
    """The Hadoop FileSystem that owns ``path``, and ``path`` as a Hadoop Path (py4j objects)."""
    context = spark.sparkContext
    hadoop_path = context._jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(context._jsc.hadoopConfiguration()), hadoop_path
