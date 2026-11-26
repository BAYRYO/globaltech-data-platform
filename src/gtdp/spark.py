"""SparkSession construction with the settings every job relies on."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import SparkSession

from gtdp.config import AppConfig


def build_spark(
    cfg: AppConfig, app_name: str, extra_conf: Mapping[str, str] | None = None
) -> SparkSession:
    """Return the SparkSession for ``cfg``; ``extra_conf`` adds or replaces Spark properties.

    The master is set only when the configuration names one (``local``). On the cluster
    spark-submit passes ``--master yarn``.
    """
    conf = {
        "spark.sql.shuffle.partitions": str(cfg.spark.shuffle_partitions),
        "spark.sql.adaptive.enabled": str(cfg.spark.adaptive).lower(),
        # A batch rewrites exactly the partitions it produces (ADR-0002).
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.session.timeZone": cfg.app.timezone,
        "spark.sql.parquet.compression.codec": "snappy",
        # The console progress bar would interleave with the JSON log lines on stderr.
        "spark.ui.showConsoleProgress": "false",
        **(extra_conf or {}),
    }
    builder = SparkSession.builder.appName(app_name)
    if cfg.spark.master:
        builder = builder.master(cfg.spark.master)
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
