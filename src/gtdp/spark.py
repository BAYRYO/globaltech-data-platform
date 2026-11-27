"""SparkSession construction with the settings every job relies on."""

from __future__ import annotations

import logging
from collections.abc import Mapping

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from gtdp.config import AppConfig

logger = logging.getLogger(__name__)


def build_spark(
    cfg: AppConfig, app_name: str, extra_conf: Mapping[str, str] | None = None
) -> SparkSession:
    """Return the SparkSession for ``cfg``; ``extra_conf`` adds or replaces Spark properties.

    For each property set here, the first of these sources that has it wins: spark-submit
    (``--conf`` or ``spark-defaults.conf``), then ``extra_conf``, then the gtdp configuration
    (``app.toml`` < ``env/<env>.toml`` < ``GTDP_*`` variables < ``run.py --set``). The
    exception is ``spark.ui.showConsoleProgress``, which stays ``false`` because the progress bar
    would cut the JSON log lines on stderr, and PySpark turns it on for any ``python`` driver. The
    effective values are logged with their origin.

    The master is set only when the configuration names one (``local``). On the cluster
    spark-submit passes ``--master yarn``.
    """
    conf = {
        "spark.sql.shuffle.partitions": str(cfg.spark.shuffle_partitions),
        "spark.sql.adaptive.enabled": str(cfg.spark.adaptive).lower(),
        "spark.sql.session.timeZone": cfg.app.timezone,
        "spark.sql.parquet.compression.codec": "snappy",
        **(extra_conf or {}),
    }
    # Starts the JVM, or joins the one spark-submit started, to read the properties it was given.
    SparkContext._ensure_initialized()
    launched = SparkConf()
    origins = {key: "spark-submit" if launched.contains(key) else "gtdp" for key in conf}
    builder = SparkSession.builder.appName(app_name).config("spark.ui.showConsoleProgress", "false")
    if cfg.spark.master:
        builder = builder.master(cfg.spark.master)
    for key, value in conf.items():
        if origins[key] == "gtdp":
            builder = builder.config(key, value)
    spark = builder.getOrCreate()
    effective = {
        key: {"value": spark.conf.get(key, None), "origin": origin}
        for key, origin in origins.items()
    }
    logger.info("spark configuration", extra={"spark_conf": effective})
    return spark
