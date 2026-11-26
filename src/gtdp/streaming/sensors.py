"""Transformations of the sensor stream, from raw JSON lines to gold aggregates and alerts.

Every function maps streaming DataFrames to streaming DataFrames (they also work on static ones),
with the semantics of ADR-0008: a 2-minute watermark on ``event_time``, deduplication by
``event_id`` within the watermark, 1-minute tumbling windows written once in append mode, and
alerts emitted in the micro-batch where the event arrives.
"""

from __future__ import annotations

import operator
from collections.abc import Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

WATERMARK_DELAY = "2 minutes"
"""Ordinary delays reach 90 s and are kept; delays beyond 5 min are dropped (data contract)."""
WINDOW_DURATION = "1 minute"
BATCH_ID = "batch_id"
"""Partition column of every output: the micro-batch that wrote the rows."""

_CORRUPT = "_corrupt_record"
_EVENT_FIELDS = (
    StructField("event_id", StringType()),
    StructField("sensor_id", StringType()),
    StructField("site", StringType()),
    StructField("machine_id", StringType()),
    StructField("metric", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("event_time", TimestampType()),
    StructField("emitted_at", TimestampType()),
)
EVENT_COLUMNS = tuple(field.name for field in _EVENT_FIELDS)
_PARSE_SCHEMA = StructType([*_EVENT_FIELDS, StructField(_CORRUPT, StringType())])

REJECT_RULES: tuple[tuple[str, str], ...] = (
    ("MALFORMED_JSON", f"{_CORRUPT} IS NOT NULL OR trim(raw_line) = ''"),
    (
        "MISSING_FIELD",
        "event_id IS NULL OR sensor_id IS NULL OR site IS NULL OR machine_id IS NULL"
        " OR metric IS NULL OR event_time IS NULL",
    ),
    ("NULL_VALUE", "value IS NULL"),
)
"""(code, SQL condition) in priority order: a rejected line gets the code of its first match."""

BREACHES = {"above": operator.gt, "below": operator.lt}
"""How a value crosses a threshold of each direction of ``[streaming.thresholds]``."""

THRESHOLDS_SCHEMA = StructType(
    [
        StructField("metric", StringType(), False),
        StructField("direction", StringType(), False),
        StructField("threshold", DoubleType(), False),
    ]
)
AGGREGATES_SCHEMA = StructType(
    [
        StructField("window_start", TimestampType()),
        StructField("window_end", TimestampType()),
        StructField("site", StringType()),
        StructField("machine_id", StringType()),
        StructField("sensor_id", StringType()),
        StructField("metric", StringType()),
        StructField("event_count", LongType()),
        StructField("avg_value", DoubleType()),
        StructField("min_value", DoubleType()),
        StructField("max_value", DoubleType()),
        StructField(BATCH_ID, IntegerType()),
    ]
)
ALERTS_SCHEMA = StructType(
    [
        StructField("alert_id", StringType()),
        StructField("sensor_id", StringType()),
        StructField("machine_id", StringType()),
        StructField("site", StringType()),
        StructField("metric", StringType()),
        StructField("window_start", TimestampType()),
        StructField("window_end", TimestampType()),
        StructField("value", DoubleType()),
        StructField("threshold", DoubleType()),
        StructField("direction", StringType()),
        StructField("event_time", TimestampType()),
        StructField("event_id", StringType()),
        StructField(BATCH_ID, IntegerType()),
    ]
)
REJECTS_SCHEMA = StructType(
    [
        StructField("reject_reason", StringType()),
        StructField("raw_line", StringType()),
        StructField("source_file", StringType()),
        StructField(BATCH_ID, IntegerType()),
    ]
)


def parse(lines: DataFrame) -> DataFrame:
    """Parse ``raw_line`` against the contract schema and add ``reject_reason`` (null if valid).

    ``lines`` holds ``raw_line`` and ``source_file``. A line that is not a JSON object of the
    contract is ``MALFORMED_JSON``, whatever leniency the JSON reader has.
    """
    options = {"columnNameOfCorruptRecord": _CORRUPT}
    parsed = lines.select(
        "raw_line", "source_file", F.from_json("raw_line", _PARSE_SCHEMA, options).alias("event")
    ).select("raw_line", "source_file", "event.*")
    reason = F.coalesce(*(F.when(F.expr(rule), F.lit(code)) for code, rule in REJECT_RULES))
    return parsed.withColumn("reject_reason", reason).drop(_CORRUPT)


def rejects(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.col("reject_reason").isNotNull()).select(
        "reject_reason", "raw_line", "source_file"
    )


def events(parsed: DataFrame) -> DataFrame:
    """Valid events, watermarked, each ``event_id`` kept once while the watermark allows.

    An event older than the watermark is dropped and counted in ``numRowsDroppedByWatermark``.
    The late-event filter of a micro-batch uses the watermark of two batches before it, so the
    first two micro-batches of a new checkpoint drop nothing.
    """
    return (
        parsed.where(F.col("reject_reason").isNull())
        .select(*EVENT_COLUMNS)
        .withWatermark("event_time", WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark(["event_id"])
    )


def window_aggregates(events: DataFrame) -> DataFrame:
    """Count, average, minimum and maximum per 1-minute window and sensor.

    In append mode a window is emitted once, when the watermark passes its end.
    """
    return (
        events.groupBy(
            F.window("event_time", WINDOW_DURATION), "site", "machine_id", "sensor_id", "metric"
        )
        .agg(
            F.count("*").alias("event_count"),
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "site",
            "machine_id",
            "sensor_id",
            "metric",
            "event_count",
            "avg_value",
            "min_value",
            "max_value",
        )
    )


def threshold_alerts(events: DataFrame, thresholds: DataFrame) -> DataFrame:
    """One alert per sensor and 1-minute window: its first event beyond a threshold.

    The alert leaves in the micro-batch of that event, without waiting for the window to close.
    ``alert_id`` hashes the sensor and the window start, so a replayed batch rewrites the same ids.
    When several breaches of one window arrive in the same micro-batch, any one of them is kept.
    """
    window = F.window("event_time", WINDOW_DURATION)
    crossed = F.coalesce(
        *(
            F.when(F.col("direction") == direction, compare(F.col("value"), F.col("threshold")))
            for direction, compare in BREACHES.items()
        )
    )
    return (
        events.join(F.broadcast(thresholds), "metric")
        .where(crossed)
        .select(
            F.sha2(F.concat_ws("|", "sensor_id", window.start.cast("long")), 256).alias("alert_id"),
            "sensor_id",
            "machine_id",
            "site",
            "metric",
            window.start.alias("window_start"),
            window.end.alias("window_end"),
            "value",
            "threshold",
            "direction",
            "event_time",
            "event_id",
        )
        .dropDuplicatesWithinWatermark(["alert_id"])
    )


def thresholds_frame(
    spark: SparkSession, thresholds: Mapping[str, Mapping[str, float]]
) -> DataFrame:
    """``[streaming.thresholds]`` as rows of (metric, direction, threshold)."""
    rows = [
        (metric, direction, float(value))
        for metric, limits in sorted(thresholds.items())
        for direction, value in sorted(limits.items())
    ]
    return spark.createDataFrame(rows, THRESHOLDS_SCHEMA)
