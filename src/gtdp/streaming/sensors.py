"""Transformations of the sensor stream, from raw JSON lines to gold aggregates and alerts.

The functions map streaming DataFrames to streaming DataFrames (they also work on static ones),
except ``first_alerts``, applied to each micro-batch. The semantics are those of ADR-0008: a
2-minute watermark on ``event_time``, deduplication by ``event_id`` within the watermark, 1-minute
tumbling windows written once in append mode, and alerts emitted in the micro-batch where the
first breach of their window arrives (see ``first_alerts`` for which breach the alert carries).
"""

from __future__ import annotations

import operator
from collections.abc import Mapping

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
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
"""Second partition column of every output: the micro-batch that wrote the rows."""
WINDOW_DATE = "window_date"
"""First partition column of the aggregates and the alerts: the UTC date of ``window_start``."""
ARRIVAL_DATE = "arrival_date"
"""First partition column of the rejects: the UTC date its file arrived (modification time)."""

_CORRUPT = "_corrupt_record"
_VALUES = "_json_values"
_STRICT_JSON = {
    "allowNonNumericNumbers": "false",
    "allowSingleQuotes": "false",
    "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX",
}
"""JSON reader options: standard JSON only, and timestamps with their UTC offset."""
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
_LINE_COLUMNS = ("raw_line", "source_file", ARRIVAL_DATE)
_PARSE_SCHEMA = StructType(
    [StructField("event", StructType(list(_EVENT_FIELDS))), StructField(_CORRUPT, StringType())]
)

REJECT_RULES: tuple[tuple[str, str], ...] = (
    ("MALFORMED_JSON", f"{_CORRUPT} IS NOT NULL OR coalesce(size({_VALUES}), 0) <> 1"),
    (
        "MISSING_FIELD",
        "event_id IS NULL OR sensor_id IS NULL OR site IS NULL OR machine_id IS NULL"
        " OR metric IS NULL OR event_time IS NULL OR emitted_at IS NULL",
    ),
    ("NULL_VALUE", "value IS NULL"),
    ("BAD_VALUE", "isnan(value) OR abs(value) = double('Infinity')"),
    ("FUTURE_EVENT", "event_time > emitted_at + make_dt_interval(0, 0, 0, {future_tolerance_s})"),
)
"""(code, SQL condition) in priority order: a rejected line gets the code of its first match.

``{future_tolerance_s}`` is ``[streaming] future_tolerance_s``: an event dated more than that
after its emission would move the watermark ahead of the stream.
"""

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
        StructField(WINDOW_DATE, DateType()),
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
        StructField(WINDOW_DATE, DateType()),
        StructField(BATCH_ID, IntegerType()),
    ]
)
REJECTS_SCHEMA = StructType(
    [
        StructField("reject_reason", StringType()),
        StructField("raw_line", StringType()),
        StructField("source_file", StringType()),
        StructField(ARRIVAL_DATE, DateType()),
        StructField(BATCH_ID, IntegerType()),
    ]
)


def parse(lines: DataFrame, future_tolerance_s: int) -> DataFrame:
    """Parse ``raw_line`` against the contract schema and add ``reject_reason`` (null if valid).

    ``lines`` holds ``raw_line``, ``source_file`` and ``arrival_date``. A line is valid JSON for
    the contract when it is exactly one JSON object. The JSON reader stops at the end of the first
    value and ignores what follows, so the line is read twice: as the value of an object,
    ``{"event": <line>}``, which fails if anything but ``}`` follows the object, and as an array,
    ``[<line>]``, which fails if anything but ``]`` follows it or holds more than one value.
    Garbage after the object cannot start with both ``}`` and ``]``: it breaks one of the two
    readings.
    """
    wrapped = F.concat(F.lit('{"event":'), "raw_line", F.lit("}"))
    listed = F.concat(F.lit("["), "raw_line", F.lit("]"))
    options = {**_STRICT_JSON, "columnNameOfCorruptRecord": _CORRUPT}
    parsed = lines.select(
        *_LINE_COLUMNS,
        F.from_json(wrapped, _PARSE_SCHEMA, options).alias("parsed"),
        F.from_json(listed, "array<string>", _STRICT_JSON).alias(_VALUES),
    ).select(*_LINE_COLUMNS, "parsed.event.*", f"parsed.{_CORRUPT}", _VALUES)
    reason = F.coalesce(
        *(
            F.when(F.expr(rule.format(future_tolerance_s=future_tolerance_s)), F.lit(code))
            for code, rule in REJECT_RULES
        )
    )
    return parsed.withColumn("reject_reason", reason).drop(_CORRUPT, _VALUES)


def rejects(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.col("reject_reason").isNotNull()).select(
        "reject_reason", "raw_line", "source_file", ARRIVAL_DATE
    )


def events(parsed: DataFrame) -> DataFrame:
    """Valid events, watermarked, each ``event_id`` kept once while the watermark allows.

    An event older than the watermark is dropped and counted in ``numRowsDroppedByWatermark``.
    Micro-batch ``k`` drops the events older than the watermark computed at the end of batch
    ``k - 2``, so the first two micro-batches of a new checkpoint drop nothing.
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
            F.to_date("window.start").alias(WINDOW_DATE),
        )
    )


def threshold_breaches(events: DataFrame, thresholds: DataFrame) -> DataFrame:
    """Every event beyond a threshold, as a candidate alert of its sensor and 1-minute window.

    ``alert_id`` hashes the sensor and the window start: the same in every run and every replay.
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
            F.to_date(window.start).alias(WINDOW_DATE),
        )
    )


def first_alerts(breaches: DataFrame, alerted: DataFrame) -> DataFrame:
    """The earliest breach of each sensor and window of ``breaches`` not in ``alerted`` yet.

    Applied to each micro-batch, with ``alerted`` the alerts of the earlier batches: a window
    alerts in the micro-batch where its first breach arrives, without waiting for its end, and
    never again. The alert carries the breach of smallest ``event_time`` (then ``event_id``) among
    those of that micro-batch only: a breach with a smaller ``event_time`` that arrives in a later
    micro-batch does not replace it. The alert's event fields therefore follow the arrival order,
    the files and how ``maxFilesPerTrigger`` cuts them into micro-batches: the same files in the
    same micro-batches give the same alert, and a replayed micro-batch rewrites the same alert.
    """
    earliest = Window.partitionBy("alert_id").orderBy("event_time", "event_id")
    return (
        breaches.withColumn("rank", F.row_number().over(earliest))
        .where(F.col("rank") == 1)
        .drop("rank")
        .join(alerted.select("alert_id"), "alert_id", "left_anti")
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
