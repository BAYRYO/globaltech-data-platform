"""Counts of a sensor stream run, and its differences with another run.

    scripts/cluster/submit.sh scripts/streaming/report.py ROOT [OTHER_ROOT]

ROOT is a lake root such as /datalake: the run's tables are ROOT/gold/sensor_aggregates,
ROOT/gold/sensor_alerts and ROOT/streaming/rejects. For each table the report gives its rows, its
keys and its duplicate keys, then lists the alerts and the streaming-sensors audit rows. With
OTHER_ROOT it compares the two runs on keys and values, and exits with 1 on any difference.
"""

import json
import sys

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from gtdp.audit import audit_path
from gtdp.config import load_config
from gtdp.io import read_table
from gtdp.streaming.sensors import AGGREGATES_SCHEMA, ALERTS_SCHEMA, REJECTS_SCHEMA

TABLES = {
    "aggregates": ("gold/sensor_aggregates", AGGREGATES_SCHEMA, ("window_start", "sensor_id")),
    "alerts": ("gold/sensor_alerts", ALERTS_SCHEMA, ("alert_id",)),
    "rejects": ("streaming/rejects", REJECTS_SCHEMA, ("source_name", "raw_line")),
}
VALUES = {
    "aggregates": ("event_count", "min_value", "max_value"),
    "alerts": ("sensor_id", "window_start", "direction", "threshold"),
    "rejects": ("reject_reason",),
}


def load(spark: SparkSession, root: str) -> dict[str, DataFrame]:
    tables = {}
    for name, (folder, schema, _) in TABLES.items():
        table = read_table(spark, f"{root}/{folder}", schema)
        if name == "rejects":
            table = table.withColumn("source_name", F.element_at(F.split("source_file", "/"), -1))
        tables[name] = table.cache()
    return tables


def describe(root: str, tables: dict[str, DataFrame]) -> None:
    print(f"== {root}")
    for name, table in tables.items():
        keys = TABLES[name][2]
        rows, distinct = table.count(), table.select(*keys).distinct().count()
        print(f"{name:<11} rows {rows:>6}  keys {distinct:>6}  duplicate keys {rows - distinct}")
    aggregates = (
        tables["aggregates"]
        .agg(
            F.sum("event_count").alias("events"),
            F.countDistinct("window_start").alias("windows"),
            F.countDistinct("sensor_id").alias("sensors"),
            F.date_format(F.min("window_start"), "yyyy-MM-dd HH:mm").alias("first"),
            F.date_format(F.max("window_start"), "yyyy-MM-dd HH:mm").alias("last"),
        )
        .first()
    )
    print(
        f"aggregates: {aggregates.events} events in {aggregates.windows} windows "
        f"({aggregates.first} to {aggregates.last}) x {aggregates.sensors} sensors"
    )
    reasons = tables["rejects"].groupBy("reject_reason").count().orderBy("reject_reason")
    print("rejects:", ", ".join(f"{r.reject_reason} {r['count']}" for r in reasons.collect()))
    print("alerts (sensor, window, direction threshold, value, event time, batch):")
    alerts = (
        tables["alerts"]
        .orderBy("sensor_id", "window_start")
        .select(
            "sensor_id",
            F.date_format("window_start", "HH:mm").alias("window"),
            "direction",
            "threshold",
            "value",
            F.date_format("event_time", "HH:mm:ss.SSS").alias("at"),
            "batch_id",
        )
    )
    for a in alerts.collect():
        print(
            f"  {a.sensor_id:<20} {a.window}  {a.direction} {a.threshold:<4}  {a.value:<6}"
            f"  {a.at}  {a.batch_id}"
        )


def differs(name: str) -> Column:
    changed = [~F.col(f"a.{c}").eqNullSafe(F.col(f"b.{c}")) for c in VALUES[name]]
    if name == "aggregates":
        changed.append(F.abs(F.col("a.avg_value") - F.col("b.avg_value")) > 1e-9)
    condition = changed[0]
    for other in changed[1:]:
        condition = condition | other
    return condition


def compare(first: dict[str, DataFrame], second: dict[str, DataFrame]) -> int:
    print("== comparison (first run - second run)")
    differences = 0
    for name in TABLES:
        keys = list(TABLES[name][2])
        a, b = first[name], second[name]
        left, right = a.select(*keys).distinct(), b.select(*keys).distinct()
        only_a, only_b = left.subtract(right).count(), right.subtract(left).count()
        changed = a.alias("a").join(b.alias("b"), keys).where(differs(name)).count()
        rows = a.count() - b.count()
        print(
            f"{name:<11} row difference {rows}  keys only in first {only_a}  "
            f"only in second {only_b}  value differences {changed}"
        )
        differences += abs(rows) + only_a + only_b + changed
    print("IDENTICAL" if differences == 0 else f"DIFFERENT ({differences})")
    return differences


def print_audit(spark: SparkSession) -> None:
    rows = (
        spark.read.parquet(audit_path(load_config("cluster")))
        .where(F.col("job_name") == "streaming-sensors")
        .orderBy("started_at")
        .collect()
    )
    print("== streaming-sensors audit rows")
    for row in rows:
        metrics = json.loads(row.metrics_json)
        queries = metrics.get("queries", {})
        late = queries.get("aggregates", {}).get("rows_dropped_by_watermark")
        print(
            f"  {row.run_id} {row.yarn_app_id} {row.status:<8} {row.duration_s:>7.1f} s  "
            f"in {row.rows_in} out {row.rows_out} rejected {row.rows_rejected} "
            f"duplicated {row.rows_duplicated} late {late} stop {metrics.get('stop_reason')!r}"
        )


def main(argv: list[str]) -> int:
    if len(argv) not in (1, 2):
        print(__doc__, file=sys.stderr)
        return 2
    spark = (
        SparkSession.builder.appName("gtdp-streaming-report")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    runs = [load(spark, root.rstrip("/")) for root in argv]
    for root, tables in zip(argv, runs, strict=True):
        describe(root, tables)
    status = 1 if len(runs) == 2 and compare(*runs) else 0
    print_audit(spark)
    spark.stop()
    return status


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
