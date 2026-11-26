"""Counts of a sensor stream run, its runs without audit row, and its differences with another.

    scripts/cluster/submit.sh scripts/streaming/report.py ROOT [OTHER_ROOT]

ROOT is a lake root such as /datalake: the run's tables are ROOT/gold/sensor_aggregates,
ROOT/gold/sensor_alerts and ROOT/streaming/rejects. For each table the report gives its rows, its
keys and its duplicate keys, then lists the alerts and the streaming-sensors audit rows of
ROOT/ops/audit. It then lists the streaming-sensors applications YARN knows that ran on
ROOT/landing and have no audit row: a driver killed outright writes none, and YARN reports a
client-mode application whose driver vanished as SUCCEEDED. A run's landing is the
--set paths.landing of its command line, else the configured one. Its counts come from the
progress reports of its Spark event log, as far as it can be read. With OTHER_ROOT it compares
the two runs on keys and values, and exits with 1 on any difference.
"""

import json
import re
import sys
import urllib.request
from collections.abc import Iterator
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from gtdp.config import load_config
from gtdp.io import path_exists, read_table
from gtdp.streaming.sensors import AGGREGATES_SCHEMA, ALERTS_SCHEMA, REJECTS_SCHEMA
from gtdp.streaming.stream import AGGREGATES, OUTPUTS, Totals, filesystem

JOB = "streaming-sensors"
APPLICATION = f"gtdp-{JOB}"
TABLES = {
    "aggregates": ("gold/sensor_aggregates", AGGREGATES_SCHEMA, ("window_start", "sensor_id")),
    "alerts": ("gold/sensor_alerts", ALERTS_SCHEMA, ("alert_id",)),
    "rejects": ("streaming/rejects", REJECTS_SCHEMA, ("source_name", "raw_line")),
}
VALUES = {
    "aggregates": ("event_count", "min_value", "max_value"),
    "alerts": ("sensor_id", "window_start", "direction", "threshold", "value", "event_id"),
    "rejects": ("reject_reason",),
}
_CHUNK = 8 << 20
_LANDING_OPTION = re.compile(r"paths\.landing=(\S+)")


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


def print_audit(spark: SparkSession, root: str) -> set[str]:
    """Print the streaming-sensors audit rows of ``root``; return their YARN application ids."""
    audit = f"{root}/ops/audit"
    rows = []
    if path_exists(spark, audit):
        rows = (
            spark.read.parquet(audit)
            .where(F.col("job_name") == JOB)
            .orderBy("started_at")
            .collect()
        )
    print(f"== {JOB} audit rows ({audit})")
    for row in rows:
        metrics = json.loads(row.metrics_json)
        queries = metrics.get("queries", {})
        late = queries.get("aggregates", {}).get("rows_dropped_by_watermark")
        print(
            f"  {row.run_id} {row.yarn_app_id} {row.status:<8} {row.duration_s:>7.1f} s  "
            f"in {row.rows_in} out {row.rows_out} rejected {row.rows_rejected} "
            f"duplicated {row.rows_duplicated} late {late} stop {metrics.get('stop_reason')!r}"
        )
    return {row.yarn_app_id for row in rows}


def print_unaudited(spark: SparkSession, root: str, audited: set[str]) -> None:
    """Print the applications of the job that read ``root`` and wrote no audit row."""
    landing = f"{root}/landing"
    print(f"== {APPLICATION} applications of {landing} without audit row (crashed or running)")
    default = _path_of(load_config("cluster").paths.landing)
    found = 0
    for app in yarn_applications(spark):
        if app["id"] in audited:
            continue
        run_landing, totals = event_log_run(spark, app["id"], landing, default)
        if run_landing != landing:
            continue
        found += 1
        started = datetime.fromtimestamp(app["startedTime"] / 1000, UTC)
        line = f"  {app['id']} YARN {app['state']}/{app['finalStatus']} started {started:%H:%M:%S}"
        if totals is None:
            print(f"{line}  event log unreadable")
            continue
        counts = totals.snapshot()[AGGREGATES]
        print(
            f"{line}  batches {counts['batches']} in {counts['input_rows']} "
            f"late {counts['rows_dropped_by_watermark']} duplicated {counts['rows_deduplicated']}"
        )
    if not found:
        print("  none")


def yarn_applications(spark: SparkSession) -> list[dict]:
    """The streaming-sensors applications the ResourceManager knows, oldest first."""
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    address = conf.get("yarn.resourcemanager.webapp.address")
    url = f"http://{address}/ws/v1/cluster/apps?applicationTypes=SPARK"
    with urllib.request.urlopen(url, timeout=30) as response:
        apps = (json.load(response).get("apps") or {}).get("app") or []
    return sorted(
        (app for app in apps if app["name"] == APPLICATION), key=lambda app: app["startedTime"]
    )


def event_log_run(
    spark: SparkSession, app_id: str, landing: str, default: str
) -> tuple[str | None, Totals | None]:
    """The landing directory of ``app_id``'s run and the counts of its progress reports.

    The landing comes from the run's command line in its event log (``--set paths.landing``),
    ``default`` without it; the reading stops there when it is not ``landing``. ``(landing,
    None)`` when the log cannot be read; a log cut short by a killed driver, or compacted by the
    History Server, gives the reports it still holds.
    """
    run_landing, totals = None, Totals()
    try:
        for event in _events(spark, app_id):
            if event["Event"].endswith("SparkListenerEnvironmentUpdate"):
                command = event["System Properties"].get("sun.java.command", "")
                found = _LANDING_OPTION.search(command)
                run_landing = _path_of(found.group(1)) if found else default
                if run_landing != landing:
                    return run_landing, None
                continue
            progress = event["progress"]
            name = progress["name"].removeprefix("sensor_")
            if name in OUTPUTS and "addBatch" in progress["durationMs"]:
                # An event log stores the sources' input rows, not their sum.
                rows = sum(source["numInputRows"] for source in progress["sources"])
                totals.add_progress(name, {**progress, "numInputRows": rows})
                totals.add_written(name, 0)
    except Exception as error:
        print(f"  ({app_id}: event log read until {type(error).__name__})", file=sys.stderr)
        return run_landing, None if run_landing is None else totals
    return run_landing, totals


def _path_of(uri: str) -> str:
    """``/datalake/landing`` from ``hdfs://namenode:8020/datalake/landing`` or itself."""
    if "://" not in uri:
        return uri.rstrip("/")
    return "/" + uri.split("://", 1)[1].partition("/")[2].rstrip("/")


def _events(spark: SparkSession, app_id: str) -> Iterator[dict]:
    """The environment and progress events of ``app_id``'s event log files, in order.

    A log is one file, or with rolling a directory of ``events_<n>_<app>`` files.
    """
    jvm = spark.sparkContext._jvm
    log_dir = spark.sparkContext.getConf().get("spark.eventLog.dir")
    fs, _ = filesystem(spark, log_dir)
    files = []
    for status in fs.globStatus(jvm.org.apache.hadoop.fs.Path(f"{log_dir}/*{app_id}*")) or []:
        if status.isDirectory():
            files += [
                (int(f.getPath().getName().split("_")[1]), f.getPath())
                for f in fs.listStatus(status.getPath())
                if f.getPath().getName().startswith("events_")
            ]
        else:
            files.append((0, status.getPath()))
    reader = jvm.org.apache.spark.deploy.history.EventLogFileReader
    for _, path in sorted(files, key=lambda file: file[0]):
        stream = reader.openEventLog(path, fs)
        pending = b""
        try:
            while chunk := bytes(stream.readNBytes(_CHUNK)):
                *lines, pending = (pending + chunk).split(b"\n")
                yield from _wanted(lines)
        finally:
            stream.close()
        yield from _wanted([pending])


def _wanted(lines: list[bytes]) -> Iterator[dict]:
    for line in lines:
        if b"QueryProgressEvent" in line[:200] or b"SparkListenerEnvironmentUpdate" in line[:200]:
            try:
                yield json.loads(line)
            except ValueError:
                continue


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
    roots = [root.rstrip("/") for root in argv]
    runs = [load(spark, root) for root in roots]
    for root, tables in zip(roots, runs, strict=True):
        describe(root, tables)
    status = 1 if len(runs) == 2 and compare(*runs) else 0
    for root in roots:
        print_unaudited(spark, root, print_audit(spark, root))
    spark.stop()
    return status


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
