"""Lot 7 benchmarks (KAN-44 to KAN-46): timed Spark runs, measured from the History Server.

    scripts/perf/bench.sh <experiment> [--runs N] [--root HDFS_DIR] [--out DIR] [options]

The driver of this application runs the experiment, then reads its own stages from the Spark
History Server once the application has stopped. Experiments:

    prepare      write the benchmark tables of every layout from a generator landing
    formats      KAN-44: the revenue query on CSV, Parquet, partitioned Parquet, snappy and zstd
    skew         KAN-45: the gold interval join, skewed, salted, with AQE skew join, broadcast
    cache        KAN-46: indicators over the priced orders, with and without cache
    partitions   KAN-46: a wide aggregation for several spark.sql.shuffle.partitions
    explain      KAN-46: three explain("formatted") plans of the dev lake
    record-hash  the bronze _record_hash column: its share of the files, hex against binary

A timed experiment runs one warm-up round (run 0, left out of the report), then ``--runs`` rounds,
each running every variant once in turn. A variant sets its SQL settings for its own run only.
Before each run the driver waits until YARN runs no application besides its own and records the
check; afterwards it lists any application that overlapped the run. It tags the Spark jobs of each
run with a job group, from which the History Server gives the stages, the bytes they read and the
task quantiles of the run's key stage. It writes ``runs.json``, ``runs.csv`` and ``report.md``
to ``--out``; an untimed experiment writes ``result.json``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import statistics
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gtdp.config import load_config
from gtdp.gold import facts
from gtdp.gold.tables import DIM_PRODUCT, FACT_SALES, ORDER_DATE, table_path
from gtdp.io import read_table
from gtdp.perf import datasets as ds
from gtdp.perf.history import (
    QUANTILES,
    completed_stages,
    group_stage_ids,
    key_stage,
    skew,
    totals,
)
from gtdp.perf.mr_compare import ACTIVE_STATES, RM, SHS, Cluster, overlapping
from gtdp.perf.skew import salted_applicable_price
from gtdp.schemas import RECORD_HASH, SOURCES, bronze_schema
from gtdp.silver.pipelines import PIPELINES
from gtdp.silver.pipelines import table_path as silver_path
from gtdp.silver.products import with_applicable_price
from gtdp.spark import build_spark

ROOT = "hdfs://namenode:8020/benchmarks/perf/full"
LANDING = "hdfs://namenode:8020/benchmarks/mr-vs-spark/full/landing"
UI = "http://localhost:18080/history"
NO_BROADCAST = {"spark.sql.autoBroadcastJoinThreshold": "-1"}
AQE_OFF = {"spark.sql.adaptive.enabled": "false"}


@dataclass(frozen=True)
class Variant:
    """One way to run the work: ``action`` runs it and returns a check of its result, equal for
    every run of every variant of an experiment, and details to keep, such as storage figures.

    ``conf`` holds SQL settings applied for the run only. ``metric`` picks the key stage, whose
    task quantiles the report gives: the largest ``shuffleReadBytes`` (the stage after the
    shuffle) or the largest ``inputBytes`` (the scan). Variants of one ``family`` compute the
    same result."""

    name: str
    action: Callable[[SparkSession], tuple[object, dict]]
    conf: Mapping[str, str] = field(default_factory=dict)
    metric: str = "shuffleReadBytes"
    family: str = ""


@dataclass
class Run:
    variant: str
    run: int
    group: str
    started_ms: int
    wall_s: float
    yarn_check: str
    check: object
    details: dict = field(default_factory=dict)
    stages: dict = field(default_factory=dict)
    key_stage: dict = field(default_factory=dict)
    overlapping: list[str] = field(default_factory=list)


def timed(spark: SparkSession, experiment: str, variants: Sequence[Variant], runs: int):
    """Run every variant once per round, a warm-up round then ``runs`` rounds."""
    cluster, sc, measured = Cluster(Path(".")), spark.sparkContext, []
    for run in range(runs + 1):
        for variant in variants:
            saved = {key: spark.conf.get(key, None) for key in variant.conf}
            for key, value in variant.conf.items():
                spark.conf.set(key, value)
            group = f"{experiment}/{variant.name}/{run}"
            sc.setJobGroup(group, group)
            check = yarn_idle(cluster, sc.applicationId)
            started_ms, start = time.time_ns() // 1_000_000, time.perf_counter()
            result, details = variant.action(spark)
            wall_s = time.perf_counter() - start
            measured.append(
                Run(variant.name, run, group, started_ms, wall_s, check, result, details)
            )
            print(f"{group}: {wall_s:.2f} s {result}", flush=True)
            for key, value in saved.items():
                spark.conf.unset(key) if value is None else spark.conf.set(key, value)
    return measured


def yarn_idle(cluster: Cluster, own: str, max_wait_s: int = 1800) -> str:
    """Wait until YARN runs no application but ``own``; return the evidence of the last check."""
    deadline = time.monotonic() + max_wait_s
    while True:
        apps = (cluster.get(f"{RM}/apps?states={ACTIVE_STATES}").get("apps") or {}).get("app", [])
        others = sorted(app["id"] for app in apps if app["id"] != own)
        stamp = datetime.now(UTC).isoformat(timespec="seconds")
        if not others:
            return f"{stamp}: 0 other application {ACTIVE_STATES.replace(',', '/')}"
        if time.monotonic() > deadline:
            raise TimeoutError(f"YARN still runs {others} after {max_wait_s} s")
        time.sleep(15)


def measure(app_id: str, runs: list[Run], metrics: Mapping[str, str]) -> None:
    """Fill each run's stage totals, key stage and overlaps from the History Server and the RM."""
    cluster = Cluster(Path("."))
    cluster.poll(f"{SHS}/{app_id}", lambda p: p["attempts"][0]["completed"], timeout_s=600)
    jobs = cluster.get(f"{SHS}/{app_id}/jobs")
    stages = cluster.get(f"{SHS}/{app_id}/stages")
    apps = (cluster.get(f"{RM}/apps").get("apps") or {}).get("app", [])
    quantiles = ",".join(str(q) for q in QUANTILES)
    for run in runs:
        own = completed_stages(stages, group_stage_ids(jobs, run.group))
        if not own:
            raise LookupError(f"the History Server has no completed stage of {run.group}")
        run.stages = totals(own)
        metric = metrics[run.variant]
        stage = key_stage(own, metric)
        sid, attempt = stage["stageId"], stage["attemptId"]
        summary = cluster.get(
            f"{SHS}/{app_id}/stages/{sid}/{attempt}/taskSummary?quantiles={quantiles}"
        )
        found = skew(stage, summary, metric)
        run.key_stage = asdict(found) | {
            "stage": sid,
            "duration_ratio": found.duration_ratio,
            "read_ratio": found.read_ratio,
            "spilled_bytes": stage.get("diskBytesSpilled", 0),
            "url": f"{UI}/{app_id}/stages/stage/?id={sid}&attempt={attempt}",
        }
        ended_ms = run.started_ms + round(run.wall_s * 1000)
        run.overlapping = overlapping(apps, app_id, run.started_ms, ended_ms)


STAGE_COLUMNS = (
    "inputBytes",
    "shuffleReadBytes",
    "diskBytesSpilled",
    "numCompleteTasks",
    "executorRunTime",
)


def _median(values: Sequence[float]) -> float:
    return statistics.median(values) if values else 0.0


def summarize(runs: Sequence[Run]) -> list[dict]:
    """Per variant, in order of first run, over the measured runs (run > 0): run count, median,
    min and max wall time, medians of the stage totals and of the key-stage ratios, the distinct
    result checks of every run, warm-up included, the applications that overlapped a run, and
    the gains of the median time against the first and the previous variant, and of the bytes
    read against the first."""
    rows = []
    for name in dict.fromkeys(r.variant for r in runs):
        own = [r for r in runs if r.variant == name and r.run > 0]
        walls = [r.wall_s for r in own]
        checks = {json.dumps(r.check, default=str) for r in runs if r.variant == name}
        rows.append(
            {
                "variant": name,
                "n": len(own),
                "median_s": _median(walls),
                "min_s": min(walls, default=0.0),
                "max_s": max(walls, default=0.0),
                **{key: _median([r.stages.get(key, 0) for r in own]) for key in STAGE_COLUMNS},
                "duration_ratio": _median([r.key_stage.get("duration_ratio", 0) for r in own]),
                "read_ratio": _median([r.key_stage.get("read_ratio", 0) for r in own]),
                "checks": sorted(checks),
                "overlapped": sorted({a for r in own for a in r.overlapping}),
            }
        )
    for previous, row in zip([rows[0], *rows], rows, strict=False):
        row["gain_pct"] = gain(rows[0]["median_s"], row["median_s"])
        row["step_gain_pct"] = gain(previous["median_s"], row["median_s"])
        row["read_gain_pct"] = gain(rows[0]["inputBytes"], row["inputBytes"])
    return rows


def gain(before: float, after: float) -> float:
    """The reduction from ``before`` to ``after`` in percent: 25.0 means a quarter less."""
    return 100 * (before - after) / before if before else 0.0


def markdown(rows: Sequence[Mapping]) -> str:
    """The report table of ``summarize`` rows."""
    head = (
        "| Variante | n | Durée médiane (s) | Min à max (s) | Gain | Gain sur la précédente "
        "| Octets lus | Gain octets lus | Shuffle lu | Déversé disque | Tâches "
        "| Étape clé : max/médiane durée | max/médiane octets |\n"
        "| --- |" + " ---: |" * 12
    )
    lines = [
        f"| `{r['variant']}` | {r['n']} | {r['median_s']:.2f} | {r['min_s']:.2f} à "
        f"{r['max_s']:.2f} | {r['gain_pct']:.1f} % | {r['step_gain_pct']:.1f} % "
        f"| {r['inputBytes']:,.0f} | {r['read_gain_pct']:.1f} % "
        f"| {r['shuffleReadBytes']:,.0f} | {r['diskBytesSpilled']:,.0f} "
        f"| {r['numCompleteTasks']:.0f} | {r['duration_ratio']:.2f} | {r['read_ratio']:.2f} |"
        for r in rows
    ]
    return "\n".join([head, *lines]).replace(",", " ")


def stored_markdown(stored: Mapping[str, Mapping[str, int]]) -> str:
    """The bytes stored per layout, logical and on the DataNodes, and the gains between them."""
    lines = [
        "| Disposition | Octets logiques | Octets physiques | Gain sur la première "
        "| Gain sur la précédente |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    names = list(stored)
    for previous, name in zip([names[0], *names], names, strict=False):
        logical = stored[name]["logical"]
        lines.append(
            f"| `{name}` | {logical:,} | {stored[name]['physical']:,} "
            f"| {gain(stored[names[0]]['logical'], logical):.1f} % "
            f"| {gain(stored[previous]['logical'], logical):.1f} % |".replace(",", " ")
        )
    return "\n".join(lines)


RUN_COLUMNS = (
    "variant",
    "run",
    "wall_s",
    "inputBytes",
    "shuffleReadBytes",
    "diskBytesSpilled",
    "numCompleteTasks",
    "duration_ratio",
    "read_ratio",
    "stage_url",
    "yarn_check",
    "overlapping",
)


def runs_csv(runs: Sequence[Run]) -> str:
    """One line per run, warm-up included: the evidence of ``runs.json`` without the details."""
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(RUN_COLUMNS)
    for r in runs:
        writer.writerow(
            [
                r.variant,
                r.run,
                f"{r.wall_s:.3f}",
                *(r.stages.get(key, 0) for key in RUN_COLUMNS[3:7]),
                f"{r.key_stage.get('duration_ratio', 0):.2f}",
                f"{r.key_stage.get('read_ratio', 0):.2f}",
                r.key_stage.get("url", ""),
                r.yarn_check,
                " ".join(r.overlapping),
            ]
        )
    return out.getvalue()


def rows_check(rows: Sequence) -> list:
    """Row count and SHA-256 prefix of the sorted rows: equal checks mean equal results."""
    text = "\n".join(sorted(str(tuple(row)) for row in rows))
    return [len(rows), hashlib.sha256(text.encode()).hexdigest()[:16]]


def du(path: str) -> dict[str, int]:
    """``hdfs dfs -du -s``: logical bytes, and bytes on the DataNodes (replicas or parity)."""
    done = subprocess.run(
        ["hdfs", "dfs", "-du", "-s", path], capture_output=True, text=True, check=True
    )
    logical, physical, _ = done.stdout.split(maxsplit=2)
    return {"logical": int(logical), "physical": int(physical)}


def formats(spark: SparkSession, args: argparse.Namespace) -> list[Variant]:
    """The revenue query on each layout, its tables read in the timed run, as a batch job reads
    them. Two more runs of the partitioned layout locate the cost of its 396 directories: listed
    by the driver instead of by a Spark job, then listed once before the timed runs."""

    def query(layout: ds.Layout, listed: bool = False) -> Callable[[SparkSession], tuple]:
        def tables(spark: SparkSession) -> list[DataFrame]:
            return [ds.read(spark, args.root, layout, name) for name in ds.SCHEMAS]

        known = tables(spark) if listed else None

        def run(spark: SparkSession) -> tuple[object, dict]:
            frame = ds.revenue_by_day(*(known or tables(spark)), args.first, args.last)
            return rows_check(frame.collect()), {}

        return run

    partitioned = ds.LAYOUT["parquet_part"]
    driver = {"spark.sql.sources.parallelPartitionDiscovery.threshold": "1024"}
    return [
        *[Variant(layout.name, query(layout), metric="inputBytes") for layout in ds.LAYOUTS],
        Variant("parquet_part+driver-listing", query(partitioned), driver, "inputBytes"),
        Variant("parquet_part+listed", query(partitioned, listed=True), metric="inputBytes"),
    ]


def priced_orders(spark: SparkSession, root: str) -> tuple[DataFrame, DataFrame]:
    orders = ds.read(spark, root, ds.LAYOUT["parquet"], "orders")
    return orders, read_table(spark, f"{root}/versions", ds.VERSIONS)


def skew_variants(spark: SparkSession, args: argparse.Namespace) -> list[Variant]:
    orders, versions = priced_orders(spark, args.root)

    def plain(spark: SparkSession) -> tuple[object, dict]:
        return ds.priced_check(with_applicable_price(orders, versions)), {}

    def salted(spark: SparkSession) -> tuple[object, dict]:
        return ds.priced_check(salted_applicable_price(orders, versions, args.salts)), {}

    aqe = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": str(args.skew_factor),
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": args.skew_threshold,
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": args.advisory_size,
    }
    return [
        Variant("baseline", plain, AQE_OFF | NO_BROADCAST),
        Variant(f"salted-{args.salts}", salted, AQE_OFF | NO_BROADCAST),
        Variant("aqe-skew-join", plain, aqe | NO_BROADCAST),
        Variant("broadcast", plain, AQE_OFF, metric="inputBytes"),
    ]


def _completed(fact: DataFrame) -> DataFrame:
    return fact.where(F.col("status") == "COMPLETED")


INDICATORS: tuple[Callable[[DataFrame], DataFrame], ...] = (
    lambda fact: _completed(fact).groupBy("order_date").agg(F.sum("revenue_eur")),
    lambda fact: (
        _completed(fact)
        .groupBy("product_id")
        .agg(F.sum("revenue_eur").alias("revenue"))
        .orderBy(F.desc("revenue"), "product_id")
        .limit(10)
    ),
    lambda fact: fact.groupBy("channel").agg(
        F.avg((F.col("status") == "CANCELLED").cast("int")),
        F.avg((F.col("status") == "RETURNED").cast("int")),
    ),
    lambda fact: (
        _completed(fact)
        .groupBy("channel", F.trunc("order_date", "month").alias("month"))
        .agg(F.sum("revenue_eur"))
    ),
    lambda fact: fact.groupBy("order_date", "status").count(),
    lambda fact: _completed(fact).groupBy("product_id").agg(F.avg("revenue_eur")),
)
"""Indicators of the priced orders: daily completed revenue, the 10 products of highest
completed revenue, cancelled and returned shares per channel, monthly revenue per channel, lines
per day and status, average completed line per product."""


def indicators(fact: DataFrame, count: int) -> list:
    """The checks of the first ``count`` indicators of ``fact``, one Spark action each."""
    return [rows_check(build(fact).collect()) for build in INDICATORS[:count]]


def storage(spark: SparkSession) -> list[dict]:
    """The cached RDDs of the application: partitions cached, bytes in memory and on disk."""
    return [
        {
            "name": info.name(),
            "cached_partitions": info.numCachedPartitions(),
            "partitions": info.numPartitions(),
            "memory_bytes": info.memSize(),
            "disk_bytes": info.diskSize(),
        }
        for info in spark.sparkContext._jsc.sc().getRDDStorageInfo()
    ]


def cache_variants(spark: SparkSession, args: argparse.Namespace) -> list[Variant]:
    """Indicators over the priced orders, the fact cached or recomputed by each indicator: 3 then
    6 indicators with the broadcast join of gold, 3 with the join shuffled (sort-merge)."""
    orders, versions = priced_orders(spark, args.root)
    revenue = (
        F.col("quantity") * F.col("applicable_unit_price") * (1 - F.col("discount_pct"))
    ).alias("revenue_eur")

    def fact() -> DataFrame:
        return with_applicable_price(orders, versions).select(
            "order_date", "product_id", "channel", "status", revenue
        )

    def uncached(count: int) -> Callable[[SparkSession], tuple[object, dict]]:
        return lambda _: (indicators(fact(), count), {})

    def cached(count: int) -> Callable[[SparkSession], tuple[object, dict]]:
        def run(spark: SparkSession) -> tuple[object, dict]:
            frame = fact().persist(StorageLevel.MEMORY_AND_DISK)
            checks = indicators(frame, count)
            held = storage(spark)
            frame.unpersist(blocking=True)
            return checks, {"cached": held, "after_unpersist": storage(spark)}

        return run

    plain = [
        Variant(f"{label}-{n}", action(n), family=str(n))
        for n in (3, len(INDICATORS))
        for label, action in (("no-cache", uncached), ("cache", cached))
    ]
    shuffled = [
        Variant(f"{label}-3-sort-merge", action(3), NO_BROADCAST, family="3")
        for label, action in (("no-cache", uncached), ("cache", cached))
    ]
    return plain + shuffled


def partitions_variants(spark: SparkSession, args: argparse.Namespace) -> list[Variant]:
    orders = ds.read(spark, args.root, ds.LAYOUT["parquet"], "orders")

    def aggregate(_: SparkSession) -> tuple[object, dict]:
        pairs = orders.groupBy("customer_id", "product_id").agg(
            F.sum(ds.amount()).alias("revenue"), F.count(F.lit(1)).alias("lines")
        )
        row = pairs.agg(F.count(F.lit(1)), F.sum("revenue"), F.sum("lines")).first()
        return [str(value) for value in row], {}

    fixed = {"spark.sql.adaptive.coalescePartitions.enabled": "false"}
    variants = [
        Variant(f"sp-{n}", aggregate, fixed | {"spark.sql.shuffle.partitions": str(n)})
        for n in args.partitions
    ]
    coalesced = {"spark.sql.adaptive.coalescePartitions.enabled": "true"}
    top = max(args.partitions)
    shuffle = {"spark.sql.shuffle.partitions": str(top)}
    return [*variants, Variant(f"sp-{top}-aqe-coalesce", aggregate, coalesced | shuffle)]


TIMED: dict[str, Callable[[SparkSession, argparse.Namespace], list[Variant]]] = {
    "formats": formats,
    "skew": skew_variants,
    "cache": cache_variants,
    "partitions": partitions_variants,
}


def prepare(spark: SparkSession, args: argparse.Namespace) -> dict:
    counts = ds.prepare(spark, args.landing, args.root)
    for name in ds.SCHEMAS:
        if len({counts[f"{layout.name}/{name}"] for layout in ds.LAYOUTS}) > 1:
            raise AssertionError(f"the layouts of {name} hold different row counts: {counts}")
    stored = {
        f"{layout.name}/{name}": du(f"{args.root}/{layout.name}/{name}")
        for layout in ds.LAYOUTS
        for name in ds.SCHEMAS
    }
    return {"rows": counts, "stored": stored}


def column_sizes(spark: SparkSession, path: str) -> dict[str, dict[str, int]]:
    """Compressed and uncompressed bytes of each column of the Parquet files under ``path``,
    summed from the file footers."""
    jvm, conf = spark._jvm, spark._jsc.hadoopConfiguration()
    root = jvm.org.apache.hadoop.fs.Path(path)
    files = root.getFileSystem(conf).listFiles(root, True)
    compressed, uncompressed = Counter(), Counter()
    while files.hasNext():
        file_path = files.next().getPath()
        if not file_path.getName().endswith(".parquet"):
            continue
        footer = jvm.org.apache.parquet.hadoop.ParquetFileReader.readFooter(conf, file_path)
        for block in footer.getBlocks():
            for column in block.getColumns():
                name = column.getPath().toDotString()
                compressed[name] += column.getTotalSize()
                uncompressed[name] += column.getTotalUncompressedSize()
    return {
        name: {"compressed": compressed[name], "uncompressed": uncompressed[name]}
        for name in compressed
    }


def record_hash(spark: SparkSession, args: argparse.Namespace) -> dict:
    """Bytes of the bronze orders files per column, then the ``_record_hash`` column written
    alone as its hex text and as the 32 bytes it encodes, both snappy."""
    bronze = f"{load_config('cluster').paths.bronze}/orders"
    hashes = read_table(spark, bronze, bronze_schema(SOURCES["orders"])).select(RECORD_HASH)
    target = f"{args.root}/record_hash"
    hashes.write.mode("overwrite").parquet(f"{target}/hex")
    hashes.select(F.unhex(RECORD_HASH).alias(RECORD_HASH)).write.mode("overwrite").parquet(
        f"{target}/binary"
    )
    return {
        "bronze_orders": column_sizes(spark, bronze),
        "bronze_du": du(bronze),
        **{form: column_sizes(spark, f"{target}/{form}") for form in ("hex", "binary")},
        "rows": hashes.count(),
    }


def plan(spark: SparkSession, frame: DataFrame) -> str:
    """``frame.explain("formatted")`` as text."""
    return spark._jvm.PythonSQLUtils.explainString(frame._jdf.queryExecution(), "formatted")


def explain(spark: SparkSession, args: argparse.Namespace) -> dict:
    """Three plans of the dev lake: a scan of ``fact_sales`` that prunes ``order_date``
    partitions, a scan of ``silver/orders`` with filters pushed to Parquet, and the gold fact
    build (``facts.sales``), broadcast and with broadcast off (sort-merge join). Each plan is
    also written to ``<out>/<name>.txt``."""
    paths = load_config("cluster").paths
    sales = read_table(spark, table_path(paths.gold, FACT_SALES.name), FACT_SALES.schema)
    products = read_table(spark, table_path(paths.gold, DIM_PRODUCT.name), DIM_PRODUCT.schema)

    def orders() -> DataFrame:
        return read_table(spark, silver_path(paths.silver, "orders"), PIPELINES["orders"].schema)

    month = F.col(ORDER_DATE).between(args.first, args.last)
    cancelled = (F.col("status") == "CANCELLED") & (F.col("quantity") >= 10)
    plans = {
        "pruning": plan(spark, sales.where(month).groupBy(ORDER_DATE).agg(F.sum("revenue_eur"))),
        "pushdown": plan(spark, orders().where(cancelled).select("transaction_id", "quantity")),
        "join-broadcast": plan(spark, facts.sales(orders(), products)),
    }
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    plans["join-sort-merge"] = plan(spark, facts.sales(orders(), products))
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    for name, text in plans.items():
        (args.out / f"{name}.txt").write_text(text)
    return plans


UNTIMED: dict[str, Callable[[SparkSession, argparse.Namespace], dict]] = {
    "prepare": prepare,
    "record-hash": record_hash,
    "explain": explain,
}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bench", description=__doc__.split("\n\n")[0])
    parser.add_argument("experiment", choices=[*TIMED, *UNTIMED])
    parser.add_argument("--runs", type=int, default=5, help="measured rounds after the warm-up")
    parser.add_argument("--root", default=ROOT, help="HDFS directory of the benchmark tables")
    parser.add_argument("--landing", default=LANDING, help="generator landing read by prepare")
    parser.add_argument("--out", type=Path, help="report directory (default build/perf/<exp>)")
    parser.add_argument("--first", type=date.fromisoformat, default=date(2025, 12, 1))
    parser.add_argument("--last", type=date.fromisoformat, default=date(2025, 12, 31))
    parser.add_argument("--salts", type=int, default=16)
    parser.add_argument("--skew-factor", type=float, default=2.0)
    parser.add_argument("--skew-threshold", default="16m")
    parser.add_argument("--advisory-size", default="8m")
    parser.add_argument("--partitions", type=int, nargs="+", default=[4, 8, 16, 64, 200])
    parser.add_argument("--label", default="", help="suffix of the default --out, e.g. 4cores")
    args = parser.parse_args(argv)
    name = args.experiment + (f"-{args.label}" if args.label else "")
    out = args.out = args.out or Path("build") / "perf" / name
    out.mkdir(parents=True, exist_ok=True)

    spark = build_spark(load_config("cluster"), f"gtdp-perf-{name}")
    app_id = spark.sparkContext.applicationId
    settings = {
        key: spark.conf.get(key, None)
        for key in (
            "spark.executor.instances",
            "spark.executor.cores",
            "spark.executor.memory",
            "spark.sql.shuffle.partitions",
            "spark.sql.adaptive.enabled",
        )
    }
    header = {"experiment": name, "app_id": app_id, "settings": settings, "args": vars(args)}
    if args.experiment in UNTIMED:
        result = UNTIMED[args.experiment](spark, args)
        spark.stop()
        (out / "result.json").write_text(json.dumps(header | result, indent=2, default=str))
        print(json.dumps(result, indent=2, default=str))
        return 0

    variants = TIMED[args.experiment](spark, args)
    runs = timed(spark, name, variants, args.runs)
    spark.stop()
    measure(app_id, runs, {v.name: v.metric for v in variants})
    rows = summarize(runs)
    report = markdown(rows)
    payload = header | {"summary": rows, "runs": [asdict(r) for r in runs]}
    if args.experiment == "formats":
        payload["stored"] = {lay.name: du(f"{args.root}/{lay.name}") for lay in ds.LAYOUTS}
        report += "\n\n" + stored_markdown(payload["stored"])
    (out / "runs.json").write_text(json.dumps(payload, indent=2, default=str))
    (out / "runs.csv").write_text(runs_csv(runs))
    (out / "report.md").write_text(f"`{app_id}`, {settings}\n\n{report}\n")
    print(report)
    families = {v.name: v.family for v in variants}
    results: dict[str, set[str]] = {}
    for row in rows:
        results.setdefault(families[row["variant"]], set()).update(row["checks"])
    differ = sorted(family for family, checks in results.items() if len(checks) > 1)
    for family in differ:
        print(f"CHECK FAILED: different results in {family!r}: {results[family]}", file=sys.stderr)
    return 1 if differ else 0


if __name__ == "__main__":
    sys.exit(main())
