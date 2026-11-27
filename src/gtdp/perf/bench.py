"""Lot 7 benchmarks (KAN-44 to KAN-46): timed Spark runs, measured from the History Server.

    scripts/perf/bench.sh <experiment|command> [--profile P] [--runs N] [--out DIR] [options]

The driver of this application runs the experiment, then reads its own stages from the Spark
History Server once the application has stopped. Experiments:

    prepare      write the benchmark tables of every layout from a generator landing
    formats      KAN-44: the revenue query on CSV, Parquet, partitioned Parquet, snappy and zstd
    skew         KAN-45: the gold interval join, skewed, salted, with AQE skew join, broadcast
    cache        KAN-46: indicators over the priced orders, with and without cache
    partitions   KAN-46: a wide aggregation for several spark.sql.shuffle.partitions
    explain      KAN-46: three explain("formatted") plans of the dev lake
    record-hash  the bronze _record_hash column: its share of the files, hex against binary

Commands, which start no Spark application:

    land         generate a profile and put the sources that prepare reads under --landing
    report       rebuild report.md from the runs.json of --out
    remeasure    read the stages of the runs.json of --out again from the History Server

A timed experiment runs ``--warmup`` rounds (numbered up to 0, left out of the statistics), then
``--runs`` rounds numbered from 1, each running every variant once in turn. A variant sets its
SQL settings for its own run only. Before each run the driver waits until YARN runs no
application besides its own and records the check; afterwards it lists any application that
overlapped the run, and the report leaves that run out. It tags the Spark jobs of each run with a
job group, from which the History Server gives the stages, the bytes they read, the task
quantiles of the run's key stage and how long its scan and key stages took. ``runs.json`` holds
every run at full precision; ``report.md`` gives the medians and, for each comparison the
experiment declares, the change and the exact two-sided Mann-Whitney p, with and without the
first measured run. Folders default to the ``[perf]`` section of the configuration.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import subprocess
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, fields
from datetime import UTC, date, datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gtdp.config import AppConfig, ConfigError, PerfSettings, load_config
from gtdp.datagen.__main__ import main as datagen
from gtdp.gold import facts
from gtdp.gold.tables import DIM_PRODUCT, FACT_SALES, ORDER_DATE, table_path
from gtdp.io import read_table
from gtdp.perf import datasets as ds
from gtdp.perf.history import (
    QUANTILES,
    Skew,
    completed_stages,
    group_stage_ids,
    key_stage,
    missing_stages,
    skew,
    stage_wall_s,
    totals,
)
from gtdp.perf.mr_compare import ACTIVE_STATES, RM, SHS, Cluster, mann_whitney, overlapping
from gtdp.perf.skew import SALT_COLUMNS, salted_applicable_price
from gtdp.schemas import RECORD_HASH, SOURCES, bronze_schema
from gtdp.silver.pipelines import PIPELINES
from gtdp.silver.pipelines import table_path as silver_path
from gtdp.silver.products import with_applicable_price
from gtdp.spark import build_spark

NO_BROADCAST = {"spark.sql.autoBroadcastJoinThreshold": "-1"}
AQE_OFF = {"spark.sql.adaptive.enabled": "false"}
RETAINED = 0.05
"""A difference is retained when the exact two-sided Mann-Whitney p is below this."""


@dataclass(frozen=True)
class Variant:
    """One way to run the work: ``action`` runs it and returns a check of its result, equal for
    every run of every variant of an experiment, and details to keep, such as storage figures.

    ``conf`` holds SQL settings applied for the run only. ``metric`` picks the key stage, whose
    task quantiles the report gives: the largest ``shuffleReadBytes`` (the stage after the
    shuffle) or the largest ``inputBytes`` (the scan). Variants of one ``family`` compute the
    same result. The report compares the variant with each of its ``references``."""

    name: str
    action: Callable[[SparkSession], tuple[object, dict]]
    conf: Mapping[str, str] = field(default_factory=dict)
    metric: str = "shuffleReadBytes"
    family: str = ""
    references: tuple[str, ...] = ()


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
    stage_walls: dict = field(default_factory=dict)


def timed(
    spark: SparkSession,
    experiment: str,
    variants: Sequence[Variant],
    runs: int,
    warmup: int,
    cluster: Cluster,
) -> list[Run]:
    """Run every variant once per round: ``warmup`` rounds numbered up to 0, then ``runs``
    rounds numbered from 1."""
    sc, measured = spark.sparkContext, []
    for run in range(1 - warmup, runs + 1):
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
            return f"{stamp}: {len(others)} other application {ACTIVE_STATES.replace(',', '/')}"
        if time.monotonic() > deadline:
            raise TimeoutError(f"YARN still runs {others} after {max_wait_s} s")
        time.sleep(15)


def measure(app_id: str, runs: list[Run], metrics: Mapping[str, str], cluster: Cluster) -> None:
    """Fill each run's stage totals, key stage, stage times and overlaps from the History Server
    and the RM. Fail when the History Server no longer holds every job and stage of a run."""
    cluster.poll(f"{SHS}/{app_id}", lambda p: p["attempts"][0]["completed"], timeout_s=600)
    jobs = cluster.get(f"{SHS}/{app_id}/jobs")
    stages = cluster.get(f"{SHS}/{app_id}/stages")
    if jobs and (first := min(job["jobId"] for job in jobs)) > 0:
        raise LookupError(f"the History Server dropped jobs 0 to {first - 1} of {app_id}")
    apps = (cluster.get(f"{RM}/apps").get("apps") or {}).get("app", [])
    quantiles = ",".join(str(q) for q in QUANTILES)
    for run in runs:
        ids = group_stage_ids(jobs, run.group)
        if missing := missing_stages(stages, ids):
            raise LookupError(f"the History Server dropped stages {sorted(missing)} of {run.group}")
        own = completed_stages(stages, ids)
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
            "attempt": attempt,
            "spilled_bytes": stage.get("diskBytesSpilled", 0),
        }
        run.stage_walls = {
            "scan": stage_wall_s(key_stage(own, "inputBytes")),
            "key": stage_wall_s(stage),
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
KEY_STAGE_COLUMNS = (
    "stage",
    "attempt",
    "tasks",
    "duration_median_ms",
    "duration_max_ms",
    "read_median_bytes",
    "read_max_bytes",
    "spilled_bytes",
)
RUN_COLUMNS = (
    "variant",
    "run",
    "started_ms",
    "wall_s",
    "yarn_check",
    "check",
    "overlapping",
    *(f"stages.{key}" for key in STAGE_COLUMNS),
    *(f"key_stage.{key}" for key in KEY_STAGE_COLUMNS),
    "stage_walls.scan",
    "stage_walls.key",
    "details",
)
"""The columns of a run in ``runs.json``; ``a.b`` is the key ``b`` of the run's dict ``a``."""


def save_runs(path: Path, header: Mapping, runs: Sequence[Run]) -> None:
    """``runs.json``: the experiment's header, then one line per run, warm-up included, its
    values in the order of ``RUN_COLUMNS``, durations at full precision."""

    def value(run: Run, column: str) -> object:
        name, _, key = column.partition(".")
        return getattr(run, name).get(key) if key else getattr(run, name)

    def compact(value: object) -> str:
        return json.dumps(value, default=str, ensure_ascii=False, separators=(",", ":"))

    lines = [compact([value(r, c) for c in RUN_COLUMNS]) for r in runs]
    head = compact({**header, "columns": RUN_COLUMNS})
    path.write_text(head[:-1] + ',"runs":[\n' + ",\n".join(lines) + "\n]}\n", encoding="utf-8")


def load_runs(path: Path) -> tuple[dict, list[Run]]:
    """The header and the runs of a ``runs.json`` written by ``save_runs``."""
    header = json.loads(path.read_text(encoding="utf-8"))
    columns, rows = header.pop("columns"), header.pop("runs")
    runs = []
    for row in rows:
        fields: dict = {"stages": {}, "key_stage": {}, "stage_walls": {}}
        for column, value in zip(columns, row, strict=True):
            name, _, key = column.partition(".")
            if not key:
                fields[name] = value
            elif value is not None:
                fields[name][key] = value
        group = f"{header['experiment']}/{fields['variant']}/{fields['run']}"
        runs.append(Run(group=group, **fields))
    return header, runs


def _median(values: Sequence[float]) -> float:
    return statistics.median(values) if values else 0.0


def _skew(run: Run) -> Skew | None:
    """The key-stage quantiles of a run, None before ``measure``."""
    if not all(f.name in run.key_stage for f in fields(Skew)):
        return None
    return Skew(**{f.name: run.key_stage[f.name] for f in fields(Skew)})


def kept(runs: Sequence[Run], variant: str, first_run: int = 1) -> list[Run]:
    """The measured runs of ``variant`` from run ``first_run`` on that no other application
    overlapped: the runs the statistics use."""
    return [r for r in runs if r.variant == variant and r.run >= first_run and not r.overlapping]


def summarize(runs: Sequence[Run]) -> list[dict]:
    """Per variant, in order of first run, over its kept runs: run count, median, min and max
    wall time, medians of the stage totals, the key-stage ratios and the stage times, and the
    key stage of the median run; the distinct result checks of every run, warm-up included; and
    the measured runs that another application overlapped, left out."""
    rows = []
    for name in dict.fromkeys(r.variant for r in runs):
        own = kept(runs, name)
        walls = [r.wall_s for r in own]
        middle = sorted(own, key=lambda r: r.wall_s)[(len(own) - 1) // 2] if own else None
        skews = [s for s in map(_skew, own) if s is not None]
        rows.append(
            {
                "variant": name,
                "n": len(own),
                "median_s": _median(walls),
                "min_s": min(walls, default=0.0),
                "max_s": max(walls, default=0.0),
                **{key: _median([r.stages.get(key, 0) for r in own]) for key in STAGE_COLUMNS},
                "duration_ratio": _median([s.duration_ratio for s in skews]),
                "read_ratio": _median([s.read_ratio for s in skews]),
                **{
                    f"{stage}_s": _median([r.stage_walls[stage] for r in own])
                    if all(stage in r.stage_walls for r in own) and own
                    else None
                    for stage in ("scan", "key")
                },
                "key_stage": middle.key_stage if middle else {},
                "checks": sorted(
                    {json.dumps(r.check, default=str) for r in runs if r.variant == name}
                ),
                "overlapped": [
                    f"{r.run} ({', '.join(r.overlapping)})"
                    for r in runs
                    if r.variant == name and r.run > 0 and r.overlapping
                ],
            }
        )
    return rows


def change(before: float, after: float) -> float:
    """The change from ``before`` to ``after`` in percent: -25.0 means a quarter less."""
    return 100 * (after - before) / before if before else 0.0


def compare(
    runs: Sequence[Run], pairs: Sequence[tuple[str, str]], first_run: int = 1
) -> list[dict]:
    """For each ``(reference, variant)`` pair, over their kept runs from ``first_run``: the
    medians, the change of the median time and bytes read, and the exact Mann-Whitney test of
    the wall times (``u`` counts the pairs where the variant is slower), and of the scan and key
    stage times when every run has them."""
    rows = []
    for reference, variant in pairs:
        base, other = kept(runs, reference, first_run), kept(runs, variant, first_run)
        u, _, p = mann_whitney([r.wall_s for r in other], [r.wall_s for r in base])
        row = {
            "reference": reference,
            "variant": variant,
            "n": (len(other), len(base)),
            "medians_s": (_median([r.wall_s for r in base]), _median([r.wall_s for r in other])),
            "change_pct": change(
                _median([r.wall_s for r in base]), _median([r.wall_s for r in other])
            ),
            "read_change_pct": change(
                _median([r.stages.get("inputBytes", 0) for r in base]),
                _median([r.stages.get("inputBytes", 0) for r in other]),
            ),
            "u": u,
            "p": p,
        }
        for stage in ("scan", "key"):
            a = [r.stage_walls.get(stage) for r in other]
            b = [r.stage_walls.get(stage) for r in base]
            whole = a and b and None not in a and None not in b
            row[f"{stage}_p"] = mann_whitney(a, b)[2] if whole else None
        rows.append(row)
    return rows


def comparisons(header: Mapping) -> list[tuple[str, str]]:
    """The ``(reference, variant)`` pairs that the variants of a ``runs.json`` header declare."""
    return [(ref, v["name"]) for v in header["variants"] for ref in v["references"]]


def _cell(value: float | None, fmt: str = ".2f") -> str:
    return "—" if value is None else format(value, fmt)


def markdown(rows: Sequence[Mapping], stage_url: Callable[[int, int], str] | None = None) -> str:
    """The table of ``summarize`` rows, then the runs left out because another application
    overlapped them."""
    head = (
        "| Variante | n | Durée médiane (s) | Min à max (s) | Octets lus | Shuffle lu "
        "| Déversé disque | Tâches | Étape de lecture (s) | Étape clé (s) "
        "| Étape clé : max/médiane durée | max/médiane octets | Étape clé, exécution médiane |\n"
        "| --- |" + " ---: |" * 11 + " --- |"
    )
    lines = []
    for r in rows:
        stage = r["key_stage"]
        link = "—"
        if "stage" in stage:
            link = str(stage["stage"])
            if stage_url:
                link = f"[{stage['stage']}]({stage_url(stage['stage'], stage['attempt'])})"
        lines.append(
            f"| `{r['variant']}` | {r['n']} | {r['median_s']:.2f} | {r['min_s']:.2f} à "
            f"{r['max_s']:.2f} | {r['inputBytes']:,.0f} | {r['shuffleReadBytes']:,.0f} "
            f"| {r['diskBytesSpilled']:,.0f} | {r['numCompleteTasks']:.0f} "
            f"| {_cell(r['scan_s'])} | {_cell(r['key_s'])} "
            f"| {r['duration_ratio']:.2f} | {r['read_ratio']:.2f} | ".replace(",", " ")
            + f"{link} |"
        )
    overlapped = [f"`{r['variant']}` {run}" for r in rows for run in r["overlapped"]]
    left_out = (
        "Exécutions chevauchées par une autre application, exclues des statistiques : "
        + (", ".join(overlapped) if overlapped else "aucune")
        + "."
    )
    return "\n".join([head, *lines, "", left_out])


def comparisons_markdown(
    rows: Sequence[Mapping], with_first: Sequence[Mapping] | None = None
) -> str:
    """The table of ``compare`` rows; given the rows of the same pairs with the first measured
    run (``with_first``), it says for each pair whether the conclusion changes without it."""
    extra = " | Retenu avec l'exécution 1 | Conclusion" if with_first is not None else ""
    head = (
        "| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus "
        f"| U (paires où la variante est plus lente) | p | Retenu | p étape de lecture "
        f"| p étape clé{extra} |\n| --- | --- |" + " ---: |" * 9 + " --- |" * (2 * bool(extra))
    )
    lines = []
    for i, r in enumerate(rows):
        retained = r["p"] < RETAINED
        cells = [
            f"`{r['reference']}`",
            f"`{r['variant']}`",
            f"{r['n'][0]} contre {r['n'][1]}",
            f"{r['medians_s'][0]:.2f} → {r['medians_s'][1]:.2f}",
            f"{r['change_pct']:+.1f} %",
            f"{r['read_change_pct']:+.1f} %",
            f"{r['u']:g} / {r['n'][0] * r['n'][1]}",
            f"{r['p']:.3f}",
            "oui" if retained else "non",
            _cell(r["scan_p"], ".3f"),
            _cell(r["key_p"], ".3f"),
        ]
        if with_first is not None:
            before = with_first[i]["p"] < RETAINED
            cells += ["oui" if before else "non", "inchangée" if before == retained else "change"]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join([head, *lines])


def stored_markdown(stored: Mapping[str, Mapping[str, int]]) -> str:
    """The bytes stored per layout, logical and on the DataNodes, and the changes between them."""
    lines = [
        "| Disposition | Octets logiques | Octets physiques | Écart sur la première "
        "| Écart sur la précédente |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    names = list(stored)
    for previous, name in zip([names[0], *names], names, strict=False):
        logical = stored[name]["logical"]
        lines.append(
            f"| `{name}` | {logical:,} | {stored[name]['physical']:,} "
            f"| {change(stored[names[0]]['logical'], logical):+.1f} % "
            f"| {change(stored[previous]['logical'], logical):+.1f} % |".replace(",", " ")
        )
    return "\n".join(lines)


def report(header: Mapping, runs: Sequence[Run], history_ui: str) -> str:
    """``report.md``: the variants, the stored bytes (formats), the comparisons the experiment
    declares, and the same comparisons without the first measured run."""
    app_id = header["app_id"]

    def stage_url(stage: int, attempt: int) -> str:
        return f"{history_ui}/{app_id}/stages/stage/?id={stage}&attempt={attempt}"

    text = [
        f"`{app_id}`, {header['settings']}, {header['warmup']} tour(s) d'échauffement exclu(s)",
        "",
        markdown(summarize(runs), stage_url),
    ]
    if "stored" in header:
        text += ["", stored_markdown(header["stored"])]
    if pairs := comparisons(header):
        measured = compare(runs, pairs)
        text += [
            "",
            "Comparaisons : test exact de Mann-Whitney sur les durées, bilatéral ; une différence "
            f"est retenue quand p < {RETAINED}.",
            "",
            comparisons_markdown(measured),
            "",
            "Sensibilité à l'échauffement : les mêmes comparaisons sans l'exécution mesurée 1.",
            "",
            comparisons_markdown(compare(runs, pairs, first_run=2), measured),
        ]
    return "\n".join(text) + "\n"


def differing_families(header: Mapping, runs: Sequence[Run]) -> dict[str, set[str]]:
    """The families of the header whose runs, warm-up included, return different checks."""
    families = {v["name"]: v["family"] for v in header["variants"]}
    results: dict[str, set[str]] = {}
    for r in runs:
        results.setdefault(families[r.variant], set()).add(json.dumps(r.check, default=str))
    return {family: checks for family, checks in results.items() if len(checks) > 1}


def write_report(out: Path, header: Mapping, runs: Sequence[Run], history_ui: str) -> int:
    """Write ``report.md`` to ``out``; 1 when two runs of one family returned different checks."""
    text = report(header, runs, history_ui)
    (out / "report.md").write_text(text)
    print(text)
    differ = differing_families(header, runs)
    for family, checks in sorted(differ.items()):
        print(f"CHECK FAILED: different results in {family!r}: {checks}", file=sys.stderr)
    return 1 if differ else 0


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
    by the driver instead of by a Spark job, then listed once before the timed runs. Each layout
    is compared with the previous one, zstd also with CSV, and the two listings with Parquet."""

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
    references = {
        "parquet": ("csv",),
        "parquet_part": ("parquet",),
        "parquet_part_zstd": ("parquet_part", "csv"),
    }
    return [
        *[
            Variant(
                layout.name,
                query(layout),
                metric="inputBytes",
                references=references.get(layout.name, ()),
            )
            for layout in ds.LAYOUTS
        ],
        Variant(
            "parquet_part+driver-listing",
            query(partitioned),
            driver,
            "inputBytes",
            references=("parquet",),
        ),
        Variant(
            "parquet_part+listed",
            query(partitioned, listed=True),
            metric="inputBytes",
            references=("parquet",),
        ),
    ]


def priced_orders(spark: SparkSession, root: str) -> tuple[DataFrame, DataFrame]:
    orders = ds.read(spark, root, ds.LAYOUT["parquet"], "orders")
    return orders, read_table(spark, f"{root}/versions", ds.VERSIONS)


def skew_variants(spark: SparkSession, args: argparse.Namespace) -> list[Variant]:
    """The skewed join, then each correction compared with it."""
    orders, versions = priced_orders(spark, args.root)

    def plain(spark: SparkSession) -> tuple[object, dict]:
        return ds.priced_check(with_applicable_price(orders, versions)), {}

    def salted(spark: SparkSession) -> tuple[object, dict]:
        frame = salted_applicable_price(orders, versions, args.salts, args.salt_on)
        return ds.priced_check(frame), {}

    aqe = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": str(args.skew_factor),
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": args.skew_threshold,
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": args.advisory_size,
    }
    base = ("baseline",)
    return [
        Variant("baseline", plain, AQE_OFF | NO_BROADCAST),
        Variant(f"salted-{args.salts}", salted, AQE_OFF | NO_BROADCAST, references=base),
        Variant("aqe-skew-join", plain, aqe | NO_BROADCAST, references=base),
        Variant("broadcast", plain, AQE_OFF, metric="inputBytes", references=base),
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
            "cached_partitions": info.numCachedPartitions(),
            "partitions": info.numPartitions(),
            "memory_bytes": info.memSize(),
            "disk_bytes": info.diskSize(),
        }
        for info in spark.sparkContext._jsc.sc().getRDDStorageInfo()
    ]


def cache_variants(spark: SparkSession, args: argparse.Namespace) -> list[Variant]:
    """Indicators over the priced orders, the fact cached or recomputed by each indicator: 3 then
    6 indicators with the broadcast join of gold, 3 with the join shuffled (sort-merge). Each
    cached variant is compared with the same indicators uncached."""
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
        variant
        for n in (3, len(INDICATORS))
        for variant in (
            Variant(f"no-cache-{n}", uncached(n), family=str(n)),
            Variant(f"cache-{n}", cached(n), family=str(n), references=(f"no-cache-{n}",)),
        )
    ]
    return [
        *plain,
        Variant("no-cache-3-sort-merge", uncached(3), NO_BROADCAST, family="3"),
        Variant(
            "cache-3-sort-merge",
            cached(3),
            NO_BROADCAST,
            family="3",
            references=("no-cache-3-sort-merge",),
        ),
    ]


def partitions_variants(spark: SparkSession, args: argparse.Namespace) -> list[Variant]:
    """The aggregation for each partition count, coalescing off, then the largest count with
    coalescing; each compared with the platform's count when the sweep holds it."""
    orders = ds.read(spark, args.root, ds.LAYOUT["parquet"], "orders")

    def aggregate(_: SparkSession) -> tuple[object, dict]:
        pairs = orders.groupBy("customer_id", "product_id").agg(
            F.sum(ds.amount()).alias("revenue"), F.count(F.lit(1)).alias("lines")
        )
        row = pairs.agg(F.count(F.lit(1)), F.sum("revenue"), F.sum("lines")).first()
        return [str(value) for value in row], {}

    platform = f"sp-{spark.conf.get('spark.sql.shuffle.partitions')}"
    names = [f"sp-{n}" for n in args.partitions]
    top = max(args.partitions)

    def against(name: str) -> tuple[str, ...]:
        return (platform,) if platform in names and name != platform else ()

    fixed = {"spark.sql.adaptive.coalescePartitions.enabled": "false"}
    variants = [
        Variant(
            name,
            aggregate,
            fixed | {"spark.sql.shuffle.partitions": str(n)},
            references=against(name),
        )
        for n, name in zip(args.partitions, names, strict=True)
    ]
    coalesced = {
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": str(top),
    }
    name = f"sp-{top}-aqe-coalesce"
    return [*variants, Variant(name, aggregate, coalesced, references=against(name))]


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


def land(args: argparse.Namespace) -> list[str]:
    """Generate the profile's J1 to J3 drops with seed ``args.seed`` and put, under
    ``args.landing``, each source that ``prepare`` reads (``ds.LANDED``) and each drop's
    manifest, as ``<source>/ingestion_date=<day>/`` and ``_manifests/ingestion_date=<day>/``
    (the layout of ``scripts/cluster/land.sh``, which ``compare.sh`` also reads). Files already
    there are replaced. Return the folders written."""
    written = []
    with tempfile.TemporaryDirectory() as tmp:
        command = ["generate", "--profile", args.profile, "--seed", str(args.seed), "--out", tmp]
        if datagen(command) != 0:
            raise RuntimeError(f"the generator failed: {command}")
        drops = (Path(tmp) / args.profile / "landing").iterdir()
        for drop in sorted(p for p in drops if p.is_dir()):
            parts = {source: sorted((drop / source).iterdir()) for source in ds.LANDED}
            for source, files in (*parts.items(), ("_manifests", [drop / "_MANIFEST.json"])):
                target = f"{args.landing}/{source}/ingestion_date={drop.name}"
                subprocess.run(["hdfs", "dfs", "-mkdir", "-p", target], check=True)
                subprocess.run(["hdfs", "dfs", "-put", "-f", *map(str, files), target], check=True)
                written.append(target)
    return written


COMMANDS = ("land", "report", "remeasure")


def perf_settings(cfg: AppConfig) -> PerfSettings:
    if cfg.perf is None:
        raise ConfigError("the configuration has no [perf] section")
    return cfg.perf


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bench", description=__doc__.split("\n\n")[0])
    parser.add_argument("experiment", choices=[*TIMED, *UNTIMED, *COMMANDS])
    parser.add_argument("--profile", default="full", help="generator profile (default full)")
    parser.add_argument("--seed", type=int, default=42, help="generator seed of land")
    parser.add_argument("--runs", type=int, default=5, help="measured rounds")
    parser.add_argument("--warmup", type=int, help="warm-up rounds ([perf] warmup_rounds)")
    parser.add_argument("--root", help="benchmark tables ([perf] tables/<profile>)")
    parser.add_argument("--landing", help="generator landing ([perf] landing/<profile>/landing)")
    parser.add_argument("--out", type=Path, help="report directory (default build/perf/<exp>)")
    parser.add_argument("--first", type=date.fromisoformat, default=date(2025, 12, 1))
    parser.add_argument("--last", type=date.fromisoformat, default=date(2025, 12, 31))
    parser.add_argument("--salts", type=int, default=16)
    parser.add_argument("--salt-on", choices=SALT_COLUMNS, default="event_time")
    parser.add_argument("--skew-factor", type=float, default=2.0)
    parser.add_argument("--skew-threshold", default="16m")
    parser.add_argument("--advisory-size", default="8m")
    parser.add_argument("--partitions", type=int, nargs="+", default=[4, 8, 16, 64, 200])
    parser.add_argument("--label", default="", help="suffix of the default --out, e.g. 4cores")
    args = parser.parse_args(argv)
    cfg = load_config("cluster")
    perf = perf_settings(cfg)
    args.root = args.root or f"{perf.tables}/{args.profile}"
    args.landing = args.landing or f"{perf.landing}/{args.profile}/landing"
    args.warmup = perf.warmup_rounds if args.warmup is None else args.warmup
    name = args.experiment + (f"-{args.label}" if args.label else "")
    out = args.out = args.out or Path("build") / "perf" / name

    if args.experiment == "land":
        print("\n".join(land(args)))
        return 0
    if args.experiment in ("report", "remeasure"):
        header, runs = load_runs(out / "runs.json")
        if args.experiment == "remeasure":
            metrics = {v["name"]: v["metric"] for v in header["variants"]}
            measure(header["app_id"], runs, metrics, Cluster(out))
            save_runs(out / "runs.json", header, runs)
        return write_report(out, header, runs, perf.history_ui)
    out.mkdir(parents=True, exist_ok=True)
    return experiment(args, name, cfg, perf)


SETTINGS = (
    "spark.executor.instances",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
)
"""The Spark settings that each report header records."""


def experiment(args: argparse.Namespace, name: str, cfg: AppConfig, perf: PerfSettings) -> int:
    """Run ``args.experiment`` as the Spark application ``gtdp-perf-<name>`` and write its
    reports to ``args.out``."""
    spark = build_spark(cfg, f"gtdp-perf-{name}")
    app_id = spark.sparkContext.applicationId
    settings = {key: spark.conf.get(key, None) for key in SETTINGS}
    header = {"experiment": name, "app_id": app_id, "settings": settings, "args": vars(args)}
    if args.experiment in UNTIMED:
        result = UNTIMED[args.experiment](spark, args)
        spark.stop()
        (args.out / "result.json").write_text(json.dumps(header | result, indent=2, default=str))
        print(json.dumps(result, indent=2, default=str))
        return 0

    variants = TIMED[args.experiment](spark, args)
    cluster = Cluster(args.out)
    runs = timed(spark, name, variants, args.runs, args.warmup, cluster)
    spark.stop()
    measure(app_id, runs, {v.name: v.metric for v in variants}, cluster)
    header |= {
        "warmup": args.warmup,
        "variants": [
            {"name": v.name, "metric": v.metric, "family": v.family, "references": v.references}
            for v in variants
        ],
    }
    if args.experiment == "formats":
        header["stored"] = {lay.name: du(f"{args.root}/{lay.name}") for lay in ds.LAYOUTS}
    save_runs(args.out / "runs.json", header, runs)
    return write_report(args.out, header, runs, perf.history_ui)


if __name__ == "__main__":
    sys.exit(main())
