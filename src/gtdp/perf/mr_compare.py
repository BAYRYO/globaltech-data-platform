"""Lot 6 comparison tool: runs the MapReduce and the Spark job N times each and reports measures.

    python3 -m gtdp.perf.mr_compare --date YYYY-MM-DD [--landing URI] [--runs N] [--out DIR]
                                    [--no-wait]

It runs inside the cluster image (scripts/mapreduce/compare.sh): it calls run_mr.sh and submit.sh
in turn, reads both outputs with ``hdfs dfs -cat``, and takes the measures from the REST APIs of
the ResourceManager (elapsed time, memory and vcore seconds), the MapReduce JobHistory server
(counters, input directories) and the Spark History Server (stage input and shuffle metrics).

A run starts only when YARN has no other application to run, and counts as exclusive only if no
other application overlapped it; ``--no-wait`` skips the wait (correctness runs) and the report
flags the runs that were not exclusive. Every run of both engines must give the revenue of the
first MapReduce run to the cent and the same counters, and the Quality counters must equal the
anomalies of the drop manifests. The tool writes ``report.md``, ``runs.json`` and the logs of
each run to ``--out``, and exits with 1 when a check fails.
"""

from __future__ import annotations

import argparse
import ast
import io
import json
import posixpath
import statistics
import subprocess
import sys
import time
import tokenize
import urllib.request
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from gtdp.config import load_config

REPO = Path(__file__).resolve().parents[3]
RM = "http://resourcemanager:8088/ws/v1/cluster"
JHS = "http://mr-history:19888/ws/v1/history/mapreduce/jobs"
SHS = "http://spark-history:18080/api/v1/applications"
ACTIVE_STATES = "NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING"
EXCLUSIVE_ATTEMPTS = 3
"""Runs of an engine before keeping one that another application overlapped."""
QUALITY_CODES = (
    "MALFORMED_ROW",
    "BAD_QUANTITY",
    "BAD_PRICE",
    "BAD_TIMESTAMP",
    "NULL_CUSTOMER",
    "UNKNOWN_CUSTOMER",
    "BAD_ENUM",
    "BAD_DISCOUNT",
)
CODE_FILES = {
    "mapreduce": ("mapreduce/mapper.py", "mapreduce/reducer.py", "scripts/mapreduce/run_mr.sh"),
    "spark": ("src/gtdp/perf/revenue_by_country.py", "src/gtdp/jobs/revenue_by_country.py"),
}
TASK = "org.apache.hadoop.mapreduce.TaskCounter"
FILESYSTEM = "org.apache.hadoop.mapreduce.FileSystemCounter"
JOB = "org.apache.hadoop.mapreduce.JobCounter"


@dataclass(frozen=True)
class Result:
    """What a run computed: country -> (revenue_eur, completed lines), and its counters."""

    revenue: dict[str, tuple[Decimal, int]]
    counters: dict[str, int]


@dataclass
class Measure:
    """One run of one engine.

    ``elapsed_s`` is the YARN application's elapsed time, ``jobs_s`` the part of it spent in jobs
    (MapReduce job start to finish, first Spark job submitted to last job completed), ``cpu_s``
    the JVM CPU time of the tasks, which misses the Python processes of Hadoop Streaming.
    ``inputs`` are the order and customer directories read.
    """

    engine: str
    run: int
    app_id: str
    elapsed_s: float
    jobs_s: float
    cpu_s: float
    memory_mb_s: int
    vcore_s: int
    lines_read: int
    bytes_read: int
    shuffle_bytes: int
    tasks: int
    inputs: list[str]
    result: Result
    details: dict[str, int] = field(default_factory=dict)
    idle_check: str = ""
    overlapping: list[str] = field(default_factory=list)


def parse_revenue(text: str) -> dict[str, tuple[Decimal, int]]:
    """``country<TAB>revenue_eur<TAB>lines`` lines, the output format of both engines."""
    revenue = {}
    for line in text.splitlines():
        country, amount, lines = line.split("\t")
        revenue[country] = (Decimal(amount), int(lines))
    return revenue


def mr_counters(payload: dict) -> dict[str, dict[str, int]]:
    """``{group: {name: total}}`` from the JobHistory ``/counters`` answer."""
    return {
        group["counterGroupName"]: {c["name"]: c["totalCounterValue"] for c in group["counter"]}
        for group in payload["jobCounters"]["counterGroup"]
    }


def result_counters(groups: dict[str, dict[str, int]]) -> dict[str, int]:
    """The Quality and Lines counters of a run as ``{"Quality.BAD_PRICE": n, ...}``."""
    return {
        f"{group}.{name}": value
        for group in ("Quality", "Lines")
        for name, value in groups.get(group, {}).items()
    }


def stage_totals(stages: Iterable[dict]) -> dict[str, int]:
    """Sums over the completed stages of the Spark History Server ``/stages`` answer."""
    keys = (
        "inputRecords",
        "inputBytes",
        "shuffleWriteBytes",
        "numCompleteTasks",
        "executorRunTime",
        "executorCpuTime",
        "jvmGcTime",
    )
    totals = Counter()
    for stage in stages:
        if stage.get("status") == "COMPLETE":
            totals.update({key: stage.get(key, 0) for key in keys})
    return {key: totals[key] for key in keys}


def jobs_window_s(jobs: Iterable[dict]) -> float:
    """Seconds from the first submission to the last completion of the Spark ``/jobs`` answer."""
    times = [
        datetime.strptime(job[key], "%Y-%m-%dT%H:%M:%S.%f%Z")
        for job in jobs
        for key in ("submissionTime", "completionTime")
    ]
    return (max(times) - min(times)).total_seconds()


def manifest_quality(manifests: Iterable[dict]) -> dict[str, int]:
    """The Quality counters the drop manifests predict: their orders anomalies of QUALITY_CODES."""
    expected = Counter()
    for manifest in manifests:
        for entry in manifest["files"]:
            if entry["source"] == "orders":
                expected.update(
                    {code: n for code, n in entry["anomalies"].items() if code in QUALITY_CODES}
                )
    return {f"Quality.{code}": n for code, n in sorted(expected.items())}


def differences(reference: Result, other: Result) -> list[str]:
    """Each country or counter where ``other`` differs from ``reference``, both values shown."""
    found = []
    for name, ref, got in (
        ("revenue", reference.revenue, other.revenue),
        ("counter", reference.counters, other.counters),
    ):
        for key in sorted(ref.keys() | got.keys()):
            if ref.get(key) != got.get(key):
                found.append(f"{name} {key}: {ref.get(key)} != {got.get(key)}")
    return found


def overlapping(apps: Iterable[dict], app_id: str, started: int, finished: int) -> list[str]:
    """The applications other than ``app_id`` that ran during [started, finished] (ms)."""
    return sorted(
        app["id"]
        for app in apps
        if app["id"] != app_id
        and app["startedTime"] < finished
        and (app["finishedTime"] == 0 or app["finishedTime"] > started)
    )


def spread(values: Sequence[float]) -> tuple[float, float, float, float]:
    """Median, min, max and (max - min) / median."""
    median = statistics.median(values)
    return median, min(values), max(values), (max(values) - min(values)) / median


def code_lines(path: Path) -> tuple[int, int]:
    """(lines, code lines): code lines leave out blank lines, comments and docstrings."""
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    if path.suffix != ".py":
        return len(lines), sum(
            1 for line in lines if line.strip() and not line.lstrip().startswith("#")
        )
    docstrings = set()
    for node in ast.walk(ast.parse(text)):
        body = getattr(node, "body", None)
        if isinstance(body, list) and body and isinstance(body[0], ast.Expr):
            value = body[0].value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                docstrings.update(range(body[0].lineno, body[0].end_lineno + 1))
    code = set()
    skipped = {tokenize.COMMENT, tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT}
    for token in tokenize.generate_tokens(io.StringIO(text).readline):
        if token.type not in skipped and token.type != tokenize.ENDMARKER:
            code.update(range(token.start[0], token.end[0] + 1))
    return len(lines), len(code - docstrings)


class Cluster:
    """Commands and REST calls of the cluster, with the logs of each run under ``out``."""

    def __init__(self, out: Path) -> None:
        self.out = out

    def get(self, url: str) -> dict:
        with urllib.request.urlopen(url, timeout=30) as response:
            return json.load(response)

    def poll(self, url: str, ready, timeout_s: int = 120) -> dict:
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                payload = self.get(url)
                if ready(payload):
                    return payload
            except OSError:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(f"{url} not ready after {timeout_s} s")
            time.sleep(3)

    def run(self, log_name: str, *command: str) -> str:
        """Run ``command`` from the repository root; return its stdout, keep both streams."""
        done = subprocess.run(command, cwd=REPO, capture_output=True, text=True, check=False)
        (self.out / f"{log_name}.log").write_text(done.stdout + done.stderr, encoding="utf-8")
        if done.returncode != 0:
            sys.stderr.write(done.stderr[-3000:])
            raise RuntimeError(f"{' '.join(command)} exited with {done.returncode}")
        return done.stdout

    def hdfs_cat(self, path: str) -> str:
        return self.run_quiet("hdfs", "dfs", "-cat", path)

    def hdfs_ls(self, path: str) -> list[str]:
        done = subprocess.run(
            ["hdfs", "dfs", "-ls", "-C", path], capture_output=True, text=True, check=False
        )
        return done.stdout.split() if done.returncode == 0 else []

    def hdfs_bytes(self, path: str) -> int:
        """Logical size of ``path`` (``hdfs dfs -du -s``, before replication)."""
        return int(self.run_quiet("hdfs", "dfs", "-du", "-s", path).split()[0])

    def run_quiet(self, *command: str) -> str:
        return subprocess.run(command, capture_output=True, text=True, check=True).stdout

    def wait_idle(self, wait: bool, max_wait_s: int) -> str:
        """Wait until YARN runs no application; return the evidence of the last check."""
        deadline = time.monotonic() + max_wait_s
        while True:
            apps = (self.get(f"{RM}/apps?states={ACTIVE_STATES}").get("apps") or {}).get("app", [])
            stamp = datetime.now(UTC).isoformat(timespec="seconds")
            evidence = f"{stamp} : {len(apps)} application(s) {ACTIVE_STATES.replace(',', '/')}"
            if not apps or not wait:
                return evidence + "".join(f" {app['id']}" for app in apps)
            if time.monotonic() > deadline:
                raise TimeoutError(f"YARN still busy after {max_wait_s} s: {evidence}")
            time.sleep(15)

    def app(self, app_id: str) -> dict:
        return self.get(f"{RM}/apps/{app_id}")["app"]

    def overlaps(self, app: dict) -> list[str]:
        apps = (self.get(f"{RM}/apps").get("apps") or {}).get("app", [])
        return overlapping(apps, app["id"], app["startedTime"], app["finishedTime"])


def run_mapreduce(cluster: Cluster, run: int, landing: str, day: date, output: str) -> Measure:
    stdout = cluster.run(
        f"mapreduce-{run}", "bash", "scripts/mapreduce/run_mr.sh", landing, day.isoformat(), output
    )
    *result_lines, last = stdout.strip().splitlines()
    job_id = last.removeprefix("job_id=")
    job = cluster.poll(f"{JHS}/{job_id}", lambda p: p["job"]["state"] == "SUCCEEDED")["job"]
    groups = mr_counters(cluster.get(f"{JHS}/{job_id}/counters"))
    conf = {p["name"]: p["value"] for p in cluster.get(f"{JHS}/{job_id}/conf")["conf"]["property"]}
    customer_files = [f for f in conf["tmpfiles"].split(",") if not f.startswith("file:")]
    app = cluster.app(job_id.replace("job_", "application_", 1))
    task, launched = groups[TASK], groups[JOB]
    return Measure(
        engine="mapreduce",
        run=run,
        app_id=app["id"],
        elapsed_s=app["elapsedTime"] / 1000,
        jobs_s=(job["finishTime"] - job["startTime"]) / 1000,
        cpu_s=task["CPU_MILLISECONDS"] / 1000,
        memory_mb_s=app["memorySeconds"],
        vcore_s=app["vcoreSeconds"],
        lines_read=task["MAP_INPUT_RECORDS"],
        bytes_read=groups[FILESYSTEM]["HDFS_BYTES_READ"],
        shuffle_bytes=task["REDUCE_SHUFFLE_BYTES"],
        tasks=launched["TOTAL_LAUNCHED_MAPS"] + launched["TOTAL_LAUNCHED_REDUCES"],
        inputs=sorted(
            conf["mapreduce.input.fileinputformat.inputdir"].split(",")
            + [posixpath.dirname(f) for f in customer_files]
        ),
        result=Result(parse_revenue("\n".join(result_lines)), result_counters(groups)),
        details={
            "killed_map_attempts": launched.get("NUM_KILLED_MAPS", 0),
            "map_output_records": task["MAP_OUTPUT_RECORDS"],
            "combine_output_records": task["COMBINE_OUTPUT_RECORDS"],
            "spilled_records": task["SPILLED_RECORDS"],
            "gc_ms": task["GC_TIME_MILLIS"],
            "map_ms": launched["MILLIS_MAPS"],
            "reduce_ms": launched["MILLIS_REDUCES"],
        },
    )


def run_spark(cluster: Cluster, run: int, landing: str, day: date) -> Measure:
    stdout = cluster.run(
        f"spark-{run}",
        "bash",
        "scripts/cluster/submit.sh",
        "revenue-by-country-spark",
        "--date",
        day.isoformat(),
        "--set",
        f"paths.landing={landing}",
    )
    records = [json.loads(line) for line in stdout.splitlines() if line.startswith("{")]
    done = next(r for r in records if r["message"] == "revenue by country written")
    app_id, output = done["yarn_app_id"], done["output"]
    cluster.poll(f"{SHS}/{app_id}", lambda p: p["attempts"][0]["completed"])
    stages = stage_totals(cluster.get(f"{SHS}/{app_id}/stages"))
    jobs = cluster.get(f"{SHS}/{app_id}/jobs")
    counters = {}
    for line in cluster.hdfs_cat(f"{output}/counters/part-*").splitlines():
        group, name, value = line.split("\t")
        counters[f"{group}.{name}"] = int(value)
    app = cluster.app(app_id)
    return Measure(
        engine="spark",
        run=run,
        app_id=app_id,
        elapsed_s=app["elapsedTime"] / 1000,
        jobs_s=jobs_window_s(jobs),
        cpu_s=stages["executorCpuTime"] / 1e9,
        memory_mb_s=app["memorySeconds"],
        vcore_s=app["vcoreSeconds"],
        lines_read=stages["inputRecords"],
        bytes_read=stages["inputBytes"],
        shuffle_bytes=stages["shuffleWriteBytes"],
        tasks=stages["numCompleteTasks"],
        inputs=sorted(
            {posixpath.dirname(path) for path in done["order_files"]} | set(done["customer_dirs"])
        ),
        result=Result(parse_revenue(cluster.hdfs_cat(f"{output}/revenue/part-*")), counters),
        details={
            "spark_jobs": len(jobs),
            "executor_run_ms": stages["executorRunTime"],
            "gc_ms": stages["jvmGcTime"],
        },
    )


def checks(measures: list[Measure], expected_quality: dict[str, int] | None) -> list[str]:
    """Every failed check, as a sentence; an empty list means the engines agree."""
    reference = measures[0]
    order_lines = next(m.lines_read for m in measures if m.engine == "mapreduce")
    failures = []
    for measure in measures:
        label = f"{measure.engine} #{measure.run}"
        failures += [f"{label}: {d}" for d in differences(reference.result, measure.result)]
        if measure.inputs != reference.inputs:
            failures.append(f"{label} read {measure.inputs}, not {reference.inputs}")
        if sum(measure.result.counters.values()) != order_lines:
            failures.append(f"{label}: the counters do not add up to the lines read")
    if expected_quality is not None:
        quality = {k: v for k, v in reference.result.counters.items() if k.startswith("Quality.")}
        if quality != expected_quality:
            failures.append(f"Quality counters {quality} != manifests {expected_quality}")
    return failures


def report(
    measures: list[Measure], failures: list[str], expected_quality: dict | None, context: str
) -> str:
    rows = [
        "| Moteur | Exécution | Application | Durée (s) | Dont jobs (s) | CPU JVM des tâches (s) "
        "| Mo·s | vcore·s | Lignes lues | Octets lus | Octets de shuffle | Tâches | Exclusive "
        "| Contrôle YARN avant l'exécution |",
        "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- "
        "| --- |",
    ]
    for m in measures:
        exclusive = "oui" if not m.overlapping else "non : " + ", ".join(m.overlapping)
        rows.append(
            f"| {m.engine} | {m.run} | `{m.app_id}` | {m.elapsed_s:.1f} | {m.jobs_s:.1f} "
            f"| {m.cpu_s:.1f} | {m.memory_mb_s} | {m.vcore_s} | {m.lines_read} | {m.bytes_read} "
            f"| {m.shuffle_bytes} | {m.tasks} | {exclusive} | {m.idle_check} |"
        )
    summary = [
        "| Moteur | Durée médiane (s) | Min | Max | Écart (max - min) / médiane | Mo·s médian "
        "| vcore·s médian |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for engine in CODE_FILES:
        own = [m for m in measures if m.engine == engine]
        median, low, high, rel = spread([m.elapsed_s for m in own])
        memory = statistics.median(m.memory_mb_s for m in own)
        vcores = statistics.median(m.vcore_s for m in own)
        summary.append(
            f"| {engine} | {median:.1f} | {low:.1f} | {high:.1f} | {rel:.0%} | {memory:.0f} "
            f"| {vcores:.0f} |"
        )
    code = ["| Moteur | Fichier | Lignes | Lignes de code |", "| --- | --- | ---: | ---: |"]
    for engine, paths in CODE_FILES.items():
        for path in paths:
            total, logic = code_lines(REPO / path)
            code.append(f"| {engine} | `{path}` | {total} | {logic} |")
    reference = measures[0].result
    revenue = ["| Pays | CA facturé (EUR) | Lignes COMPLETED |", "| --- | ---: | ---: |"]
    revenue += [f"| {c} | {a} | {n} |" for c, (a, n) in sorted(reference.revenue.items())]
    counters = ["| Compteur | Valeur |", "| --- | ---: |"]
    counters += [f"| {k} | {v} |" for k, v in sorted(reference.counters.items())]
    verdict = (
        [
            "Toutes les exécutions des deux moteurs donnent le même CA au centime et les mêmes "
            "compteurs ; les compteurs couvrent toutes les lignes lues."
        ]
        if not failures
        else ["Écarts :", "", *[f"- {f}" for f in failures]]
    )
    if expected_quality is not None and not failures:
        verdict.append("Les compteurs Quality sont égaux aux anomalies des manifestes.")
    return "\n".join(
        [
            "# MapReduce contre Spark : mesures",
            "",
            context,
            "",
            "## Exécutions",
            "",
            *rows,
            "",
            "## Durée",
            "",
            *summary,
            "",
            "## Résultat de référence",
            "",
            *revenue,
            "",
            *counters,
            "",
            "## Vérifications",
            "",
            *verdict,
            "",
            "## Taille du code",
            "",
            *code,
            "",
        ]
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mr_compare", description=__doc__.split("\n\n")[0])
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--landing", help="landing root (default: paths.landing of --env cluster)")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--out", type=Path, default=REPO / "build" / "mr-vs-spark")
    parser.add_argument("--no-wait", action="store_true", help="do not wait for an idle YARN")
    parser.add_argument("--max-wait", type=int, default=3600, help="seconds, per run")
    args = parser.parse_args(argv)
    cfg = load_config("cluster")
    landing = (args.landing or cfg.paths.landing).rstrip("/")
    mr_output = f"{cfg.paths.ops}/mr_vs_spark/mapreduce"
    args.out.mkdir(parents=True, exist_ok=True)
    cluster = Cluster(args.out)

    measures: list[Measure] = []
    for run in range(1, args.runs + 1):
        for engine in CODE_FILES:
            for _attempt in range(EXCLUSIVE_ATTEMPTS):
                evidence = cluster.wait_idle(not args.no_wait, args.max_wait)
                if engine == "mapreduce":
                    measure = run_mapreduce(cluster, run, landing, args.date, mr_output)
                else:
                    measure = run_spark(cluster, run, landing, args.date)
                measure.idle_check = evidence
                measure.overlapping = cluster.overlaps(cluster.app(measure.app_id))
                print(f"{engine} #{run}: {measure.app_id} {measure.elapsed_s:.1f} s", flush=True)
                if args.no_wait or not measure.overlapping:
                    break
                print(f"  discarded: {', '.join(measure.overlapping)} ran at the same time")
            measures.append(measure)
            (args.out / "runs.json").write_text(
                json.dumps([asdict(m) for m in measures], default=str, indent=2), "utf-8"
            )

    manifests = [
        json.loads(cluster.hdfs_cat(f"{path}/_MANIFEST.json"))
        for path in cluster.hdfs_ls(f"{landing}/_manifests")
        if path.rsplit("=", 1)[-1] <= args.date.isoformat()
    ]
    expected = manifest_quality(manifests) if manifests else None
    failures = checks(measures, expected)
    sizes = "\n".join(
        f"- `{path.removeprefix(landing)}` : {cluster.hdfs_bytes(path)} octets"
        for path in measures[0].inputs
    )
    context = (
        f"Entrée : `{landing}`, partitions `ingestion_date` jusqu'au {args.date} :\n\n{sizes}\n\n"
        f"{args.runs} exécutions par moteur en alternance, le {datetime.now(UTC):%Y-%m-%d %H:%M} "
        f"UTC, par `scripts/mapreduce/compare.sh` au commit `{cfg.git_sha}`."
    )
    (args.out / "report.md").write_text(report(measures, failures, expected, context), "utf-8")
    print(f"report: {args.out / 'report.md'}")
    for failure in failures:
        print(f"FAILED {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
