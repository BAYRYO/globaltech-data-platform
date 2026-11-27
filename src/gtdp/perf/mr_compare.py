"""Lot 6 comparison tool: runs the MapReduce and the Spark job on several volumes and reports.

    python3 -m gtdp.perf.mr_compare --date YYYY-MM-DD [--volumes NAME=LANDING ...] [--runs N]
                                    [--mr-conf KEY=VALUE ...] [--out DIR] [--no-wait]
    python3 -m gtdp.perf.mr_compare --reread NAME=APP_ID,APP_ID... [NAME=...] [--out DIR]

It runs inside the cluster image (scripts/mapreduce/compare.sh). A volume is a landing root with
orders/ and customers/ partitions. On each volume, the tool runs every variant N times, in turn:
the MapReduce job (run_mr.sh), the Spark job (submit.sh) and, with --mr-conf, the MapReduce job
with those -D settings. It reads both outputs with ``hdfs dfs -cat`` and takes the measures from
the REST APIs of the ResourceManager, the MapReduce JobHistory server and the Spark History Server.

A run starts only when YARN has no other application to run, and counts as exclusive only if no
other application overlapped it; ``--no-wait`` skips the wait (correctness runs) and the report
flags the runs that were not exclusive. On each volume, every run must give the output of the
first MapReduce run byte for byte and the same counters, and the Quality counters must equal the
non-zero anomalies of the drop manifests. Per volume, the report gives each variant's median
duration with its range, an exact Mann-Whitney test for each pair of variants, memory and core
seconds, and the fixed and per-record cost of a task. ``--reread`` rebuilds the measures of
finished applications without running anything. The tool writes ``report.md``, ``runs.json`` and
the logs of each run to ``--out``, and exits with 1 when a check fails.
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
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from itertools import combinations
from pathlib import Path

from gtdp.config import load_config

REPO = Path(__file__).resolve().parents[3]
RM = "http://resourcemanager:8088/ws/v1/cluster"
JHS = "http://mr-history:19888/ws/v1/history/mapreduce/jobs"
SHS = "http://spark-history:18080/api/v1/applications"
ACTIVE_STATES = "NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING"
FINAL_STATES = frozenset(("FINISHED", "FAILED", "KILLED"))
EXCLUSIVE_ATTEMPTS = 3
"""Runs of a variant before keeping one that another application overlapped."""
SETTLE_S = 5.0
"""Seconds between two ResourceManager readings of an application that must agree: the RM keeps
adding the resources of its containers until the NodeManagers report them released."""
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
class Volume:
    name: str
    landing: str


@dataclass(frozen=True)
class Variant:
    """One way to run an engine; ``mr_conf`` holds the KEY=VALUE -D settings of a MapReduce job."""

    name: str
    engine: str
    mr_conf: tuple[str, ...] = ()


@dataclass(frozen=True)
class Result:
    """What a run computed: its revenue output, byte for byte, and its counters."""

    output: str
    counters: dict[str, int]

    @property
    def revenue(self) -> dict[str, tuple[Decimal, int]]:
        return parse_revenue(self.output)


@dataclass(frozen=True)
class Task:
    """A task that read orders: records read, and its CPU and run seconds as its engine counts."""

    records: int
    cpu_s: float
    run_s: float


@dataclass(frozen=True)
class TaskCost:
    """Least-squares lines of task CPU and run time against the millions of records read."""

    fixed_cpu_s: float
    cpu_s_per_million: float
    fixed_run_s: float
    run_s_per_million: float


@dataclass
class Measure:
    """One run of one variant on one volume.

    ``elapsed_s`` is the YARN application's elapsed time, ``jobs_s`` the part of it spent in jobs
    (MapReduce job start to finish, first Spark job submitted to last job completed). ``cpu_s`` is
    the CPU time of the tasks as the engine counts it: the JVMs only for MapReduce, whose Python
    processes Hadoop does not count. ``memory_mb_s`` and ``vcore_s`` are the ResourceManager's
    figures once settled; with the DefaultResourceCalculator it counts one vcore per container,
    whatever cores a Spark executor runs. ``core_s`` counts the cores held: the ApplicationMaster
    over the elapsed time, plus each MapReduce task (one core) or each Spark executor (its cores)
    over its life. ``tasks`` are the tasks that read the orders: the MapReduce maps, or the Spark
    stage that read the most records. ``inputs`` are the directories read and ``result`` what the
    run computed; both stay empty when a finished application is reread.
    """

    volume: str
    variant: str
    run: int
    app_id: str
    elapsed_s: float
    jobs_s: float
    cpu_s: float
    memory_mb_s: int
    vcore_s: int
    core_s: float
    lines_read: int
    bytes_read: int
    shuffle_bytes: int
    task_count: int
    tasks: list[Task] = field(default_factory=list)
    details: dict = field(default_factory=dict)
    inputs: list[str] = field(default_factory=list)
    result: Result | None = None
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


def mr_task(task: dict, counters: dict) -> Task:
    """A map task from the JobHistory ``/tasks`` entry and its ``/counters`` answer."""
    values = {
        c["name"]: c["value"]
        for group in counters["jobTaskCounters"]["taskCounterGroup"]
        for c in group["counter"]
    }
    return Task(
        values["MAP_INPUT_RECORDS"], values["CPU_MILLISECONDS"] / 1000, task["elapsedTime"] / 1000
    )


def mr_core_seconds(job_counters: dict[str, int], elapsed_s: float) -> float:
    """The ApplicationMaster over ``elapsed_s``, plus the vcore time of every task attempt."""
    tasks_ms = job_counters.get("VCORES_MILLIS_MAPS", 0) + job_counters.get(
        "VCORES_MILLIS_REDUCES", 0
    )
    return elapsed_s + tasks_ms / 1000


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


def main_stage(stages: Iterable[dict]) -> dict:
    """The completed stage that read the most records: in this job, the one that reads orders."""
    return max(
        (s for s in stages if s.get("status") == "COMPLETE"), key=lambda s: s["inputRecords"]
    )


def spark_tasks(task_list: Iterable[dict]) -> list[Task]:
    """The successful tasks of a History Server ``/taskList`` answer."""
    return [
        Task(
            t["taskMetrics"]["inputMetrics"]["recordsRead"],
            t["taskMetrics"]["executorCpuTime"] / 1e9,
            t["taskMetrics"]["executorRunTime"] / 1000,
        )
        for t in task_list
        if t["status"] == "SUCCESS"
    ]


def shs_ms(text: str) -> int:
    """Epoch milliseconds of a History Server time such as ``2026-11-26T18:43:38.220GMT``."""
    moment = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%f%Z").replace(tzinfo=UTC)
    return round(moment.timestamp() * 1000)


def spark_core_seconds(executors: Iterable[dict], elapsed_s: float, finished_ms: int) -> float:
    """The ApplicationMaster over ``elapsed_s``, plus each executor's cores over its life.

    An executor lives from its registration to its removal, or to the application's end
    (``finished_ms``) when it was never removed. The client-mode driver runs outside YARN.
    """
    held = elapsed_s
    for executor in executors:
        if executor["id"] == "driver":
            continue
        end = shs_ms(executor["removeTime"]) if executor.get("removeTime") else finished_ms
        held += executor["totalCores"] * (end - shs_ms(executor["addTime"])) / 1000
    return held


def jobs_window_s(jobs: Iterable[dict]) -> float:
    """Seconds from the first submission to the last completion of the Spark ``/jobs`` answer."""
    times = [shs_ms(job[key]) for job in jobs for key in ("submissionTime", "completionTime")]
    return (max(times) - min(times)) / 1000


def manifest_quality(manifests: Iterable[dict]) -> dict[str, int]:
    """The Quality counters the drop manifests predict: their non-zero orders anomalies."""
    expected = Counter()
    for manifest in manifests:
        for entry in manifest["files"]:
            if entry["source"] == "orders":
                expected.update(
                    {code: n for code, n in entry["anomalies"].items() if code in QUALITY_CODES}
                )
    expected = +expected
    return {f"Quality.{code}": n for code, n in sorted(expected.items())}


def differences(reference: Result, other: Result) -> list[str]:
    """How ``other`` differs from ``reference``: its output bytes, then each country or counter."""
    found = []
    if other.output != reference.output:
        found.append(f"output {other.output!r} != {reference.output!r}")
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


def mann_whitney(a: Sequence[float], b: Sequence[float]) -> tuple[float, float, float]:
    """Exact Mann-Whitney test of ``a`` against ``b``: U, one-sided p and two-sided p.

    U counts the pairs where the value from ``a`` is the greater one, a tie for half. Under the
    hypothesis that both samples come from one distribution, every split of the pooled values into
    groups of len(a) and len(b) is equally likely: the one-sided p is the share of splits whose U
    is at least the observed one (small when ``a`` tends to be greater), the two-sided p twice the
    smaller tail.
    """

    def u(x: Sequence[float], y: Sequence[float]) -> float:
        return sum((xi > yi) + (xi == yi) / 2 for xi in x for yi in y)

    pooled = [*a, *b]
    observed = u(a, b)
    splits = []
    for chosen in map(set, combinations(range(len(pooled)), len(a))):
        group = [v for i, v in enumerate(pooled) if i in chosen]
        rest = [v for i, v in enumerate(pooled) if i not in chosen]
        splits.append(u(group, rest))
    greater = sum(s >= observed for s in splits) / len(splits)
    smaller = sum(s <= observed for s in splits) / len(splits)
    return observed, greater, min(1.0, 2 * min(greater, smaller))


def task_cost(tasks: Sequence[Task]) -> TaskCost | None:
    """The fixed cost of a task (intercept) and its cost per million records (slope), for CPU and
    run time; None unless the tasks read at least two different numbers of records."""
    millions = [t.records / 1e6 for t in tasks]
    if len(set(millions)) < 2:
        return None
    cpu = statistics.linear_regression(millions, [t.cpu_s for t in tasks])
    run = statistics.linear_regression(millions, [t.run_s for t in tasks])
    return TaskCost(cpu.intercept, cpu.slope, run.intercept, run.slope)


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

    def __init__(self, out: Path, settle_s: float = SETTLE_S) -> None:
        self.out = out
        self.settle_s = settle_s

    def get(self, url: str) -> dict:
        with urllib.request.urlopen(url, timeout=30) as response:
            return json.load(response)

    def poll(self, url: str, ready: Callable[[dict], bool], timeout_s: int = 120) -> dict:
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
        """The bytes of ``path`` as text; an invalid UTF-8 byte maps to a surrogate, not U+FFFD."""
        done = subprocess.run(["hdfs", "dfs", "-cat", path], capture_output=True, check=True)
        return done.stdout.decode("utf-8", "surrogateescape")

    def hdfs_ls(self, path: str) -> list[str]:
        done = subprocess.run(
            ["hdfs", "dfs", "-ls", "-C", path], capture_output=True, text=True, check=False
        )
        return done.stdout.split() if done.returncode == 0 else []

    def hdfs_bytes(self, path: str) -> int:
        """Logical size of ``path`` (``hdfs dfs -du -s``, before replication)."""
        done = subprocess.run(
            ["hdfs", "dfs", "-du", "-s", path], capture_output=True, text=True, check=True
        )
        return int(done.stdout.split()[0])

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

    def settled_app(self, app_id: str, timeout_s: int = 300) -> dict:
        """The RM record of ``app_id`` once final, when two readings ``settle_s`` apart agree."""
        deadline = time.monotonic() + timeout_s
        previous = None
        while True:
            app = self.app(app_id)
            reading = (app["state"], app["memorySeconds"], app["vcoreSeconds"])
            if app["state"] in FINAL_STATES and reading == previous:
                return app
            if time.monotonic() > deadline:
                raise TimeoutError(f"{app_id}: resources still changing after {timeout_s} s")
            previous = reading
            time.sleep(self.settle_s)

    def overlaps(self, app: dict) -> list[str]:
        apps = (self.get(f"{RM}/apps").get("apps") or {}).get("app", [])
        return overlapping(apps, app["id"], app["startedTime"], app["finishedTime"])


def measure_mapreduce(
    cluster: Cluster, app_id: str, volume: str, variant: str, run: int
) -> Measure:
    job_id = app_id.replace("application_", "job_", 1)
    job = cluster.poll(f"{JHS}/{job_id}", lambda p: p["job"]["state"] == "SUCCEEDED")["job"]
    groups = mr_counters(cluster.get(f"{JHS}/{job_id}/counters"))
    maps = cluster.get(f"{JHS}/{job_id}/tasks?type=m")["tasks"]["task"]
    tasks = [
        mr_task(t, cluster.get(f"{JHS}/{job_id}/tasks/{t['id']}/counters"))
        for t in maps
        if t["state"] == "SUCCEEDED"
    ]
    app = cluster.settled_app(app_id)
    task, launched = groups[TASK], groups[JOB]
    return Measure(
        volume=volume,
        variant=variant,
        run=run,
        app_id=app_id,
        elapsed_s=app["elapsedTime"] / 1000,
        jobs_s=(job["finishTime"] - job["startTime"]) / 1000,
        cpu_s=task["CPU_MILLISECONDS"] / 1000,
        memory_mb_s=app["memorySeconds"],
        vcore_s=app["vcoreSeconds"],
        core_s=mr_core_seconds(launched, app["elapsedTime"] / 1000),
        lines_read=task["MAP_INPUT_RECORDS"],
        bytes_read=groups[FILESYSTEM]["HDFS_BYTES_READ"],
        shuffle_bytes=task["REDUCE_SHUFFLE_BYTES"],
        task_count=launched["TOTAL_LAUNCHED_MAPS"] + launched["TOTAL_LAUNCHED_REDUCES"],
        tasks=tasks,
        details={
            "rack_local_maps": launched.get("RACK_LOCAL_MAPS", 0),
            "data_local_maps": launched.get("DATA_LOCAL_MAPS", 0),
            "killed_map_attempts": launched.get("NUM_KILLED_MAPS", 0),
            "map_output_records": task["MAP_OUTPUT_RECORDS"],
            "combine_output_records": task["COMBINE_OUTPUT_RECORDS"],
            "spilled_records": task["SPILLED_RECORDS"],
            "gc_ms": task["GC_TIME_MILLIS"],
            "map_ms": launched["MILLIS_MAPS"],
            "reduce_ms": launched["MILLIS_REDUCES"],
            "map_start_offsets_ms": sorted(t["startTime"] - job["startTime"] for t in maps),
        },
    )


def measure_spark(cluster: Cluster, app_id: str, volume: str, variant: str, run: int) -> Measure:
    cluster.poll(f"{SHS}/{app_id}", lambda p: p["attempts"][0]["completed"])
    stages = cluster.get(f"{SHS}/{app_id}/stages")
    totals = stage_totals(stages)
    main = main_stage(stages)
    task_list = cluster.get(
        f"{SHS}/{app_id}/stages/{main['stageId']}/{main['attemptId']}/taskList?length=100000"
    )
    jobs = cluster.get(f"{SHS}/{app_id}/jobs")
    executors = cluster.get(f"{SHS}/{app_id}/allexecutors")
    app = cluster.settled_app(app_id)
    return Measure(
        volume=volume,
        variant=variant,
        run=run,
        app_id=app_id,
        elapsed_s=app["elapsedTime"] / 1000,
        jobs_s=jobs_window_s(jobs),
        cpu_s=totals["executorCpuTime"] / 1e9,
        memory_mb_s=app["memorySeconds"],
        vcore_s=app["vcoreSeconds"],
        core_s=spark_core_seconds(executors, app["elapsedTime"] / 1000, app["finishedTime"]),
        lines_read=totals["inputRecords"],
        bytes_read=totals["inputBytes"],
        shuffle_bytes=totals["shuffleWriteBytes"],
        task_count=totals["numCompleteTasks"],
        tasks=spark_tasks(task_list),
        details={
            "spark_jobs": len(jobs),
            "executor_run_ms": totals["executorRunTime"],
            "gc_ms": totals["jvmGcTime"],
        },
    )


def run_mapreduce(
    cluster: Cluster, volume: Volume, variant: Variant, run: int, day: date, output: str
) -> Measure:
    settings = [arg for setting in variant.mr_conf for arg in ("-D", setting)]
    stdout = cluster.run(
        f"{volume.name}-{variant.name}-{run}",
        "bash",
        "scripts/mapreduce/run_mr.sh",
        volume.landing,
        day.isoformat(),
        output,
        *settings,
    )
    job_id = stdout.strip().splitlines()[-1].removeprefix("job_id=")
    app_id = job_id.replace("job_", "application_", 1)
    measure = measure_mapreduce(cluster, app_id, volume.name, variant.name, run)
    conf = {p["name"]: p["value"] for p in cluster.get(f"{JHS}/{job_id}/conf")["conf"]["property"]}
    customer_files = [f for f in conf["tmpfiles"].split(",") if not f.startswith("file:")]
    measure.inputs = sorted(
        conf["mapreduce.input.fileinputformat.inputdir"].split(",")
        + [posixpath.dirname(f) for f in customer_files]
    )
    counters = result_counters(mr_counters(cluster.get(f"{JHS}/{job_id}/counters")))
    measure.result = Result(cluster.hdfs_cat(f"{output}/part-*"), counters)
    return measure


def run_spark(cluster: Cluster, volume: Volume, variant: Variant, run: int, day: date) -> Measure:
    stdout = cluster.run(
        f"{volume.name}-{variant.name}-{run}",
        "bash",
        "scripts/cluster/submit.sh",
        "revenue-by-country-spark",
        "--date",
        day.isoformat(),
        "--set",
        f"paths.landing={volume.landing}",
    )
    records = [json.loads(line) for line in stdout.splitlines() if line.startswith("{")]
    done = next(r for r in records if r["message"] == "revenue by country written")
    measure = measure_spark(cluster, done["yarn_app_id"], volume.name, variant.name, run)
    measure.inputs = sorted(
        {posixpath.dirname(path) for path in done["order_files"]} | set(done["customer_dirs"])
    )
    counters = {}
    for line in cluster.hdfs_cat(f"{done['output']}/counters/part-*").splitlines():
        group, name, value = line.split("\t")
        counters[f"{group}.{name}"] = int(value)
    measure.result = Result(cluster.hdfs_cat(f"{done['output']}/revenue/part-*"), counters)
    return measure


def checks(measures: list[Measure], expected_quality: dict[str, int] | None) -> list[str]:
    """Every failed check of one volume's runs, as a sentence; empty when the engines agree."""
    reference = measures[0]
    order_lines = next(m.lines_read for m in measures if m.variant == "mapreduce")
    failures = []
    for measure in measures:
        label = f"{measure.variant} #{measure.run}"
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


def _seconds(value: float | None) -> str:
    return "—" if value is None else f"{value:.2f}"


PYTHON_CPU_S_PER_MILLION = (2.1, 2.7)
"""CPU seconds of the Python mapper per million lines, measured in a client container outside
YARN, which does not count it (docs/mapreduce-vs-spark.md): 2.1 on a quiet machine, 2.7 the
median of the current mapper under load."""
COMBINER_CPU_S_PER_MILLION = 0.24
"""CPU seconds of the combiner per million lines, measured the same way."""


@dataclass(frozen=True)
class EngineComparison:
    """A MapReduce variant against Spark on one volume, from their medians: the change of the
    memory MapReduce reserves, of the cores Spark holds, and the range of the ratio of CPU per
    line, MapReduce over Spark (None without a CPU slope)."""

    memory_change_pct: float
    core_change_pct: float
    cpu_ratio: tuple[float, float] | None


def engine_comparison(
    mapreduce: Mapping[str, float | None], spark: Mapping[str, float | None]
) -> EngineComparison:
    """``mapreduce`` and ``spark`` hold the medians ``memory_mb_s``, ``core_s`` and
    ``cpu_s_per_million``, the slope of the task CPU (the JVMs only for MapReduce). MapReduce's
    CPU per line adds the Python mapper and the combiner, measured aside."""
    jvm, slope = mapreduce["cpu_s_per_million"], spark["cpu_s_per_million"]
    ratio = None
    if jvm is not None and slope:
        low, high = PYTHON_CPU_S_PER_MILLION
        ratio = tuple((jvm + p + COMBINER_CPU_S_PER_MILLION) / slope for p in (low, high))
    return EngineComparison(
        100 * (mapreduce["memory_mb_s"] - spark["memory_mb_s"]) / spark["memory_mb_s"],
        100 * (spark["core_s"] - mapreduce["core_s"]) / mapreduce["core_s"],
        ratio,
    )


def engines_markdown(
    syntheses: Mapping[str, Mapping[str, Mapping[str, float | None]]],
) -> list[str]:
    """One line per volume and MapReduce variant: its ``engine_comparison`` with Spark."""
    lines = [
        "| Volume | Variante MapReduce | Mo·s de MapReduce contre Spark "
        "| Cœurs·s de Spark contre MapReduce | CPU par ligne, MapReduce / Spark |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for volume, medians in syntheses.items():
        for variant, own in medians.items():
            if variant == "spark" or "spark" not in medians:
                continue
            found = engine_comparison(own, medians["spark"])
            cpu = "—" if found.cpu_ratio is None else "{:.1f} à {:.1f}".format(*found.cpu_ratio)
            lines.append(
                f"| `{volume}` | {variant} | {found.memory_change_pct:+.1f} % "
                f"| {found.core_change_pct:+.1f} % | {cpu} |"
            )
    return lines


SYNTHESIS_COLUMNS = {"memory_mb_s": 6, "core_s": 7, "cpu_s_per_million": 9}
"""The cells of a variant's row in the Synthèse table of ``volume_report``."""


def synthesis(report: str) -> dict[str, dict[str, dict[str, float | None]]]:
    """The medians of each variant in the Synthèse tables of a campaign ``report.md``, by
    volume, as ``volume_report`` printed them."""
    found = {}
    for section in report.split("## Volume `")[1:]:
        table = section.split("### Synthèse", 1)[1].split("###", 1)[0]
        rows = {}
        for line in table.splitlines():
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) > max(SYNTHESIS_COLUMNS.values()) and cells[1].isdigit():
                rows[cells[0]] = {
                    key: None if cells[i] == "—" else float(cells[i])
                    for key, i in SYNTHESIS_COLUMNS.items()
                }
        found[section.split("`", 1)[0]] = rows
    return found


COST_FIELDS = ("fixed_cpu_s", "cpu_s_per_million", "fixed_run_s", "run_s_per_million")


def volume_report(
    volume: str,
    measures: list[Measure],
    context: str,
    failures: list[str] | None,
    manifests: bool = False,
) -> list[str]:
    """The report section of one volume; ``failures`` is None when the runs were not checked,
    ``manifests`` tells whether the Quality counters were checked against drop manifests."""
    variants = list(dict.fromkeys(m.variant for m in measures))
    rows = [
        "| Variante | Exécution | Application | Durée (s) | Dont jobs (s) | CPU compté (s) | Mo·s "
        "| Cœurs·s | Lignes lues | Octets lus | Octets de shuffle | Tâches | Exclusive "
        "| Contrôle YARN avant l'exécution |",
        "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- "
        "| --- |",
    ]
    for m in measures:
        exclusive = "oui" if not m.overlapping else "non : " + ", ".join(m.overlapping)
        rows.append(
            f"| {m.variant} | {m.run} | `{m.app_id}` | {m.elapsed_s:.1f} | {m.jobs_s:.1f} "
            f"| {m.cpu_s:.1f} | {m.memory_mb_s} | {m.core_s:.1f} | {m.lines_read} "
            f"| {m.bytes_read} | {m.shuffle_bytes} | {m.task_count} | {exclusive} "
            f"| {m.idle_check} |"
        )
    summary = [
        "| Variante | n | Durée médiane (s) | Min | Max | Écart (max - min) / médiane "
        "| Mo·s médian | Cœurs·s médian | CPU fixe par tâche (s) | CPU par M lignes (s) "
        "| Durée fixe par tâche (s) | Durée par M lignes (s) |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    medians: dict[str, dict[str, float | None]] = {}
    for variant in variants:
        own = [m for m in measures if m.variant == variant]
        median, low, high, rel = spread([m.elapsed_s for m in own])
        costs = [c for c in (task_cost(m.tasks) for m in own) if c is not None]
        cost = [
            _seconds(statistics.median(getattr(c, name) for c in costs) if costs else None)
            for name in COST_FIELDS
        ]
        medians[variant] = {
            "memory_mb_s": statistics.median(m.memory_mb_s for m in own),
            "core_s": statistics.median(m.core_s for m in own),
            "cpu_s_per_million": (
                statistics.median(c.cpu_s_per_million for c in costs) if costs else None
            ),
        }
        summary.append(
            f"| {variant} | {len(own)} | {median:.1f} | {low:.1f} | {high:.1f} | {rel:.0%} "
            f"| {statistics.median(m.memory_mb_s for m in own):.0f} "
            f"| {statistics.median(m.core_s for m in own):.1f} | {' | '.join(cost)} |"
        )
    tests = [
        "| A | B | n (A, B) | U (paires où A est plus lent) | p unilatéral (A plus lent) "
        "| p bilatéral |",
        "| --- | --- | --- | ---: | ---: | ---: |",
    ]
    for a, b in combinations(variants, 2):
        times_a = [m.elapsed_s for m in measures if m.variant == a]
        times_b = [m.elapsed_s for m in measures if m.variant == b]
        u, one_sided, two_sided = mann_whitney(times_a, times_b)
        tests.append(
            f"| {a} | {b} | {len(times_a)}, {len(times_b)} | {u:g} / {len(times_a) * len(times_b)} "
            f"| {one_sided:.3f} | {two_sided:.3f} |"
        )
    lines = [
        f"## Volume `{volume}`",
        "",
        context,
        "",
        "### Exécutions",
        "",
        *rows,
        "",
        "### Synthèse",
        "",
        *summary,
        "",
        "### Comparaison des moteurs",
        "",
        *engines_markdown({volume: medians}),
        "",
        "### Test de Mann-Whitney exact sur la durée",
        "",
        *tests,
        "",
    ]
    if failures is None:
        return lines
    reference = measures[0].result
    if failures:
        verdict = ["Écarts :", "", *[f"- {f}" for f in failures]]
    else:
        verdict = [
            "Toutes les exécutions donnent la même sortie octet pour octet et les mêmes "
            "compteurs ; les compteurs couvrent toutes les lignes lues."
        ]
        if manifests:
            verdict.append("Les compteurs Quality sont égaux aux anomalies des manifestes.")
    return [
        *lines,
        "### Résultat de référence",
        "",
        "| Pays | CA facturé (EUR) | Lignes COMPLETED |",
        "| --- | ---: | ---: |",
        *[f"| {c} | {a} | {n} |" for c, (a, n) in sorted(reference.revenue.items())],
        "",
        "| Compteur | Valeur |",
        "| --- | ---: |",
        *[f"| {k} | {v} |" for k, v in sorted(reference.counters.items())],
        "",
        "### Vérifications",
        "",
        *verdict,
        "",
    ]


def report(header: str, sections: list[list[str]]) -> str:
    code = ["| Moteur | Fichier | Lignes | Lignes de code |", "| --- | --- | ---: | ---: |"]
    for engine, paths in CODE_FILES.items():
        for path in paths:
            total, logic = code_lines(REPO / path)
            code.append(f"| {engine} | `{path}` | {total} | {logic} |")
    body = [line for section in sections for line in section]
    return "\n".join(
        [
            "# MapReduce contre Spark : mesures",
            "",
            header,
            "",
            *body,
            "## Taille du code",
            "",
            *code,
        ]
    )


def exclusive_run(
    cluster: Cluster, volume: Volume, variant: Variant, run: int, args: argparse.Namespace
) -> Measure:
    """One run after YARN is idle, run again while another application overlapped it."""
    for _ in range(EXCLUSIVE_ATTEMPTS):
        evidence = cluster.wait_idle(not args.no_wait, args.max_wait)
        if variant.engine == "mapreduce":
            measure = run_mapreduce(cluster, volume, variant, run, args.date, args.mr_output)
        else:
            measure = run_spark(cluster, volume, variant, run, args.date)
        measure.idle_check = evidence
        measure.overlapping = cluster.overlaps(cluster.app(measure.app_id))
        print(f"{volume.name} {variant.name} #{run}: {measure.app_id} {measure.elapsed_s:.1f} s")
        if args.no_wait or not measure.overlapping:
            break
        print(f"  discarded: {', '.join(measure.overlapping)} ran at the same time")
    return measure


def campaign(cluster: Cluster, args: argparse.Namespace) -> int:
    variants = [Variant("mapreduce", "mapreduce"), Variant("spark", "spark")]
    if args.mr_conf:
        variants.insert(1, Variant("mapreduce-tuned", "mapreduce", tuple(args.mr_conf)))
    measures: list[Measure] = []
    for volume in args.volumes:
        for run in range(1, args.runs + 1):
            for variant in variants:
                measures.append(exclusive_run(cluster, volume, variant, run, args))
                (args.out / "runs.json").write_text(
                    json.dumps([asdict(m) for m in measures], indent=2), "utf-8"
                )

    sections, failed = [], False
    for volume in args.volumes:
        own = [m for m in measures if m.volume == volume.name]
        manifests = [
            json.loads(cluster.hdfs_cat(f"{path}/_MANIFEST.json"))
            for path in cluster.hdfs_ls(f"{volume.landing}/_manifests")
            if path.rsplit("=", 1)[-1] <= args.date.isoformat()
        ]
        failures = checks(own, manifest_quality(manifests) if manifests else None)
        sizes = "\n".join(
            f"- `{path.removeprefix(volume.landing)}` : {cluster.hdfs_bytes(path)} octets"
            for path in own[0].inputs
        )
        context = f"Entrée : `{volume.landing}`, partitions jusqu'au {args.date} :\n\n{sizes}"
        sections.append(volume_report(volume.name, own, context, failures, bool(manifests)))
        failed |= bool(failures)
        for failure in failures:
            print(f"FAILED {volume.name}: {failure}", file=sys.stderr)
    settings = ", ".join(f"`{s}`" for s in args.mr_conf) or "aucun"
    header = (
        f"{args.runs} exécutions par variante en alternance, le "
        f"{datetime.now(UTC):%Y-%m-%d %H:%M} UTC, par `scripts/mapreduce/compare.sh` au commit "
        f"`{load_config('cluster').git_sha}`. Réglages de `mapreduce-tuned` : {settings}."
    )
    (args.out / "report.md").write_text(report(header, sections), "utf-8")
    print(f"report: {args.out / 'report.md'}")
    return 1 if failed else 0


def reread(cluster: Cluster, groups: dict[str, list[str]], out: Path) -> int:
    sections, measures = [], []
    for volume, app_ids in groups.items():
        own: list[Measure] = []
        for app_id in app_ids:
            engine = (
                "mapreduce" if cluster.app(app_id)["applicationType"] == "MAPREDUCE" else "spark"
            )
            run = 1 + sum(m.variant == engine for m in own)
            measure = (measure_mapreduce if engine == "mapreduce" else measure_spark)(
                cluster, app_id, volume, engine, run
            )
            print(f"{volume} {engine} #{run}: {app_id} {measure.memory_mb_s} Mo·s", flush=True)
            own.append(measure)
        measures += own
        context = f"Applications relues : {', '.join(f'`{a}`' for a in app_ids)}."
        sections.append(volume_report(volume, own, context, None))
    header = (
        f"Relecture des applications terminées le {datetime.now(UTC):%Y-%m-%d %H:%M} UTC, sans "
        "nouvelle exécution : les sorties, remplacées depuis, ne sont pas revérifiées."
    )
    (out / "runs.json").write_text(json.dumps([asdict(m) for m in measures], indent=2), "utf-8")
    (out / "report.md").write_text(report(header, sections), "utf-8")
    print(f"report: {out / 'report.md'}")
    return 0


def _pair(text: str) -> tuple[str, str]:
    name, sep, value = text.partition("=")
    if not (name and sep and value):
        raise argparse.ArgumentTypeError(f"expected NAME=VALUE, got {text!r}")
    return name, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mr_compare", description=__doc__.split("\n\n")[0])
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument(
        "--volumes",
        nargs="+",
        type=_pair,
        metavar="NAME=LANDING",
        help="landing roots to measure (default: landing=paths.landing of --env cluster)",
    )
    parser.add_argument("--runs", type=int, default=5, help="runs per variant and volume")
    parser.add_argument(
        "--mr-conf",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="adds the variant mapreduce-tuned: the MapReduce job with -D KEY=VALUE",
    )
    parser.add_argument("--out", type=Path, default=REPO / "build" / "mr-vs-spark")
    parser.add_argument("--no-wait", action="store_true", help="do not wait for an idle YARN")
    parser.add_argument("--max-wait", type=int, default=3600, help="seconds, per run")
    parser.add_argument(
        "--reread",
        nargs="+",
        type=_pair,
        metavar="NAME=APP_ID,APP_ID",
        help="rebuild the measures of finished applications instead of running",
    )
    parser.add_argument(
        "--summarize",
        type=Path,
        metavar="REPORT",
        help="print the engine comparison of each volume of a campaign report.md, then stop",
    )
    args = parser.parse_args(argv)
    if args.summarize:
        print("\n".join(engines_markdown(synthesis(args.summarize.read_text("utf-8")))))
        return 0
    for setting in args.mr_conf:
        _pair(setting)
    args.out.mkdir(parents=True, exist_ok=True)
    cluster = Cluster(args.out)
    if args.reread:
        return reread(cluster, {name: ids.split(",") for name, ids in args.reread}, args.out)
    if args.date is None:
        parser.error("--date is required unless --reread is given")
    cfg = load_config("cluster")
    pairs = args.volumes or [("landing", cfg.paths.landing)]
    args.volumes = [Volume(name, landing.rstrip("/")) for name, landing in pairs]
    args.mr_output = f"{cfg.paths.ops}/mr_vs_spark/mapreduce"
    return campaign(cluster, args)


if __name__ == "__main__":
    sys.exit(main())
