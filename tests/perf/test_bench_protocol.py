"""The timed protocol, played with a fake SparkSession and fake ResourceManager and History Server
answers: alternation, warm-up rounds, settings per variant, the exclusivity wait, the stages of
each run, and prepare's row-count check."""

import socket
from argparse import Namespace
from urllib.error import URLError

import pytest

from gtdp.perf import bench
from gtdp.perf import datasets as ds
from gtdp.perf.bench import Run, Variant
from gtdp.perf.mr_compare import RM, SHS, Cluster

OWN = "application_1"
IDLE = {"apps": {"app": [{"id": OWN}]}}


class Turns(list):
    """Answers given in turn, one per request."""


class FakeCluster(Cluster):
    """REST answers by URL, query string left out."""

    def __init__(self, answers):
        super().__init__(out=None, settle_s=0)
        self.answers = answers
        self.asked: list[str] = []

    def get(self, url):
        self.asked.append(url)
        answer = self.answers[url.split("?")[0]]
        answer = answer.pop(0) if isinstance(answer, Turns) else answer
        if isinstance(answer, Exception):
            raise answer
        return answer


class FakeConf:
    def __init__(self, values):
        self.values = dict(values)

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value

    def unset(self, key):
        self.values.pop(key, None)


class FakeContext:
    applicationId = OWN

    def __init__(self):
        self.groups: list[str] = []

    def setJobGroup(self, group, description):
        self.groups.append(group)


class FakeSpark:
    def __init__(self, conf=()):
        self.conf = FakeConf(conf)
        self.sparkContext = FakeContext()


def recording(name, seen, conf=None):
    def action(spark):
        seen.append((name, dict(spark.conf.values)))
        return [1, "ab"], {}

    return Variant(name, action, conf or {})


def idle_cluster():
    return FakeCluster({f"{RM}/apps": IDLE})


def test_every_round_runs_every_variant_in_turn_after_the_warm_up_rounds():
    spark, seen = FakeSpark(), []
    variants = [recording("a", seen), recording("b", seen)]
    runs = bench.timed(spark, "exp", variants, runs=2, warmup=2, cluster=idle_cluster())
    order = ["a/-1", "b/-1", "a/0", "b/0", "a/1", "b/1", "a/2", "b/2"]
    assert [r.group for r in runs] == [f"exp/{o}" for o in order]
    assert spark.sparkContext.groups == [f"exp/{o}" for o in order]
    assert [name for name, _ in seen] == [o[0] for o in order]
    assert [r.run for r in runs if r.run > 0] == [1, 1, 2, 2]


def test_no_warm_up_round_when_none_is_asked():
    runs = bench.timed(FakeSpark(), "exp", [recording("a", [])], 3, 0, idle_cluster())
    assert [r.run for r in runs] == [1, 2, 3]


def test_a_variant_sets_its_settings_for_its_own_run_only():
    spark, seen = FakeSpark({"x": "base"}), []
    variants = [recording("a", seen, {"x": "a", "y": "1"}), recording("b", seen)]
    bench.timed(spark, "exp", variants, runs=1, warmup=0, cluster=idle_cluster())
    assert seen == [("a", {"x": "a", "y": "1"}), ("b", {"x": "base"})]
    assert spark.conf.values == {"x": "base"}


def test_each_run_records_the_yarn_check_made_before_it():
    runs = bench.timed(FakeSpark(), "exp", [recording("a", [])], 1, 0, idle_cluster())
    assert runs[0].yarn_check.endswith(
        ": 0 other application NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING"
    )


def test_the_driver_waits_until_yarn_runs_no_other_application(monkeypatch):
    busy = {"apps": {"app": [{"id": OWN}, {"id": "application_2"}]}}
    cluster = FakeCluster({f"{RM}/apps": Turns([busy, busy, IDLE])})
    sleeps = []
    monkeypatch.setattr(bench.time, "sleep", sleeps.append)
    check = bench.yarn_idle(cluster, OWN)
    assert ": 0 other application" in check
    assert len(cluster.asked) == 3
    assert sleeps == [15, 15]


def test_the_driver_keeps_waiting_while_the_resource_manager_does_not_answer(monkeypatch):
    """A name-resolution failure of the client container, seen twice in the fix round, must not
    end a benchmark: the check is only made once the RM answers."""
    unreachable = URLError(socket.gaierror(-3, "Temporary failure in name resolution"))
    cluster = FakeCluster({f"{RM}/apps": Turns([unreachable, IDLE])})
    monkeypatch.setattr(bench.time, "sleep", lambda _: None)
    assert ": 0 other application" in bench.yarn_idle(cluster, OWN)
    assert len(cluster.asked) == 2
    with pytest.raises(TimeoutError, match="did not answer"):
        bench.yarn_idle(FakeCluster({f"{RM}/apps": Turns([unreachable] * 2)}), OWN, -1)


def test_the_wait_gives_up_after_its_limit(monkeypatch):
    busy = {"apps": {"app": [{"id": "application_2"}]}}
    monkeypatch.setattr(bench.time, "sleep", lambda _: None)
    with pytest.raises(TimeoutError, match="application_2"):
        bench.yarn_idle(FakeCluster({f"{RM}/apps": Turns([busy, busy])}), OWN, max_wait_s=-1)


def stage(sid, attempt=0, status="COMPLETE", start="03:00:00.000", end="03:00:01.000", **bytes_):
    return {
        "stageId": sid,
        "attemptId": attempt,
        "status": status,
        "numCompleteTasks": 4,
        "firstTaskLaunchedTime": f"2026-11-27T{start}GMT",
        "completionTime": f"2026-11-27T{end}GMT",
        **bytes_,
    }


SUMMARY = {
    "quantiles": [0.5, 1.0],
    "duration": [100.0, 400.0],
    "inputMetrics": {"bytesRead": [10.0, 20.0]},
    "shuffleReadMetrics": {"readBytes": [5.0, 50.0]},
}
JOBS = [
    {"jobId": 0, "jobGroup": "exp/scan/1", "stageIds": [0, 1]},
    {"jobId": 1, "jobGroup": "exp/join/1", "stageIds": [2, 3, 4]},
]
STAGES = [
    stage(0, inputBytes=900, shuffleReadBytes=0, end="03:00:02.500"),
    stage(1, inputBytes=0, shuffleReadBytes=60),
    stage(2, inputBytes=100, shuffleReadBytes=0),
    stage(3, inputBytes=0, shuffleReadBytes=500, start="03:00:05.000", end="03:00:05.750"),
    stage(4, status="SKIPPED", inputBytes=0, shuffleReadBytes=0),
]


def history(jobs=JOBS, stages=STAGES, apps=()):
    answers = {
        f"{SHS}/{OWN}": {"attempts": [{"completed": True}]},
        f"{SHS}/{OWN}/jobs": jobs,
        f"{SHS}/{OWN}/stages": stages,
        f"{RM}/apps": {"apps": {"app": [{"id": OWN, "startedTime": 0, "finishedTime": 0}, *apps]}},
    }
    for s in stages:
        answers[f"{SHS}/{OWN}/stages/{s['stageId']}/{s['attemptId']}/taskSummary"] = SUMMARY
    return FakeCluster(answers)


def measured(variant, started_ms=1000, wall_s=2.0):
    return Run(variant, 1, f"exp/{variant}/1", started_ms, wall_s, "0 other", [1])


def test_each_run_gets_the_totals_and_key_stage_of_its_own_job_group():
    runs = [measured("scan"), measured("join")]
    bench.measure(OWN, runs, {"scan": "inputBytes", "join": "shuffleReadBytes"}, history())
    scan, join = runs
    assert (scan.stages["inputBytes"], scan.stages["shuffleReadBytes"]) == (900, 60)
    assert (scan.key_stage["stage"], scan.key_stage["read_max_bytes"]) == (0, 20.0)
    assert (join.key_stage["stage"], join.key_stage["read_max_bytes"]) == (3, 50.0)
    assert (join.key_stage["duration_median_ms"], join.key_stage["duration_max_ms"]) == (100, 400)
    assert [r["read_ratio"] for r in bench.summarize(runs)] == [2.0, 10.0]
    assert scan.stage_walls == {"scan": 2.5, "key": 2.5}
    assert join.stage_walls == {"scan": 1.0, "key": 0.75}


def test_the_applications_that_overlapped_a_run_are_recorded():
    other = {"id": "application_9", "startedTime": 2500, "finishedTime": 9000}
    before = {"id": "application_8", "startedTime": 0, "finishedTime": 900}
    runs = [measured("scan", started_ms=1000, wall_s=2.0)]
    bench.measure(OWN, runs, {"scan": "inputBytes"}, history(apps=[other, before]))
    assert runs[0].overlapping == ["application_9"]


def test_a_run_whose_stages_the_history_server_dropped_fails_loudly():
    stages = [s for s in STAGES if s["stageId"] != 3]
    with pytest.raises(LookupError, match=r"dropped stages \[3\] of exp/join/1"):
        bench.measure(OWN, [measured("join")], {"join": "shuffleReadBytes"}, history(stages=stages))


def test_an_application_whose_first_jobs_were_dropped_fails_loudly():
    with pytest.raises(LookupError, match="dropped jobs 0 to 0"):
        bench.measure(OWN, [measured("join")], {"join": "shuffleReadBytes"}, history(jobs=JOBS[1:]))


def counts(**differ):
    found = {f"{layout.name}/{name}": 10 for layout in ds.LAYOUTS for name in ds.SCHEMAS}
    return found | differ | {"versions": 3}


def test_prepare_refuses_layouts_that_hold_different_row_counts(monkeypatch):
    monkeypatch.setattr(bench.ds, "prepare", lambda *_: counts(**{"parquet_part/orders": 9}))
    monkeypatch.setattr(bench, "du", lambda path: {"logical": 1, "physical": 3})
    with pytest.raises(AssertionError, match="orders"):
        bench.prepare(None, Namespace(landing="l", root="r"))


def test_prepare_reports_the_rows_and_the_bytes_stored_of_every_layout(monkeypatch):
    monkeypatch.setattr(bench.ds, "prepare", lambda *_: counts())
    monkeypatch.setattr(bench, "du", lambda path: {"logical": 1, "physical": 3, "path": path})
    found = bench.prepare(None, Namespace(landing="l", root="r"))
    assert found["rows"]["csv/orders"] == 10
    assert found["stored"]["parquet_part_zstd/products"]["path"] == "r/parquet_part_zstd/products"
