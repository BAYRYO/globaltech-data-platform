"""Taking a table lock over: the YARN state of the run that holds it, its age, racing takers."""

import itertools
import json
import logging
import os
import socket
import threading
import time
from contextlib import ExitStack, suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

import gtdp.io
from gtdp.io import (
    RUN_ID_PROPERTY,
    TableLockedError,
    path_exists,
    read_table,
    replace_table,
    table_lock,
)

pytestmark = pytest.mark.slow

RM_ADDRESS = "yarn.resourcemanager.webapp.address"
APP = "application_1795715756506_0007"
DAY_OLD = 24 * 3600


def lock_of(application_id):
    return json.dumps(
        {
            "run_id": "run-dead",
            "application_id": application_id,
            "locked_at": "2026-11-27T00:57:40+00:00",
            "token": "d613c746e8564759a20fec2e0e987470",
        }
    )


def make_old(path):
    old = time.time() - DAY_OLD
    os.utime(path, (old, old))


def closed_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture
def table(tmp_path):
    """The path of a table, and its lock file."""
    return str(tmp_path / "orders"), tmp_path / "_orders.lock"


@pytest.fixture
def resource_manager(spark):
    """The REST API of a ResourceManager on localhost, named by the Hadoop configuration.

    ``states`` maps an application id to the state it reports, or to the raw body it answers.
    Any other id is unknown (404). ``requests`` lists the paths it was asked for.
    """
    rm = SimpleNamespace(states={}, requests=[])

    class Api(BaseHTTPRequestHandler):
        def do_GET(self):
            rm.requests.append(self.path)
            application_id = self.path.rsplit("/", 1)[-1]
            answer = rm.states.get(application_id)
            if answer is None:
                status, body = 404, {"RemoteException": {"exception": "NotFoundException"}}
            else:
                status, body = 200, {"app": {"id": application_id, "state": answer}}
            data = answer if isinstance(answer, bytes) else json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Api)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    previous = conf.get(RM_ADDRESS)
    conf.set(RM_ADDRESS, f"127.0.0.1:{server.server_port}")
    try:
        yield rm
    finally:
        if previous is None:
            conf.unset(RM_ADDRESS)
        else:
            conf.set(RM_ADDRESS, previous)
        server.shutdown()
        server.server_close()


@pytest.mark.parametrize("state", ["FINISHED", "FAILED", "KILLED"])
def test_the_lock_of_a_run_that_yarn_reports_ended_is_taken_over_at_once(
    spark, table, resource_manager, caplog, state
):
    path, lock = table
    lock.write_text(lock_of(APP))
    resource_manager.states[APP] = state

    with caplog.at_level(logging.WARNING, logger="gtdp.io"), table_lock(spark, path):
        held = json.loads(lock.read_text())

    assert held["application_id"] == spark.sparkContext.applicationId
    assert set(resource_manager.requests) == {f"/ws/v1/cluster/apps/{APP}"}
    assert APP in caplog.text
    assert not lock.exists()


@pytest.mark.parametrize("state", ["RUNNING", "ACCEPTED"])
def test_the_lock_of_a_run_that_yarn_reports_alive_holds_whatever_its_age(
    spark, table, resource_manager, state
):
    path, lock = table
    lock.write_text(lock_of(APP))
    make_old(lock)
    resource_manager.states[APP] = state

    with pytest.raises(TableLockedError, match=APP), table_lock(spark, path):
        pass

    assert lock.read_text() == lock_of(APP)


UNREACHABLE = "unreachable"


@pytest.mark.parametrize("old", [False, True], ids=["fresh", "old"])
@pytest.mark.parametrize(
    ("application_id", "answer"),
    [
        ("local-1795715756506", None),
        (APP, None),
        (APP, b"<html>Service Unavailable</html>"),
        (APP, UNREACHABLE),
    ],
    ids=["local-mode", "unknown-to-yarn", "not-json", "rm-unreachable"],
)
def test_without_a_state_from_yarn_a_lock_is_judged_by_its_age(
    spark, table, resource_manager, caplog, application_id, answer, old
):
    path, lock = table
    lock.write_text(lock_of(application_id))
    if old:
        make_old(lock)
    if answer == UNREACHABLE:
        spark.sparkContext._jsc.hadoopConfiguration().set(RM_ADDRESS, f"127.0.0.1:{closed_port()}")
    elif answer is not None:
        resource_manager.states[application_id] = answer

    with caplog.at_level(logging.INFO, logger="gtdp.io"):
        if old:
            with table_lock(spark, path):
                pass
        else:
            with pytest.raises(TableLockedError, match=application_id), table_lock(spark, path):
                pass

    assert application_id in caplog.text
    assert lock.exists() is not old
    if application_id.startswith("local-"):
        assert resource_manager.requests == []


@pytest.mark.parametrize(
    ("state", "old", "put_back"),
    [("FINISHED", False, True), ("RUNNING", True, False)],
    ids=["writer-ended", "writer-running"],
)
def test_a_reader_finishes_an_interrupted_swap_once_yarn_reports_its_writer_ended(
    spark, tmp_path, resource_manager, state, old, put_back
):
    gold = tmp_path / "gold"
    path = str(gold / "dim")
    replace_table(spark.createDataFrame([(1, "v1")], "id int, version string"), path)
    os.rename(gold / "dim", gold / "_dim.old")  # the writer died between its two renames
    lock = gold / "_dim.lock"
    lock.write_text(lock_of(APP))
    if old:
        make_old(lock)
    resource_manager.states[APP] = state

    assert path_exists(spark, path) is put_back
    if put_back:
        assert [tuple(r) for r in read_table(spark, path).collect()] == [(1, "v1")]


# The takeover of a stale lock is a few calls on the lock and on files named after it (the steps).
# Each test below runs its takers through every split of their steps.
SPLITS = range(14)


class Steps:
    """The filesystem of one taker, which calls ``pause`` before its step number ``at``."""

    def __init__(self, fs, at, pause):
        self._fs, self._at, self._pause, self.count = fs, at, pause, 0

    def __getattr__(self, name):
        member = getattr(self._fs, name)

        def call(*args):
            if args and ".lock" in str(args[0]).rsplit("/", 1)[-1]:
                if self.count == self._at:
                    self._pause()
                self.count += 1
            return member(*args)

        return call


def hand_out(monkeypatch, *wrappers):
    """Give the next calls of ``_filesystem``, in order, the filesystems that ``wrappers`` wrap."""
    real, queue = gtdp.io._filesystem, list(wrappers)

    def filesystem(spark, path):
        fs, hadoop_path = real(spark, path)
        return (queue.pop(0)(fs) if queue else fs), hadoop_path

    monkeypatch.setattr(gtdp.io, "_filesystem", filesystem)


def race(spark, table, monkeypatch, a_first, b_first):
    """run-a and run-b take the lock of ``table``, interleaved at the step numbers given.

    run-a takes ``a_first`` steps, run-b ``b_first`` steps, then run-a finishes taking the lock
    (or fails), then run-b. Return who holds it, the run that the lock file names, and how many
    steps each took, while both still hold what they took.
    """
    path, lock = table
    held, steps = {}, {}
    b_parked, b_resume = threading.Event(), threading.Event()

    def take(run, stack):
        spark.sparkContext.setLocalProperty(RUN_ID_PROPERTY, run)
        try:
            stack.enter_context(table_lock(spark, path))
            held[run] = True
        except TableLockedError:
            held[run] = False

    def run_b():
        try:
            take("run-b", b_holds)
        finally:
            b_parked.set()

    b = threading.Thread(target=run_b)

    def start_b():
        if b.ident is None:
            b.start()
            assert b_parked.wait(60)

    def park_b():
        b_parked.set()
        assert b_resume.wait(60)

    def steps_of(run, at, pause):
        def wrap(fs):
            steps[run] = Steps(fs, at, pause)
            return steps[run]

        return wrap

    with monkeypatch.context() as patch, ExitStack() as a_holds, ExitStack() as b_holds:
        hand_out(patch, steps_of("run-a", a_first, start_b), steps_of("run-b", b_first, park_b))
        take("run-a", a_holds)
        counts = [steps["run-a"].count]
        start_b()
        b_resume.set()
        b.join(60)
        counts.append(steps["run-b"].count)
        named = json.loads(lock.read_text())["run_id"] if lock.exists() else None
    return held, named, counts


def test_two_runs_taking_over_a_dead_run_lock_never_both_hold_it(spark, table, monkeypatch):
    _, lock = table
    wrong, most_steps = [], 0
    for a_first, b_first in itertools.product(SPLITS, SPLITS):
        lock.write_text(lock_of("local-1795715756506"))
        make_old(lock)

        held, named, counts = race(spark, table, monkeypatch, a_first, b_first)

        holders = [run for run, holds in held.items() if holds]
        leftovers = sorted(os.listdir(lock.parent))
        if len(holders) != 1 or named != holders[0] or leftovers:
            wrong.append((a_first, b_first, holders, named, leftovers))
        most_steps = max(most_steps, *counts)
        for leftover in lock.parent.iterdir():
            leftover.unlink()

    assert wrong == []
    assert most_steps < len(SPLITS), "a taker takes more steps than SPLITS covers"


class Killed(BaseException):
    """The process of the taker died: it makes no more step."""


def die():
    raise Killed


def test_a_taker_killed_at_any_step_never_blocks_the_next_run_for_good(spark, table, monkeypatch):
    path, lock = table
    blocked, most_left = [], 0
    for at in SPLITS:
        lock.write_text(lock_of("local-1795715756506"))
        make_old(lock)
        with monkeypatch.context() as patch, suppress(Killed):
            hand_out(patch, lambda fs, at=at: Steps(fs, at, die))
            with table_lock(spark, path):
                pass
        left = [p for p in lock.parent.iterdir() if not p.name.endswith(".crc")]
        most_left = max(most_left, len(left))
        for leftover in lock.parent.iterdir():
            make_old(leftover)

        try:
            with table_lock(spark, path):
                pass
        except TableLockedError:
            blocked.append((at, "locked"))
        if leftovers := sorted(os.listdir(lock.parent)):
            blocked.append((at, leftovers))
            for leftover in lock.parent.iterdir():
                leftover.unlink()

    assert blocked == []
    assert most_left > 1, "no taker died leaving more than the lock behind"
