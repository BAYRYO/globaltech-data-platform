"""Taking a table lock over: the YARN state of the run that holds it, its age, racing takers."""

import json
import logging
import os
import signal
import socket
import threading
import time
from contextlib import suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest
from pyspark import SparkContext

import gtdp.io
from gtdp.audit import RunStopped
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

    ``states`` maps an application id to the state it reports, to the ``app`` object it answers,
    or to the raw body it answers. Any other id is unknown (404). The next ``failures`` requests
    get 503. ``requests`` lists the paths it was asked for.
    """
    rm = SimpleNamespace(states={}, requests=[], failures=0)

    class Api(BaseHTTPRequestHandler):
        def do_GET(self):
            rm.requests.append(self.path)
            application_id = self.path.rsplit("/", 1)[-1]
            answer = rm.states.get(application_id)
            if rm.failures:
                rm.failures -= 1
                status, answer = 503, b"<html>Service Unavailable</html>"
            elif answer is None:
                status, body = 404, {"RemoteException": {"exception": "NotFoundException"}}
            elif isinstance(answer, dict):
                status, body = 200, {"app": answer}
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


# A run takes and releases a lock in fewer steps than this: calls on the lock and on files named
# after it, and writes and closes of the streams that create them.
STEPS = range(40)


class Killed(BaseException):
    """The process of the run died."""


class Mortal:
    """The filesystem of a run that dies at its step number ``at``: no call goes through after."""

    def __init__(self, fs, at):
        self._fs, self._at, self.steps, self.dead = fs, at, 0, False

    def step(self):
        if self.dead or self.steps == self._at:
            self.dead = True
            raise Killed
        self.steps += 1

    def __getattr__(self, name):
        member = getattr(self._fs, name)

        def call(*args):
            if self.dead:
                raise Killed
            if not (args and ".lock" in str(args[0]).rsplit("/", 1)[-1]):
                return member(*args)
            self.step()
            result = member(*args)
            return MortalStream(result, self) if name == "create" else result

        return call


class MortalStream:
    def __init__(self, stream, fs):
        self._stream, self._fs = stream, fs

    def write(self, data):
        self._fs.step()
        self._stream.write(data)

    def close(self):
        self._fs.step()
        self._stream.close()


def hand_out(monkeypatch, *wrappers):
    """Give the next calls of ``_filesystem``, in order, the filesystems that ``wrappers`` wrap."""
    real, queue = gtdp.io._filesystem, list(wrappers)

    def filesystem(spark, path):
        fs, hadoop_path = real(spark, path)
        return (queue.pop(0)(fs) if queue else fs), hadoop_path

    monkeypatch.setattr(gtdp.io, "_filesystem", filesystem)


def test_a_run_killed_at_any_step_never_blocks_the_next_one_once_yarn_says_it_ended(
    spark, table, resource_manager, monkeypatch
):
    """No file is aged: the next run takes the lock at once, whatever step the killed run was at,
    a stream it had not closed yet included."""
    path, lock = table
    killed = "application_1795715756506_0008"
    resource_manager.states.update({APP: "FINISHED", killed: "FINISHED"})
    blocked, most_steps, dying = [], 0, []
    for at in STEPS:
        lock.write_text(lock_of(APP))  # left by a run that died
        dying.clear()
        with monkeypatch.context() as patch, suppress(Killed):
            patch.setattr(SparkContext, "applicationId", property(lambda _self: killed))
            hand_out(patch, lambda fs, at=at: dying.append(Mortal(fs, at)) or dying[-1])
            with table_lock(spark, path):
                pass
        most_steps = max(most_steps, dying[0].steps)

        try:
            with table_lock(spark, path):
                pass
        except TableLockedError as exc:
            blocked.append((at, str(exc)))
        left = sorted(p.name for p in lock.parent.iterdir() if not p.name.startswith("."))
        if [name for name in left if not name.startswith("_orders.lock.")]:
            blocked.append((at, left))
        for leftover in lock.parent.iterdir():
            leftover.unlink()

    assert blocked == []
    assert most_steps < STEPS[-1], "the run always died: STEPS does not cover its steps"


class Gate:
    """The filesystem of one run, which waits before its first delete of the lock file."""

    def __init__(self):
        self.fs, self.armed = None, True
        self.reached, self.resume = threading.Event(), threading.Event()

    def delete(self, path, recursive):
        if self.armed and path.getName().endswith(".lock"):
            self.armed = False
            self.reached.set()
            assert self.resume.wait(60)
        return self.fs.delete(path, recursive)

    def __getattr__(self, name):
        return getattr(self.fs, name)


def gate(monkeypatch, *runs):
    """Give each thread named in ``runs`` a ``Gate`` around the filesystem ``gtdp.io`` uses."""
    real, gates = gtdp.io._filesystem, {run: Gate() for run in runs}

    def filesystem(spark, path):
        fs, hadoop_path = real(spark, path)
        found = gates.get(threading.current_thread().name)
        if found is None:
            return fs, hadoop_path
        found.fs = fs
        return found, hadoop_path

    monkeypatch.setattr(gtdp.io, "_filesystem", filesystem)
    return gates


class Run(threading.Thread):
    """A run that takes the lock of ``path`` and holds it until ``leave`` is set."""

    def __init__(self, spark, path, name):
        super().__init__(name=name)
        self.spark, self.path, self.outcome = spark, path, None
        self.inside, self.leave = threading.Event(), threading.Event()

    def run(self):
        self.spark.sparkContext.setLocalProperty(RUN_ID_PROPERTY, self.name)
        try:
            with table_lock(self.spark, self.path):
                self.outcome = "held"
                self.inside.set()
                assert self.leave.wait(60)
        except TableLockedError:
            self.outcome = "locked"
        finally:
            self.inside.set()


def until(condition):
    deadline = time.monotonic() + 60
    while not condition():
        assert time.monotonic() < deadline
        time.sleep(0.01)


def test_the_release_of_a_run_taken_for_dead_never_deletes_the_lock_of_its_taker(
    spark, table, monkeypatch
):
    """run-x is alive but outlives staleAfterSeconds and YARN gives no state (local mode): the age
    rule takes it for dead, the documented limit. run-b takes its lock over while run-x
    releases. run-x must leave run-b's lock alone, so that no third run walks in."""
    path, lock = table
    gates = gate(monkeypatch, "run-x", "run-b")
    x, b = Run(spark, path, "run-x"), Run(spark, path, "run-b")
    x.start()
    assert x.inside.wait(60)
    make_old(lock)
    b.start()
    assert gates["run-b"].reached.wait(60)  # run-b claimed run-x's lock, is about to delete it
    x.leave.set()
    until(lambda: gates["run-x"].reached.is_set() or not x.is_alive())
    gates["run-b"].resume.set()
    assert b.inside.wait(60)
    assert b.outcome == "held"
    gates["run-x"].resume.set()
    x.join(60)
    try:
        assert lock.exists(), "run-x deleted the lock that run-b holds"
        assert json.loads(lock.read_text())["run_id"] == "run-b"
        with pytest.raises(TableLockedError), table_lock(spark, path):
            pass
    finally:
        b.leave.set()
        b.join(60)


def test_a_taker_and_a_run_that_takes_the_released_lock_never_both_hold_it(
    spark, table, monkeypatch
):
    """Same run-x. run-b has checked that the lock still holds what it judged dead when run-x
    releases and run-a tries the lock: one of run-a and run-b holds it, never both."""
    path, lock = table
    gates = gate(monkeypatch, "run-b")
    x, b, a = (Run(spark, path, name) for name in ("run-x", "run-b", "run-a"))
    x.start()
    assert x.inside.wait(60)
    make_old(lock)
    b.start()
    assert gates["run-b"].reached.wait(60)  # run-b is between its check and its delete
    x.leave.set()
    x.join(60)
    a.start()
    assert a.inside.wait(60)
    gates["run-b"].resume.set()
    assert b.inside.wait(60)
    holders = [run.name for run in (a, b) if run.outcome == "held"]
    named = json.loads(lock.read_text())["run_id"] if lock.exists() else None
    for run in (a, b):
        run.leave.set()
        run.join(60)

    assert len(holders) == 1
    assert named == holders[0]


def test_a_stop_signal_received_while_taking_the_lock_still_releases_it(spark, table, monkeypatch):
    """The bronze runbook: the lock goes at the end of the write, also on failure or stop signal."""
    path, lock = table
    real = gtdp.io._acquire

    def stopped_while_acquiring(*args):
        os.kill(os.getpid(), signal.SIGTERM)  # for example while YARN is asked
        return real(*args)

    def raise_stopped(signum, _frame):
        raise RunStopped(signal.Signals(signum))

    monkeypatch.setattr(gtdp.io, "_acquire", stopped_while_acquiring)
    previous = signal.signal(signal.SIGTERM, raise_stopped)
    try:
        with pytest.raises(RunStopped), table_lock(spark, path):
            pass
    finally:
        signal.signal(signal.SIGTERM, previous)

    assert not lock.exists()


class FailedReads:
    """A filesystem on which the lock file cannot be opened ``times`` times (None: always)."""

    def __init__(self, fs, times):
        self._fs, self._left = fs, times

    def open(self, path):
        if path.getName().endswith(".lock") and self._left != 0:
            self._left = None if self._left is None else self._left - 1
            path = path.suffix(".unreachable")  # the NameNode does not answer
        return self._fs.open(path)

    def __getattr__(self, name):
        return getattr(self._fs, name)


def test_a_release_that_cannot_read_its_lock_once_reads_it_again_and_releases_it(
    spark, table, resource_manager, monkeypatch, caplog
):
    """The application stays RUNNING: a lock it kept would refuse it for the rest of its life."""
    path, lock = table
    monkeypatch.setattr(SparkContext, "applicationId", property(lambda _self: APP))
    resource_manager.states[APP] = "RUNNING"

    with monkeypatch.context() as patch, caplog.at_level(logging.WARNING, logger="gtdp.io"):
        hand_out(patch, lambda fs: FailedReads(fs, 1))
        with table_lock(spark, path):
            pass

    assert not lock.exists()
    assert caplog.text == ""
    with table_lock(spark, path):  # the same application, later
        pass


def test_a_release_that_never_reads_its_lock_keeps_it_and_says_how_to_free_it(
    spark, table, monkeypatch, caplog
):
    path, lock = table
    monkeypatch.setattr(gtdp.io, "_RETRY_PAUSES_S", (0.0, 0.0), raising=False)

    with monkeypatch.context() as patch, caplog.at_level(logging.WARNING, logger="gtdp.io"):
        hand_out(patch, lambda fs: FailedReads(fs, None))
        with table_lock(spark, path):
            pass

    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert lock.exists()
    assert len(errors) == 1
    assert str(lock) in errors[0]
    assert "hdfs dfs -rm" in errors[0]


def test_one_failed_request_to_yarn_never_frees_the_old_lock_of_a_running_application(
    spark, table, resource_manager, monkeypatch
):
    """From the review: a RUNNING application holds its lock whatever its age, yet a single
    failed request to the ResourceManager handed the lock to the next run."""
    path, lock = table
    lock.write_text(lock_of(APP))
    make_old(lock)
    resource_manager.states[APP] = "RUNNING"
    resource_manager.failures = 1
    monkeypatch.setattr(gtdp.io, "_RETRY_PAUSES_S", (0.0, 0.0))

    with pytest.raises(TableLockedError, match=APP), table_lock(spark, path):
        pass

    assert lock.read_text() == lock_of(APP)
    assert resource_manager.requests == [f"/ws/v1/cluster/apps/{APP}"] * 2
