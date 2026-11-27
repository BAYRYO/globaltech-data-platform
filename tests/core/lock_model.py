"""Model checker of the table lock: every interleaving of the runs that take or release it.

The runs drive the real ``gtdp.io._locked`` over an in-memory filesystem. Each filesystem call,
each close of a written stream, and each rename is one atomic step. A run is a thread that waits
at every step for the checker to let it go on. The checker explores every order of the steps, depth
first, and skips a state it has already seen (files, clock, and the history of each run).

Two properties hold in every state:

- **exclusion**: no two runs are inside the lock at once. A ``slow`` run, which holds the lock
  past ``staleAfterSeconds`` while YARN gives no state, does not count: the age rule takes it for
  dead by design (ADR-0002).
- **ownership**: a run deletes a file that holds the content of another run still alive only if
  it judged that file left by a dead run (``_stale_reason``).

Once every run is done, dead or holding, no run that ended has left a file, some run got the lock,
and a ``last`` run, started once the others were done or dead, got it.

The clock moves once, when the ``slow`` run outlives ``staleAfterSeconds``, and only while no
other run is between two steps of a taking or a release: only the ``slow`` run stalls that long.

``python tests/core/lock_model.py [scenario ...]`` explores the scenarios and prints their counts.
"""

from __future__ import annotations

import json
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from py4j.protocol import Py4JJavaError

from gtdp import io

STALE = 100.0
OLD = STALE * 10
LOCK = "/lake/_orders.lock"
TABLE = "/lake/orders"
EXISTS = "org.apache.hadoop.fs.FileAlreadyExistsException"
NOT_FOUND = "java.io.FileNotFoundException"
DEAD_APP, A_APP, B_APP, C_APP, D_APP, E_APP = (f"application_1_000{i}" for i in range(1, 7))


class JavaError(Py4JJavaError):
    def __init__(self, name: str) -> None:
        self.name = name
        self.java_exception = SimpleNamespace(
            getClass=lambda: SimpleNamespace(getName=lambda: name)
        )

    def __str__(self) -> str:
        return self.name


class P:
    """A Hadoop ``Path``, as far as ``gtdp.io`` uses it."""

    def __init__(self, s: str) -> None:
        self.s = s

    def suffix(self, text: str) -> P:
        return P(self.s + text)

    def getName(self) -> str:
        return self.s.rsplit("/", 1)[-1]

    def getParent(self) -> P:
        return P(self.s.rsplit("/", 1)[0])

    def toString(self) -> str:
        return self.s

    def __str__(self) -> str:
        return self.s


class Abort(BaseException):
    """Ends the thread of a run when the checker tears a schedule down."""


class World:
    def __init__(self) -> None:
        self.files: dict[str, list[Any]] = {}  # path -> [content, mtime, inode]
        self.inode = 0
        self.now = 1000.0
        self.yarn: dict[str, str] = {}
        self.inside: set[str] = set()
        self.violation: str | None = None
        self.actors: dict[str, Actor] = {}
        self.aborting = False
        self.control = threading.Semaphore(0)
        self.log: list[tuple[Any, ...]] = []

    def current(self) -> Actor | None:
        return self.actors.get(threading.current_thread().name)

    def pause(self, label: tuple[Any, ...]) -> None:
        actor = self.current()
        if actor is None or self.aborting:
            return
        actor.label = label
        self.control.release()
        actor.gate.acquire()
        if self.aborting:
            raise Abort

    def step(self, name: str) -> None:
        self.actors[name].gate.release()
        self.control.acquire()

    def record(self, *event: Any) -> None:
        actor = self.current()
        if actor is not None:
            actor.history.append(event)
        self.log.append((threading.current_thread().name, *event))

    def fail(self, message: str) -> None:
        if self.violation is None:
            self.violation = message

    def deleted(self, path: str, content: str) -> None:
        """Check the ownership property for a delete of ``path``, which held ``content``."""
        actor = self.current()
        if actor is None or content == actor.holder or (path, content) in actor.judged:
            return
        for owner in self.actors.values():
            if owner.holder == content and owner.alive():
                self.fail(f"{actor.name} deleted {path}, which {owner.name} holds")


class Fs:
    """The Hadoop ``FileSystem`` calls ``gtdp.io`` makes on a lock and its siblings."""

    def __init__(self, world: World) -> None:
        self.w = world

    def create(self, path: P, overwrite: bool) -> WriteStream:
        w = self.w
        w.pause(("create", path.s))
        if path.s in w.files and not overwrite:
            w.record("create", path.s, False)
            raise JavaError(EXISTS)
        w.inode += 1
        w.files[path.s] = ["", w.now, w.inode]
        w.record("create", path.s, True)
        return WriteStream(w, path.s, w.inode)

    def open(self, path: P) -> SimpleNamespace:
        w = self.w
        w.pause(("open", path.s))
        if path.s not in w.files:
            w.record("open", path.s, None)
            raise JavaError(NOT_FOUND)
        content = w.files[path.s][0]
        w.record("open", path.s, content)
        return SimpleNamespace(readAllBytes=lambda: content.encode(), close=lambda: None)

    def delete(self, path: P, recursive: bool) -> bool:
        w = self.w
        w.pause(("delete", path.s))
        entry = w.files.pop(path.s, None)
        w.record("delete", path.s, entry is not None)
        if entry is not None:
            w.deleted(path.s, entry[0])
        return entry is not None

    def exists(self, path: P) -> bool:
        w = self.w
        w.pause(("exists", path.s))
        w.record("exists", path.s, path.s in w.files)
        return path.s in w.files

    def getFileStatus(self, path: P) -> SimpleNamespace:
        w = self.w
        w.pause(("stat", path.s))
        if path.s not in w.files:
            w.record("stat", path.s, None)
            raise JavaError(NOT_FOUND)
        mtime = w.files[path.s][1]
        w.record("stat", path.s, mtime)
        return SimpleNamespace(getModificationTime=lambda: mtime * 1000)

    def listStatus(self, parent: P) -> list[SimpleNamespace]:
        w = self.w
        w.pause(("list", parent.s))
        found = [
            SimpleNamespace(getPath=lambda p=p: P(p), getModificationTime=lambda m=m: m * 1000)
            for p, (_, m, _i) in sorted(w.files.items())
            if p.rsplit("/", 1)[0] == parent.s
        ]
        w.record("list", parent.s, tuple(sorted(w.files)))
        return found

    def rename(self, source: P, destination: P) -> None:
        """``FileContext.rename`` without overwrite: one atomic NameNode call on HDFS."""
        w = self.w
        w.pause(("rename", source.s, destination.s))
        if source.s not in w.files:
            w.record("rename", source.s, destination.s, None)
            raise JavaError(NOT_FOUND)
        if destination.s in w.files:
            w.record("rename", source.s, destination.s, False)
            raise FileExistsError(destination.s)
        w.files[destination.s] = w.files.pop(source.s)
        w.record("rename", source.s, destination.s, True)


class WriteStream:
    def __init__(self, world: World, path: str, inode: int) -> None:
        self.w, self.path, self.data, self.inode = world, path, b"", inode

    def write(self, data: bytearray) -> None:
        self.data += bytes(data)

    def close(self) -> None:
        w = self.w
        w.pause(("close", self.path))
        # The stream writes to the inode it created. HDFS fails the close of a file deleted
        # meanwhile (its lease is gone).
        entry = w.files.get(self.path)
        if entry is None or entry[2] != self.inode:
            w.record("close", self.path, False)
            raise JavaError("org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException")
        entry[0] = self.data.decode()
        w.record("close", self.path, True)


@dataclass
class Actor:
    """A run: ``release`` takes the lock and releases it, ``hold`` keeps it for good, ``slow``
    holds it at the start and outlives ``staleAfterSeconds`` before it releases, ``mortal`` may be
    killed at any step (YARN then reports its application ended), ``plain`` keeps the lock but
    starts once the ``slow`` run is done, ``last`` starts once every other run is done or dead."""

    world: World
    name: str
    app: str
    kind: str
    gate: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(0))
    history: list[tuple[Any, ...]] = field(default_factory=list)
    judged: set[tuple[str, str]] = field(default_factory=set)
    label: tuple[Any, ...] | None = None
    done: bool = False
    dead: bool = False
    outcome: str | None = None

    @property
    def holder(self) -> str:
        return json.dumps(
            {
                "run_id": self.name,
                "application_id": self.app,
                "locked_at": FIXED.isoformat(timespec="seconds"),
                "token": f"tok-{self.name}",
            }
        )

    def alive(self) -> bool:
        return not self.done and not self.dead

    def spark(self) -> SimpleNamespace:
        return SimpleNamespace(
            sparkContext=SimpleNamespace(
                getLocalProperty=lambda _k: self.name, applicationId=self.app
            ),
            conf=SimpleNamespace(get=lambda _k, _d=None: str(STALE)),
        )

    def run(self) -> None:
        w = self.world
        self.gate.acquire()
        try:
            try:
                with io._locked(self.spark(), Fs(w), P(TABLE)):
                    self.outcome = "held"
                    if self.kind != "slow":
                        w.inside.add(self.name)
                        if len(w.inside) > 1:
                            w.fail(f"inside the lock together: {sorted(w.inside)}")
                    w.record("enter")
                    if self.kind == "slow":
                        w.pause(("slow-work",))
                        w.now += STALE + 1  # the run outlives staleAfterSeconds
                    if self.kind in ("hold", "plain"):
                        self.label = ("holding",)
                        w.control.release()
                        self.gate.acquire()
                        raise Abort
                    w.pause(("inside",))
                    w.inside.discard(self.name)
                    w.record("leave")
            except io.TableLockedError:
                self.outcome = "locked"
            except Py4JJavaError as exc:
                self.outcome = f"error {exc}"
        except Abort:
            pass
        finally:
            self.done = True
            self.label = None
            w.control.release()


FIXED = datetime(2026, 11, 27, tzinfo=UTC)
MODEL = SimpleNamespace(world=None)  # the world of the schedule being replayed


def patch(setattr_: Any) -> None:
    """Point ``gtdp.io`` at the model; ``setattr_`` is ``monkeypatch.setattr`` or ``setattr``."""

    def yarn_state(_spark: Any, application_id: str) -> str:
        assert MODEL.world is not None
        if application_id not in MODEL.world.yarn:
            raise LookupError("no state")
        return MODEL.world.yarn[application_id]

    real_stale_reason = io._stale_reason

    def stale_reason(spark: Any, fs: Any, path: P, held: str) -> str | None:
        reason = real_stale_reason(spark, fs, path, held)
        actor = MODEL.world.current() if MODEL.world else None
        if reason is not None and actor is not None:
            actor.judged.add((path.s, held))
        return reason

    def thread_token() -> SimpleNamespace:
        return SimpleNamespace(hex=f"tok-{threading.current_thread().name}")

    setattr_(io, "uuid", SimpleNamespace(uuid4=thread_token))
    setattr_(io, "datetime", SimpleNamespace(now=lambda _tz=None: FIXED))
    setattr_(
        io,
        "time",
        SimpleNamespace(time=lambda: MODEL.world.now, sleep=lambda _s: None, monotonic=lambda: 0.0),
    )
    setattr_(io, "_yarn_state", yarn_state)
    setattr_(io, "_stale_reason", stale_reason)
    setattr_(io, "_rename", lambda fs, source, destination: fs.rename(source, destination))
    setattr_(io.logger, "disabled", True)


@dataclass(frozen=True)
class Scenario:
    """Runs ``(name, application, kind)``, files ``{path: (content, age)}``, YARN states.

    ``ci``: small enough for every test run; the others run with ``python lock_model.py``.
    """

    title: str
    actors: tuple[tuple[str, str, str], ...]
    files: tuple[tuple[str, tuple[str, float]], ...] = ()
    yarn: tuple[tuple[str, str], ...] = ()
    ci: bool = False

    def world(self) -> World:
        w = World()
        w.yarn = dict(self.yarn)
        for path, (content, age) in self.files:
            w.inode += 1
            w.files[path] = [content, w.now - age, w.inode]
        for name, app, kind in self.actors:
            w.actors[name] = Actor(w, name, app, kind)
        return w


@dataclass
class Result:
    runs: int = 0
    states: int = 0
    violation: str | None = None
    schedule: list[tuple[Any, ...]] = field(default_factory=list)
    terminals: dict[tuple[Any, ...], list[tuple[Any, ...]]] = field(default_factory=dict)


def _replay(scenario: Scenario, prefix: tuple[str, ...]) -> SimpleNamespace:
    w = scenario.world()
    MODEL.world = w
    threads = [threading.Thread(target=a.run, name=a.name, daemon=True) for a in w.actors.values()]
    for actor, thread in zip(w.actors.values(), threads, strict=True):
        thread.start()
        w.step(actor.name)  # to its first step
        while actor.kind == "slow" and actor.label != ("slow-work",):
            w.step(actor.name)
    for name in prefix:
        if name.startswith("!"):
            victim = w.actors[name[1:]]
            victim.dead = True
            w.yarn[victim.app] = "FINISHED"
            w.inside.discard(victim.name)
            w.log.append((victim.name, "killed before", victim.label))
        else:
            w.step(name)
    live = [
        a
        for a in w.actors.values()
        if not a.done and not a.dead and a.label is not None and a.label != ("holding",)
    ]
    slow_done = all(a.done for a in w.actors.values() if a.kind == "slow")
    first = [a for a in live if a.kind != "last" and (a.kind != "plain" or slow_done)]
    quiet = all(
        not a.history or a.done or a.dead or a.label == ("holding",)
        for a in w.actors.values()
        if a.kind != "slow"
    )
    enabled = [a.name for a in (first or live) if quiet or a.label != ("slow-work",)]
    enabled += [f"!{a.name}" for a in first if a.kind == "mortal"]
    key = (
        tuple(sorted((p, c, m) for p, (c, m, _i) in w.files.items())),
        w.now,
        tuple((a.name, a.done, a.dead, a.label, tuple(a.history)) for a in w.actors.values()),
    )
    leaked = sorted(
        path
        for path, (content, _m, _i) in w.files.items()
        for a in w.actors.values()
        if a.holder == content and a.done and not a.dead
    )
    snapshot = SimpleNamespace(
        key=key,
        enabled=enabled,
        violation=w.violation,
        log=list(w.log),
        outcomes=tuple(
            sorted((a.name, "DEAD" if a.dead else a.outcome or "-") for a in w.actors.values())
        ),
        files=tuple(sorted(w.files)),
        leaked=leaked,
    )
    w.aborting = True
    for actor in w.actors.values():
        if not actor.done:
            actor.gate.release()
    for thread in threads:
        thread.join(5)
    return snapshot


def explore(scenario: Scenario, limit: int = 2_000_000) -> Result:
    """Run ``scenario`` through every interleaving; stop at the first violation."""
    result, seen, stack = Result(), set(), [()]
    while stack:
        prefix = stack.pop()
        state = _replay(scenario, prefix)
        result.runs += 1
        if state.violation is None and not state.enabled:
            state.violation = _stuck(scenario, state)
        if state.violation is not None:
            result.violation, result.schedule = state.violation, state.log
            break
        if state.key in seen:
            continue
        seen.add(state.key)
        if not state.enabled:
            result.terminals.setdefault((state.outcomes, state.files), state.log)
        stack.extend((*prefix, name) for name in state.enabled)
        if result.runs > limit:
            raise RuntimeError(f"{scenario.title}: more than {limit} runs")
    result.states = len(seen)
    return result


def _stuck(scenario: Scenario, state: SimpleNamespace) -> str | None:
    """What is wrong once every run is done, dead or holding: a run that ended left a file, no
    run got the lock, or a ``last`` run, started once the others were done or dead, did not."""
    outcomes = dict(state.outcomes)
    if state.leaked:
        return f"a run that ended left {state.leaked}"
    if "held" not in outcomes.values():
        return "no run got the lock"
    last = [name for name, _app, kind in scenario.actors if kind == "last"]
    if any(outcomes[name] != "held" for name in last):
        return f"{last} did not get the lock: {state.files}"
    return None


def lock_of(app: str, token: str = "dead") -> str:
    return json.dumps(
        {"run_id": "run-dead", "application_id": app, "locked_at": "x", "token": token}
    )


def _claim(content: str) -> str:
    import hashlib

    return f"{LOCK}.takeover-{hashlib.sha256(content.encode()).hexdigest()[:16]}"


_DEAD_LOCK = lock_of(DEAD_APP)
_DEAD_TAKER = lock_of(D_APP, "taker")
_YARN = (
    (DEAD_APP, "FINISHED"),
    (D_APP, "FINISHED"),
    (A_APP, "RUNNING"),
    (B_APP, "RUNNING"),
    (C_APP, "RUNNING"),
    (E_APP, "RUNNING"),
)

_DEAD_AGE = ((LOCK, (lock_of("local-0"), OLD)),)
_DEAD_YARN = ((LOCK, (_DEAD_LOCK, 0)),)

SCENARIOS = {
    s.title: s
    for s in (
        Scenario(
            "age: two runs take over a dead lock and keep it",
            (("A", "local-1", "hold"), ("B", "local-1", "hold")),
            _DEAD_AGE,
            ci=True,
        ),
        Scenario(
            "age: two runs take over a dead lock and release it",
            (("A", "local-1", "release"), ("B", "local-1", "release")),
            _DEAD_AGE,
        ),
        Scenario(
            "age: a live holder outlives staleAfterSeconds and releases, one run takes",
            (("X", "local-1", "slow"), ("A", "local-1", "release")),
            ci=True,
        ),
        Scenario(
            "age: a live holder outlives staleAfterSeconds and releases, two runs take",
            (("X", "local-1", "slow"), ("A", "local-1", "hold"), ("B", "local-1", "hold")),
        ),
        Scenario(
            "age: a live holder outlives staleAfterSeconds and releases, two runs take, one comes "
            "once it is done",
            (
                ("X", "local-1", "slow"),
                ("A", "local-1", "hold"),
                ("B", "local-1", "hold"),
                ("C", "local-1", "plain"),
            ),
        ),
        Scenario(
            "age: an empty dead lock, two runs",
            (("A", "local-1", "release"), ("B", "local-1", "release")),
            ((LOCK, ("", OLD)),),
        ),
        Scenario(
            "yarn: two runs take over a dead lock and release it",
            (("A", A_APP, "release"), ("B", B_APP, "release")),
            _DEAD_YARN,
            _YARN,
        ),
        Scenario(
            "yarn: three runs take over a dead lock and keep it",
            (("A", A_APP, "hold"), ("B", B_APP, "hold"), ("C", C_APP, "hold")),
            _DEAD_YARN,
            _YARN,
        ),
        Scenario(
            "yarn: a RUNNING holder outlives staleAfterSeconds and releases, one run takes",
            (("X", A_APP, "slow"), ("A", B_APP, "release")),
            (),
            _YARN,
            ci=True,
        ),
        Scenario(
            "yarn: a RUNNING holder outlives staleAfterSeconds and releases, three runs take",
            (
                ("X", A_APP, "slow"),
                ("A", B_APP, "hold"),
                ("B", C_APP, "hold"),
                ("C", E_APP, "hold"),
            ),
            (),
            _YARN,
        ),
        Scenario(
            "yarn: three runs of one application take over a dead lock and keep it",
            (("A", A_APP, "hold"), ("B", A_APP, "hold"), ("C", A_APP, "hold")),
            _DEAD_YARN,
            _YARN,
        ),
        Scenario(
            "yarn: a dead lock and the claim of a dead taker, two runs",
            (("A", A_APP, "release"), ("B", B_APP, "release")),
            ((LOCK, (_DEAD_LOCK, 0)), (_claim(_DEAD_LOCK), (_DEAD_TAKER, 0))),
            _YARN,
        ),
        Scenario(
            "yarn: the claim of a dead taker alone, two runs",
            (("A", A_APP, "release"), ("B", B_APP, "release")),
            ((_claim(_DEAD_LOCK), (_DEAD_TAKER, 0)),),
            _YARN,
            ci=True,
        ),
        Scenario(
            "yarn: the lock of a dead taker and its claim, two runs",
            (("A", A_APP, "release"), ("B", B_APP, "release")),
            ((LOCK, (_DEAD_TAKER, 0)), (_claim(_DEAD_LOCK), (_DEAD_TAKER, 0))),
            _YARN,
        ),
        Scenario(
            "yarn: a run killed at any step, then a new run",
            (("A", A_APP, "mortal"), ("Z", C_APP, "last")),
            _DEAD_YARN,
            _YARN,
            ci=True,
        ),
        Scenario(
            "yarn: a run killed at any step while another takes, then a new run",
            (("A", A_APP, "mortal"), ("B", B_APP, "release"), ("Z", C_APP, "last")),
            _DEAD_YARN,
            _YARN,
        ),
    )
}


def main(titles: list[str]) -> int:
    patch(setattr)
    failed = 0
    for title, scenario in SCENARIOS.items():
        if titles and not any(t in title for t in titles):
            continue
        started = time.perf_counter()
        result = explore(scenario)
        seconds = time.perf_counter() - started
        print(
            f"{title}: {result.runs} runs, {result.states} states, "
            f"{len(result.terminals)} ends, {seconds:.0f} s"
        )
        for outcomes, files in sorted(result.terminals, key=str):
            print(f"    end: {dict(outcomes)} files: {list(files)}")
        if result.violation:
            failed += 1
            print(f"    VIOLATION: {result.violation}")
            for event in result.schedule:
                print(f"      {event}")
    return failed


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
