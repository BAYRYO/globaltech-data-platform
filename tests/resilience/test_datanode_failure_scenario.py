"""scripts/resilience/datanode-failure.sh against the fake cluster of conftest.py.

The first three tests come from review B of PR #24 (B1, B2, and its control); the others stop the
scenario early in each phase that changes the cluster."""

import signal
import subprocess
import time

SCRIPT = "datanode-failure.sh"
SPARE_STOPPED_WHILE_EXCLUDED = (
    "[[ -e $S/created/gtdp-datanode4-1 && ! -e $S/running/gtdp-datanode4-1 ]]"
    ' && grep -qx datanode4 "$S/exclude"'
)
DECOMMISSION_REQUESTED = 'grep -qx datanode4 "$S/exclude"'
SPARE_UP_VICTIM_DOWN = "[[ -e $S/running/gtdp-datanode4-1 && ! -e $S/running/gtdp-datanode1-1 ]]"


def retired(cluster) -> bool:
    return not cluster.created("datanode4") and not cluster.spare_volume and cluster.exclude == ""


def test_harness_happy_path_reaches_nominal(cluster):
    result = cluster.run(SCRIPT, "run1")

    assert "==> resilience scenario OK" in result.stdout, result.stdout + result.stderr
    assert result.returncode == 0
    assert (cluster.state / "exclude").read_text() == "\n"
    assert not cluster.running("datanode4")
    assert retired(cluster)


def test_an_early_stop_after_the_spare_is_stopped_takes_it_out_of_the_exclude_file(cluster):
    """B1: a DataNode that reuses the stopped spare's address while the exclude file names it is
    decommissioned; the poller dies between `docker stop datanode4` and the exclude clearing."""
    result = cluster.run(SCRIPT, "run1", POLLER_DIES_WHEN=SPARE_STOPPED_WHILE_EXCLUDED)

    assert result.returncode != 0
    assert not cluster.running("datanode4")
    assert (cluster.state / "exclude").read_text().strip() == "", (
        "scenario exited with the stopped spare still in dfs.exclude:\n" + result.stderr
    )
    assert retired(cluster)


def test_a_rerun_after_an_early_stop_during_the_decommission_converges(cluster):
    """B2: a `dfsadmin -report` failure while datanode4 decommissions ends the run with 4
    DataNodes; the rerun finishes the retirement, then runs the scenario."""
    first = cluster.run(SCRIPT, "run1", REPORT_FAILS_WHEN=DECOMMISSION_REQUESTED)
    assert first.returncode != 0
    assert cluster.running("datanode4")

    second = cluster.run(SCRIPT, "run2")

    assert second.returncode == 0, second.stdout[-600:] + second.stderr[-600:]
    assert retired(cluster)


def test_an_early_stop_with_the_spare_running_leaves_it_decommissioning(cluster):
    first = cluster.run(SCRIPT, "run1", POLLER_DIES_WHEN=SPARE_UP_VICTIM_DOWN)

    assert first.returncode != 0
    assert cluster.running("datanode1"), first.stderr
    assert cluster.running("datanode4")
    assert cluster.exclude == "datanode4"
    assert "finishes the retirement first" in first.stderr

    second = cluster.run(SCRIPT, "run2")

    assert second.returncode == 0, second.stdout[-600:] + second.stderr[-600:]
    assert cluster.events("run2")[:2] == ["spare_decommissioned", "spare_stopped"]
    assert retired(cluster)


def test_a_stop_during_the_replay_starts_the_victim_again(cluster):
    run = subprocess.Popen(
        cluster.command(SCRIPT, "run1"),
        cwd=cluster.root,
        env=cluster.environ(GOLD_SECONDS="1 60"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    events = cluster.root / "output" / "run1" / "events.csv"
    deadline = time.monotonic() + 60
    while not (events.exists() and "victim_stopped" in events.read_text()):
        assert time.monotonic() < deadline and run.poll() is None, run.communicate()
        time.sleep(0.2)

    run.send_signal(signal.SIGINT)
    _, err = run.communicate(timeout=60)

    assert run.returncode != 0
    assert cluster.running("datanode1"), err
    assert not cluster.created("datanode4")


def test_a_stopped_spare_left_by_an_earlier_run_is_removed_first(cluster):
    (cluster.state / "created" / "gtdp-datanode4-1").touch()
    (cluster.state / "volume").touch()

    result = cluster.run(SCRIPT, "run1")

    assert result.returncode == 0, result.stdout[-600:] + result.stderr[-600:]
    assert cluster.events("run1")[:3] == ["spare_dead", "spare_removed", "start"]
    assert retired(cluster)


def test_an_exclude_file_naming_another_host_stops_the_run_before_any_change(cluster):
    (cluster.state / "exclude").write_text("datanode2\n")

    result = cluster.run(SCRIPT, "run1")

    assert result.returncode != 0
    assert "finish that decommission first" in result.stderr
    assert cluster.exclude == "datanode2"
    assert not (cluster.state / "log").exists()
