"""scripts/resilience/gold-write-under-failure.sh against the fake cluster of conftest.py."""

import signal
import subprocess
import time
from datetime import datetime

SCRIPT = "gold-write-under-failure.sh"


def test_the_datanode_stops_at_the_first_fact_sales_file_and_returns_after_the_job(cluster):
    result = cluster.run(SCRIPT, "write")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "identical to /datalake/gold" in result.stdout
    events = cluster.events("write")
    order = ["fact_sales_first_file", "victim_stopped", "job_exit_0", "victim_started"]
    assert [event for event in events if event in order] == order
    assert events[-1] == "scratch_deleted"
    assert cluster.running("datanode1")


def test_the_run_waits_for_the_copies_still_asked_after_the_datanode_returns(cluster):
    """fsck counts a block whose third copy is asked and not yet made as under-replicated; the
    NameNode counter ``Under replicated blocks`` does not (49 such blocks at the end of the
    second exercise of 2026-11-27)."""
    restarted_lately = '(( $(date +%s) - $(stat -c %Y "$S/log") < 12 ))'
    pending = f'grep -q "^start gtdp-datanode1-1" "$S/log" && {restarted_lately}'
    result = cluster.run(SCRIPT, "write", PENDING_WHEN=pending)

    assert result.returncode == 0, result.stdout + result.stderr
    stamps = dict(
        reversed(line.split(",", 1))
        for line in (cluster.root / "output" / "write" / "events.csv").read_text().splitlines()
    )
    waited = datetime.fromisoformat(stamps["replicated"]) - datetime.fromisoformat(
        stamps["victim_started"]
    )
    assert waited.total_seconds() >= 11


def test_a_stop_while_the_datanode_is_down_starts_it_again(cluster):
    run = subprocess.Popen(
        cluster.command(SCRIPT, "write"),
        cwd=cluster.root,
        env=cluster.environ(GOLD_SECONDS="60"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    events = cluster.root / "output" / "write" / "events.csv"
    deadline = time.monotonic() + 60
    while not (events.exists() and "victim_stopped" in events.read_text()):
        assert time.monotonic() < deadline and run.poll() is None, run.communicate()
        time.sleep(0.2)
    assert not cluster.running("datanode1")

    run.send_signal(signal.SIGTERM)
    _, err = run.communicate(timeout=60)

    assert run.returncode != 0
    assert cluster.running("datanode1"), err
    assert "delete the scratch area" in err
