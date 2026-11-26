"""The streaming-sensors job through run.py: it stops by itself and writes a SUCCESS audit row."""

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import fields
from pathlib import Path

from stream_helpers import JOB, drop_files, event, lake, local_path

from gtdp.config import LakePaths

FILES = [
    [event("E1", "00:00:10", 60.0), event("E2", "00:00:20", 90.0), "not json"],
    [event("E3", "00:03:00", 61.0), event("E1", "00:00:10", 60.0)],
    [event("E4", "00:03:10", 62.0)],
]
LINES = sum(len(lines) for lines in FILES)


def job_command(root: Path, *overrides: str) -> list[str]:
    lake_args = [
        arg
        for area in fields(LakePaths)
        for arg in ("--set", f"paths.{area.name}={root / area.name}")
    ]
    settings = ["streaming.trigger_interval_s=1", "spark.shuffle_partitions=2", *overrides]
    return [
        sys.executable,
        "-m",
        "gtdp.jobs.run",
        JOB,
        "--env",
        "local",
        *lake_args,
        *(arg for setting in settings for arg in ("--set", setting)),
    ]


def clean_env() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if not k.startswith("GTDP_")}


def audit_row(spark, root: Path) -> dict:
    [row] = spark.read.parquet(str(root / "ops" / "audit")).where(f"job_name = '{JOB}'").collect()
    return {**row.asDict(), "metrics": json.loads(row.metrics_json)}


def test_the_job_stops_after_idle_triggers_and_audits_its_counts(spark, tmp_path):
    root = tmp_path / "lake"
    paths, _ = lake(root)
    drop_files(paths, FILES)

    done = subprocess.run(
        job_command(root, "streaming.stop_after_idle_triggers=3"),
        capture_output=True,
        text=True,
        env=clean_env(),
        timeout=300,
        check=False,
    )

    assert done.returncode == 0, done.stderr[-3000:]
    row = audit_row(spark, root)
    assert row["status"] == "SUCCESS"
    assert row["metrics"]["stop_reason"] == "idle"
    assert (row["rows_in"], row["rows_rejected"], row["rows_duplicated"]) == (LINES, 1, 1)
    # One alert (E2 at 90) and one final window (00:00, closed by the 00:03:10 event).
    assert row["rows_out"] == 2


def start_running_job(tmp_path: Path) -> tuple[subprocess.Popen, Path, Path]:
    """Start the job without an idle stop; return once its first micro-batch is committed."""
    root = tmp_path / "lake"
    paths, _ = lake(root)
    drop_files(paths, FILES)
    commits = local_path(paths.checkpoint("aggregates")) / "commits"
    log = tmp_path / "job.log"
    with log.open("w") as stderr:
        job = subprocess.Popen(
            job_command(root), stdout=subprocess.DEVNULL, stderr=stderr, env=clean_env()
        )
    deadline = time.monotonic() + 240
    while not (commits.is_dir() and any(commits.iterdir())):
        assert job.poll() is None, log.read_text()[-3000:]
        assert time.monotonic() < deadline, "the job committed no batch"
        time.sleep(0.5)
    return job, root, log


def test_a_stop_marker_ends_the_job_between_batches(spark, tmp_path):
    job, root, log = start_running_job(tmp_path)
    marker = local_path(lake(root)[0].stop_marker)

    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()
    job.wait(timeout=120)

    assert job.returncode == 0, log.read_text()[-3000:]
    assert audit_row(spark, root)["metrics"]["stop_reason"] == "stop marker"
    assert not marker.exists()


def test_sigterm_to_the_driver_stops_the_job_with_a_stopped_audit_row_and_its_counts(
    spark, tmp_path
):
    job, root, log = start_running_job(tmp_path)

    job.send_signal(signal.SIGTERM)
    job.wait(timeout=120)

    assert job.returncode == 143, log.read_text()[-3000:]
    row = audit_row(spark, root)
    assert row["status"] == "STOPPED"
    # The three files fit in the first micro-batch, committed before the signal.
    assert (row["rows_in"], row["rows_rejected"], row["rows_duplicated"]) == (LINES, 1, 1)
    assert row["rows_out"] is not None
    assert row["metrics"]["stop_reason"] == "signal SIGTERM"
