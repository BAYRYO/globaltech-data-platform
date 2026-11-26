import json
import os
import shutil
import signal
import subprocess
import sys
import time
import zipfile
from dataclasses import fields
from datetime import date
from pathlib import Path

import pytest

from gtdp.config import LakePaths
from gtdp.jobs.run import main

REPO = Path(__file__).resolve().parents[2]
RUN_PY = REPO / "src" / "gtdp" / "jobs" / "run.py"


def clean_env(**extra):
    env = {k: v for k, v in os.environ.items() if not k.startswith("GTDP_")}
    return {**env, **extra}


def lake_args(tmp_path):
    """run.py options that put every lake area under ``tmp_path / "lake"``."""
    lake = tmp_path / "lake"
    return [
        arg
        for area in fields(LakePaths)
        for arg in ("--set", f"paths.{area.name}={lake / area.name}")
    ]


def run_module(*args, env=None):
    return subprocess.run(
        [sys.executable, "-m", "gtdp.jobs.run", *args],
        capture_output=True,
        text=True,
        env=env or clean_env(),
        timeout=300,
        check=False,
    )


def json_lines(text):
    return [json.loads(line) for line in text.splitlines() if line.startswith("{")]


def raw_tracebacks(text):
    return [line for line in text.splitlines() if line.startswith("Traceback")]


def test_list_prints_every_job_with_its_layer(capsys):
    assert main(["list"]) == 0

    [smoke] = [line for line in capsys.readouterr().out.splitlines() if line.startswith("smoke ")]
    assert "ops" in smoke.split()


def test_unknown_job_fails_and_names_the_available_jobs(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["nope", "--env", "local"])

    assert exit_info.value.code == 2
    error = capsys.readouterr().err
    assert "'nope'" in error
    assert "'smoke'" in error


@pytest.mark.parametrize(
    "argv",
    [
        ["smoke"],
        ["smoke", "--env", "prod"],
        ["smoke", "--env", "local", "--date", "02/01/2026"],
        ["smoke", "--env", "local", "--set", "spark.adaptive"],
    ],
    ids=["no-env", "unknown-env", "bad-date", "set-without-value"],
)
def test_invalid_arguments_exit_with_usage_error(argv, capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(argv)

    assert exit_info.value.code == 2


def test_invalid_configuration_exits_before_spark_starts(stderr_lines):
    assert main(["smoke", "--env", "local", "--set", "spark.shuffle_partitions=many"]) == 2

    [entry] = stderr_lines()
    assert entry["level"] == "ERROR"
    assert entry["job"] == "smoke"
    assert "spark.shuffle_partitions" in entry["message"]


@pytest.mark.parametrize("setting", ["app.timezone=Mars/Phobos", "spark.master=nope"])
def test_an_unknown_timezone_or_master_exits_2_before_spark_starts(tmp_path, setting):
    result = run_module("smoke", "--env", "local", *lake_args(tmp_path), "--set", setting)

    assert result.returncode == 2
    [error] = [entry for entry in json_lines(result.stderr) if entry["level"] == "ERROR"]
    assert setting.partition("=")[0].partition(".")[2] in error["message"]
    assert not (tmp_path / "lake").exists()


def test_spark_that_cannot_start_exits_2_with_a_json_error(tmp_path):
    result = run_module(
        "smoke", "--env", "local", *lake_args(tmp_path), env=clean_env(JAVA_HOME="/nonexistent")
    )

    assert result.returncode == 2
    [error] = [entry for entry in json_lines(result.stderr) if entry["level"] == "ERROR"]
    assert error["message"] == "could not start Spark"
    assert raw_tracebacks(result.stderr) == []


def test_runs_as_a_module():
    result = subprocess.run(
        [sys.executable, "-m", "gtdp.jobs.run", "list"],
        capture_output=True,
        text=True,
        env=clean_env(),
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "smoke" in result.stdout


@pytest.fixture
def cluster_like_checkout(tmp_path):
    """conf/ and dist/gtdp.zip side by side, as spark-submit sees the mounted repository."""
    checkout = tmp_path / "checkout"
    shutil.copytree(REPO / "conf", checkout / "conf")
    (checkout / "dist").mkdir()
    with zipfile.ZipFile(checkout / "dist" / "gtdp.zip", "w") as archive:
        for module in (REPO / "src" / "gtdp").rglob("*.py"):
            archive.write(module, module.relative_to(REPO / "src"))
    return checkout


@pytest.mark.slow
def test_smoke_job_runs_from_the_zip_and_writes_its_audit_row(spark, cluster_like_checkout):
    salt = "do-not-print-this-salt"
    args = ["smoke", "--env", "local", "--date", "2026-01-02", "--source", "orders", "--force"]
    result = subprocess.run(
        [sys.executable, str(RUN_PY), *args],
        cwd=cluster_like_checkout,
        capture_output=True,
        text=True,
        env=clean_env(
            PYTHONPATH=str(cluster_like_checkout / "dist" / "gtdp.zip"), GTDP_PSEUDO_SALT=salt
        ),
        timeout=300,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert salt not in result.stderr
    ours = [json.loads(line) for line in result.stderr.splitlines() if line.startswith("{")]
    assert {entry["job"] for entry in ours} == {"smoke"}

    lake = (cluster_like_checkout / "data" / "lake").as_uri()
    [row] = spark.read.parquet(f"{lake}/ops/audit").collect()
    assert (row.job_name, row.layer, row.env, row.status) == ("smoke", "ops", "local", "SUCCESS")
    assert (row.source, row.ingestion_date) == ("orders", date(2026, 1, 2))
    assert (row.rows_in, row.rows_out) == (1000, 10)
    assert json.loads(row.metrics_json) == {"force": True}
    smoke = spark.read.parquet(f"{lake}/ops/smoke")
    assert smoke.count() == 10
    assert {r.run_id for r in smoke.select("run_id").distinct().collect()} == {row.run_id}


PROBE = """
import faulthandler
import logging
import signal
import sys

from gtdp.audit import Layer
from gtdp.jobs.registry import job

faulthandler.register(signal.SIGUSR1, all_threads=True)


@job("effective-conf", Layer.PERF, "store the effective Spark properties in the audit row")
def effective_conf(ctx):
    for key in ("spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled"):
        ctx.run.metrics[key] = ctx.spark.conf.get(key)


@job("blocked", Layer.STREAMING, "wait inside one Spark action that runs for days")
def blocked(ctx):
    logging.getLogger("probe").info("blocking")
    ctx.spark.range(0, 10**15, 1, 1).selectExpr("sum(id)").collect()


# Imported after the @job functions: run.py lists the jobs registered when it is imported.
from gtdp.jobs.run import main  # noqa: E402

sys.exit(main())
"""


@pytest.mark.slow
def test_spark_submit_properties_win_over_the_configuration(spark, cluster_like_checkout, tmp_path):
    (cluster_like_checkout / "probe.py").write_text(PROBE)
    env = clean_env(PYSPARK_PYTHON=sys.executable, PYSPARK_DRIVER_PYTHON=sys.executable)
    env.pop("PYTHONPATH", None)
    submit = ["spark-submit", "--master", "local[1]", "--py-files", "dist/gtdp.zip"]
    tuning = [
        "--conf",
        "spark.sql.shuffle.partitions=200",
        "--conf",
        "spark.sql.adaptive.enabled=false",
    ]
    job = ["probe.py", "effective-conf", "--env", "local", *lake_args(tmp_path)]

    result = subprocess.run(
        [*submit, *tuning, *job],
        cwd=cluster_like_checkout,
        capture_output=True,
        text=True,
        env=env,
        timeout=300,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    [row] = spark.read.parquet(str(tmp_path / "lake" / "ops" / "audit")).collect()
    assert json.loads(row.metrics_json) == {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.shuffle.partitions": "200",
    }
    # spark-submit merges the stderr of the Python driver into its stdout.
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert all(line.startswith("{") for line in lines), lines
    [effective] = [e for e in json_lines(result.stdout) if e["message"] == "spark configuration"]
    assert effective["spark_conf"]["spark.sql.shuffle.partitions"] == {
        "value": "200",
        "origin": "spark-submit",
    }
    assert effective["spark_conf"]["spark.sql.session.timeZone"] == {
        "value": "UTC",
        "origin": "gtdp",
    }


def test_help_lists_the_exit_codes(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--help"])

    assert exit_info.value.code == 0
    help_text = capsys.readouterr().out
    assert "130  stopped by SIGINT" in help_text
    assert "143  stopped by SIGTERM" in help_text


@pytest.mark.slow
@pytest.mark.parametrize("stop", [signal.SIGTERM, signal.SIGINT], ids=lambda s: s.name)
def test_a_stop_signal_gives_a_stopped_row_and_exit_128_plus_the_signal(spark, tmp_path, stop):
    (tmp_path / "probe.py").write_text(PROBE)
    log = tmp_path / "driver.log"
    # local[1]: the endless action holds the only executor thread until it is cancelled.
    job = ["blocked", "--env", "local", "--set", "spark.master=local[1]", *lake_args(tmp_path)]
    with log.open("w") as stderr:
        driver = subprocess.Popen(
            [sys.executable, str(tmp_path / "probe.py"), *job],
            stdout=subprocess.DEVNULL,
            stderr=stderr,
            env=clean_env(),
        )
    try:
        deadline = time.monotonic() + 120
        while '"message": "blocking"' not in log.read_text() and time.monotonic() < deadline:
            time.sleep(0.2)
        time.sleep(2)  # the signal then arrives while the driver waits inside a py4j call

        driver.send_signal(stop)

        try:
            returncode = driver.wait(timeout=60)
        except subprocess.TimeoutExpired:
            driver.send_signal(signal.SIGUSR1)  # the probe dumps its Python stacks
            time.sleep(2)
            pytest.fail(f"driver alive 60 s after {stop.name}:\n{log.read_text()[-6000:]}")
    finally:
        driver.kill()
    output = log.read_text()
    assert returncode == 128 + stop, output[-6000:]
    [stopped] = [entry for entry in json_lines(output) if entry["message"] == "run stopped"]
    assert stopped["signal"] == stop.name
    assert raw_tracebacks(output) == []
    [row] = spark.read.parquet(str(tmp_path / "lake" / "ops" / "audit")).collect()
    assert (row.job_name, row.status, row.error_message) == (
        "blocked",
        "STOPPED",
        f"stopped by {stop.name}",
    )
