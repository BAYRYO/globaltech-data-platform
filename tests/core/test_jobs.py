import json
import os
import shutil
import subprocess
import sys
import zipfile
from datetime import date
from pathlib import Path

import pytest

from gtdp.jobs.run import main

REPO = Path(__file__).resolve().parents[2]
RUN_PY = REPO / "src" / "gtdp" / "jobs" / "run.py"


def clean_env(**extra):
    env = {k: v for k, v in os.environ.items() if not k.startswith("GTDP_")}
    return {**env, **extra}


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
    args = ["smoke", "--env", "local", "--date", "2026-01-02", "--source", "orders"]
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
    smoke = spark.read.parquet(f"{lake}/ops/smoke")
    assert smoke.count() == 10
    assert {r.run_id for r in smoke.select("run_id").distinct().collect()} == {row.run_id}
