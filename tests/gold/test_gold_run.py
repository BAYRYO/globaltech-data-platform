"""The gold job through run.py: what an operator sees from the exit code."""

import json
import os
import shutil
import subprocess
import sys
from dataclasses import fields
from pathlib import Path

import pytest

from gtdp.config import LakePaths

pytestmark = pytest.mark.slow

CONF = Path(__file__).resolve().parents[2] / "conf"


def run_gold(tmp_path: Path, *args: str, conf: Path = CONF) -> subprocess.CompletedProcess:
    lake = tmp_path / "lake"
    areas = [f"paths.{area.name}={lake / area.name}" for area in fields(LakePaths)]
    paths = [arg for area in areas for arg in ("--set", area)]
    env = {k: v for k, v in os.environ.items() if not k.startswith("GTDP_")}
    return subprocess.run(
        [sys.executable, "-m", "gtdp.jobs.run", "gold", "--env", "local", *paths, *args],
        capture_output=True,
        text=True,
        env={**env, "GTDP_CONF_DIR": str(conf)},
        timeout=300,
        check=False,
    )


def errors(result: subprocess.CompletedProcess) -> list[str]:
    lines = [json.loads(line) for line in result.stderr.splitlines() if line.startswith("{")]
    return [line["message"] for line in lines if line["level"] == "ERROR"]


def test_gold_without_date_exits_2(tmp_path):
    result = run_gold(tmp_path)

    assert result.returncode == 2, result.stderr[-3000:]
    assert errors(result) == ["invalid configuration: gold needs --date"]


def test_gold_without_a_gold_section_exits_2(tmp_path):
    conf = tmp_path / "conf"
    shutil.copytree(CONF, conf)
    app = conf / "app.toml"
    app.write_text(app.read_text().split("[gold]")[0])

    result = run_gold(tmp_path, "--date", "2026-01-01", conf=conf)

    assert result.returncode == 2, result.stderr[-3000:]
    assert errors(result) == ["invalid configuration: the configuration has no [gold] section"]
