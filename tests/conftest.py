import os
from dataclasses import fields

import pytest

from gtdp.config import LakePaths, load_config
from gtdp.spark import build_spark


@pytest.fixture(autouse=True)
def _isolated_environment(monkeypatch):
    """Keep GTDP_* variables of the developer shell out of every test."""
    for name in list(os.environ):
        if name.startswith("GTDP_"):
            monkeypatch.delenv(name)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    cfg = load_config(
        "local", overrides={"spark.master": "local[2]", "spark.shuffle_partitions": "2"}
    )
    session = build_spark(
        cfg,
        "gtdp-tests",
        extra_conf={
            "spark.ui.enabled": "false",
            "spark.sql.warehouse.dir": str(tmp_path_factory.mktemp("warehouse")),
        },
    )
    yield session
    session.stop()


@pytest.fixture
def lake_config(tmp_path):
    """Local configuration whose lake areas all live under ``tmp_path / "lake"``."""
    lake = tmp_path / "lake"
    areas = {f"paths.{area.name}": str(lake / area.name) for area in fields(LakePaths)}
    return load_config("local", overrides=areas)
