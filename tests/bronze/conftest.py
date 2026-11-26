import hashlib
import json
import shutil
from dataclasses import dataclass, fields
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import pytest
from pyspark.sql import DataFrame, SparkSession

from gtdp.audit import Run, audit_path, audited_run
from gtdp.config import AppConfig, LakePaths, load_config
from gtdp.datagen.generate import generate
from gtdp.datagen.model import PROFILES
from gtdp.io import read_table
from gtdp.jobs.catalog import JOBS
from gtdp.jobs.registry import JobContext

KIT = Path(__file__).resolve().parents[2] / "data" / "kit"


@dataclass
class Lake:
    """A local lake, and the generated ``ci`` drops that can be landed into it."""

    spark: SparkSession
    cfg: AppConfig
    drops: Path

    def land(self, day: str) -> dict:
        """Publish a drop with the layout of ``scripts/cluster/land.sh``; return its manifest."""
        manifest = json.loads((self.drops / day / "_MANIFEST.json").read_text())
        for entry in manifest["files"]:
            target = self.landing_file(day, entry["path"])
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(self.drops / day / entry["path"], target)
        self.write_manifest(day, manifest)
        return manifest

    def landing_file(self, day: str, key: str) -> Path:
        source, name = key.split("/")
        return _local(self.cfg.paths.landing) / source / f"ingestion_date={day}" / name

    def landing_uri(self, day: str, key: str) -> str:
        source, name = key.split("/")
        return f"{self.cfg.paths.landing}/{source}/ingestion_date={day}/{name}"

    def manifest_file(self, day: str) -> Path:
        landing = _local(self.cfg.paths.landing)
        return landing / "_manifests" / f"ingestion_date={day}" / "_MANIFEST.json"

    def write_manifest(self, day: str, manifest: dict) -> None:
        self.manifest_file(day).parent.mkdir(parents=True, exist_ok=True)
        self.manifest_file(day).write_text(json.dumps(manifest))

    def rewrite(self, day: str, manifest: dict, key: str, data: bytes, rows: int | None = None):
        """Land ``data`` as ``key``, its manifest entry updated as the generator would write it."""
        self.landing_file(day, key).write_bytes(data)
        [entry] = [entry for entry in manifest["files"] if entry["path"] == key]
        entry["sha256"] = hashlib.sha256(data).hexdigest()
        if rows is not None:
            entry["rows"] = rows
        self.write_manifest(day, manifest)

    def run(self, day: str, source: str | None = None) -> Run:
        """Run the registered ``bronze`` job inside its audited run, as run.py does."""
        spec, spark, cfg = JOBS["bronze"], self.spark, self.cfg
        with audited_run(spark, cfg, spec.name, spec.layer, source, date.fromisoformat(day)) as run:
            spec.func(JobContext(spark, cfg, run))
        return run

    def partition(self, source: str, day: str) -> Path:
        return _local(self.cfg.paths.bronze) / source / f"ingestion_date={day}"

    def table(self, source: str) -> DataFrame:
        return read_table(self.spark, f"{self.cfg.paths.bronze}/{source}", merge_schema=True)

    def audit(self) -> list:
        return self.spark.read.parquet(audit_path(self.cfg)).orderBy("started_at").collect()


def _local(uri: str) -> Path:
    return Path(urlparse(uri).path)


def _config(root: Path) -> AppConfig:
    lake = root / "lake"
    areas = {f"paths.{area.name}": str(lake / area.name) for area in fields(LakePaths)}
    return load_config("local", overrides=areas)


@pytest.fixture(scope="session")
def ci_drops(tmp_path_factory) -> Path:
    """The J1, J2 and J3 drops of the ``ci`` profile, seed 42."""
    out = tmp_path_factory.mktemp("generated")
    generate(PROFILES["ci"], 42, out, KIT)
    return out / "ci" / "landing"


@pytest.fixture
def lake(spark, ci_drops, tmp_path) -> Lake:
    return Lake(spark, _config(tmp_path), ci_drops)


@pytest.fixture(scope="module")
def module_lake(spark, ci_drops, tmp_path_factory) -> Lake:
    return Lake(spark, _config(tmp_path_factory.mktemp("module")), ci_drops)
