"""A local lake of generated ``ci`` drops, and the jobs that fill it."""

import json
import shutil
from dataclasses import dataclass, fields
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession

from gtdp.audit import Run, audit_path, audited_run
from gtdp.config import AppConfig, LakePaths, load_config
from gtdp.datagen.generate import generate
from gtdp.datagen.model import PROFILES
from gtdp.io import read_table
from gtdp.jobs.catalog import JOBS
from gtdp.jobs.registry import JobContext
from gtdp.schemas import SILVER_REJECTS
from gtdp.silver.pipelines import PIPELINES, rejects_path, table_path

KIT = Path(__file__).resolve().parents[2] / "data" / "kit"
DAYS = ("2026-01-01", "2026-01-02", "2026-01-03")
SALT = "test-salt-7f3a91c2"


@dataclass
class Lake:
    """A local lake holding the bronze tables of generated ``ci`` drops."""

    spark: SparkSession
    cfg: AppConfig
    drops: Path

    def land(self, day: str) -> dict:
        """Publish a drop with the layout of ``scripts/cluster/land.sh``; return its manifest."""
        manifest = json.loads((self.drops / day / "_MANIFEST.json").read_text())
        landing = local(self.cfg.paths.landing)
        for entry in manifest["files"]:
            source, name = entry["path"].split("/")
            target = landing / source / f"ingestion_date={day}" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(self.drops / day / entry["path"], target)
        manifest_file = landing / "_manifests" / f"ingestion_date={day}" / "_MANIFEST.json"
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        manifest_file.write_text(json.dumps(manifest))
        return manifest

    def manifest(self, day: str) -> dict:
        return json.loads((self.drops / day / "_MANIFEST.json").read_text())

    def run(self, job: str, day: str, source: str | None = None, *, force: bool = False) -> Run:
        """Run a registered job inside its audited run, as run.py does."""
        spec, spark, cfg = JOBS[job], self.spark, self.cfg
        with audited_run(spark, cfg, spec.name, spec.layer, source, date.fromisoformat(day)) as run:
            spec.func(JobContext(spark, cfg, run, force=force))
        return run

    def table(self, source: str) -> DataFrame:
        path = table_path(self.cfg.paths.silver, source)
        return read_table(self.spark, path, PIPELINES[source].schema)

    def rejects(self, source: str) -> DataFrame:
        path = rejects_path(self.cfg.paths.silver, source)
        return read_table(self.spark, path, SILVER_REJECTS)

    def audit(self) -> list:
        return read_table(self.spark, audit_path(self.cfg)).orderBy("started_at").collect()


def local(uri: str) -> Path:
    return Path(urlparse(uri).path)


def lake_config(root: Path) -> AppConfig:
    lake = root / "lake"
    areas = {f"paths.{area.name}": str(lake / area.name) for area in fields(LakePaths)}
    return load_config("local", overrides=areas)


def drops(root: Path, seed: int) -> Path:
    generate(PROFILES["ci"], seed, root, KIT)
    return root / "ci" / "landing"


def bronze_lake(spark: SparkSession, root: Path, seed: int) -> Lake:
    """A lake with the three ``ci`` drops of ``seed`` landed and ingested in bronze."""
    lake = Lake(spark, lake_config(root), drops(root / "generated", seed))
    for day in DAYS:
        lake.land(day)
        lake.run("bronze", day)
    return lake


def copy_of(lake: Lake, root: Path) -> Lake:
    """A lake of its own under ``root`` holding a copy of the landing and bronze of ``lake``."""
    copy = Lake(lake.spark, lake_config(root), lake.drops)
    for area in ("landing", "bronze"):
        source, target = getattr(lake.cfg.paths, area), getattr(copy.cfg.paths, area)
        shutil.copytree(local(source), local(target))
    return copy
