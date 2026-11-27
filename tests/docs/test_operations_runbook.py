"""The operations runbook and the scripts that the README names, against what the code does."""

import fnmatch
import re
from pathlib import Path

import pytest

from gtdp.audit import audit_path
from gtdp.config import load_config
from gtdp.gold.kpis import KPIS
from gtdp.gold.tables import STAR, table_path
from gtdp.io import _lock_path
from gtdp.jobs.catalog import JOBS
from gtdp.streaming.stream import SensorPaths
from silver.lakes import SALT, Lake, drops, lake_config

ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "docs" / "runbook" / "exploitation.md"
GOLD_RUNBOOK = ROOT / "docs" / "runbook" / "gold.md"


def daily_audit_query() -> str:
    """The SQL of step 5 of « Traiter un jour »."""
    text = RUNBOOK.read_text(encoding="utf-8")
    start = text.index("\n## Traiter un jour\n")
    section = text[start : text.index("\n## ", start + 1)]
    [query] = re.findall(r'spark\.sql\("""\n(.*?)"""\)', section, flags=re.DOTALL)
    return query


@pytest.mark.slow
def test_the_daily_audit_query_adds_up_on_every_row_after_another_job_ran_that_day(
    spark, tmp_path, monkeypatch
):
    """« Chaque job a une ligne `SUCCESS` et `adds_up` vaut `true` ». The MapReduce comparison
    of the README runs `revenue-by-country-spark --date`, whose row sits on the same day and
    counts countries, not lines."""
    query = daily_audit_query()
    day = re.search(r"ingestion_date = DATE'([\d-]+)'", query).group(1)
    monkeypatch.setenv("GTDP_PSEUDO_SALT", SALT)
    lake = Lake(spark, lake_config(tmp_path), drops(tmp_path / "generated", 42))
    lake.land(day)
    for job in ("bronze", "silver", "gold", "revenue-by-country-spark"):
        lake.run(job, day)

    spark.read.parquet(audit_path(lake.cfg)).createOrReplaceTempView("audit")
    try:
        rows = spark.sql(query).collect()
    finally:
        spark.catalog.dropTempView("audit")
    assert sorted(row.job_name for row in rows) == ["bronze", "gold", "silver"]
    assert [row for row in rows if row.status != "SUCCESS" or row.adds_up is not True] == []


def test_only_silver_reads_the_salt_and_no_script_or_config_says_every_job_needs_it():
    assert [spec.name for spec in JOBS.values() if spec.pseudonymizes] == ["silver"]
    claims = [
        f"{path.relative_to(ROOT)}:{number}: {line.strip()}"
        for folder in ("scripts", "conf")
        for path in sorted((ROOT / folder).rglob("*"))
        if path.suffix in {".sh", ".py", ".toml"}
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if "GTDP_PSEUDO_SALT" in line and re.search(r"\b(every|all)\b", line)
    ]
    assert claims == []


def gold_lock_removal(page: Path) -> list[str]:
    """The paths of the manual removal of gold's locks on ``page``."""
    [line] = [
        line
        for line in page.read_text(encoding="utf-8").splitlines()
        if "hdfs dfs -rm" in line and "/datalake/_gold.lock" in line
    ]
    return [path.strip("'") for path in re.findall(r"'?/datalake/[^\s']+'?", line)]


@pytest.mark.slow
@pytest.mark.parametrize("page", [RUNBOOK, GOLD_RUNBOOK], ids=lambda page: page.name)
def test_the_manual_gold_lock_removal_deletes_every_gold_lock_and_no_stream_lock(spark, page):
    paths = load_config("cluster").paths
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path

    def lock(table: str) -> str:
        return _lock_path(hadoop_path(table)).toUri().getPath()

    tables = [table.name for table in STAR] + list(KPIS)
    gold = [lock(paths.gold)] + [lock(table_path(paths.gold, name)) for name in tables]
    stream = [lock(table) for table in SensorPaths.of(paths, "streaming-sensors").tables.values()]
    removal = gold_lock_removal(page)

    def removed(path: str) -> bool:
        return any(fnmatch.fnmatchcase(path, pattern) for pattern in removal)

    assert [path for path in gold if not removed(path)] == []
    assert [path for path in stream if removed(path)] == []
