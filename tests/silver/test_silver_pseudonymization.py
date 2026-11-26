"""KAN-30: personal data pseudonymised with a salt that never shows in a plan or an event log."""

import contextlib
import hashlib
import io
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from gtdp.config import LakePaths
from gtdp.io import read_table
from gtdp.schemas import SOURCES, bronze_schema
from gtdp.silver.pipelines import PIPELINES, classified
from gtdp.silver.pseudonymize import salt_frame
from gtdp.silver.quality import DISPOSITION

from .lakes import DAYS, SALT, local

pytestmark = pytest.mark.slow

J1 = DAYS[0]
SRC = Path(__file__).resolve().parents[2] / "src"


def salted(value: str) -> str:
    return hashlib.sha256(f"{SALT}{value}".encode()).hexdigest()


def bronze_rows(lake, source, day=J1):
    path = f"{lake.cfg.paths.bronze}/{source}"
    table = read_table(lake.spark, path, bronze_schema(SOURCES[source]))
    return table.where(F.col("ingestion_date") == day)


def test_customers_keep_a_salted_email_hash_and_lose_name_and_phone(published):
    customers = published.table("customers")
    raw = {
        row.customer_id: row.email
        for row in bronze_rows(published, "customers").select("customer_id", "email").collect()
    }
    sample = customers.where(F.size("quality_flags") == 0).orderBy("customer_id").limit(50)

    assert {"email", "full_name", "phone"}.isdisjoint(customers.columns)
    for row in sample.collect():
        assert row.email_hash == salted(raw[row.customer_id].strip().lower())


def test_a_flagged_email_has_no_hash(published):
    flagged = published.table("customers").where(
        F.array_contains("quality_flags", "BAD_EMAIL")
        | F.array_contains("quality_flags", "MISSING_EMAIL")
    )

    assert flagged.count() > 0
    assert flagged.where(F.col("email_hash").isNotNull()).count() == 0


def test_weblogs_keep_a_salted_ip_hash_and_the_slash_24_network(published):
    lines = {
        row._record_hash: row.value.split(" ", 1)[0]
        for row in bronze_rows(published, "weblogs").select("_record_hash", "value").collect()
    }
    weblogs = published.table("weblogs").where(F.col("ingestion_date") == J1).limit(50)

    assert "ip" not in weblogs.columns
    for row in weblogs.collect():
        ip = lines[row.log_id]
        assert row.ip_hash == salted(ip)
        assert row.ip_prefix == ip.rsplit(".", 1)[0] + ".0/24"


def plans(frame) -> str:
    """Every plan Spark prints for ``frame``, once it ran."""
    frame.collect()
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        for mode in ("extended", "formatted", "cost", "codegen"):
            frame.explain(mode=mode)
    return out.getvalue() + frame._jdf.queryExecution().toString()


@pytest.mark.parametrize("source", ["customers", "weblogs"])
def test_the_salt_is_absent_from_the_plans(published, source):
    pipeline = PIPELINES[source]
    records = classified(published.spark, published.cfg.paths, source, date.fromisoformat(J1))
    clean = records.where(F.col(DISPOSITION) == "clean")
    rows = pipeline.build(clean, salt_frame(published.spark, published.cfg.pseudonymization.salt))

    printed = plans(rows)

    assert "_salt" in printed
    assert SALT not in printed


def test_the_salt_is_absent_from_the_event_log(fresh_lake, tmp_path):
    events = tmp_path / "events"
    events.mkdir()
    env = {
        **{k: v for k, v in os.environ.items() if not k.startswith("GTDP_")},
        **{
            f"GTDP_PATHS__{area.upper()}": str(local(getattr(fresh_lake.cfg.paths, area)))
            for area in LakePaths.__dataclass_fields__
        },
        "GTDP_PSEUDO_SALT": SALT,
        "PYTHONPATH": str(SRC),
        "PYSPARK_SUBMIT_ARGS": (
            f"--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file://{events} "
            "--conf spark.ui.enabled=false pyspark-shell"
        ),
    }
    command = [sys.executable, "-m", "gtdp.jobs.run", "silver", "--env", "local", "--date", J1]

    job = subprocess.run(command, env=env, capture_output=True, text=True, timeout=600, check=False)
    logs = list(events.iterdir())

    assert job.returncode == 0, job.stderr[-2000:]
    assert logs
    assert all(SALT not in log.read_text(encoding="utf-8") for log in logs)
    assert "email_hash" in "".join(log.read_text(encoding="utf-8") for log in logs)
    assert SALT not in job.stdout + job.stderr
    assert fresh_lake.table("customers").where(F.col("email_hash").isNotNull()).count() > 0
