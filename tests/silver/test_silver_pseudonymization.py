"""KAN-30: personal data pseudonymised with a salt that never shows in a plan or an event log."""

import contextlib
import hashlib
import io
import os
import subprocess
import sys
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from gtdp.config import DEV_SALT, Env, LakePaths, Pseudonymization, Secret
from gtdp.io import read_table
from gtdp.schemas import SOURCES, bronze_schema
from gtdp.silver.pipelines import PIPELINES, classified
from gtdp.silver.pseudonymize import SaltError, salt_frame
from gtdp.silver.quality import DISPOSITION

from .lakes import DAYS, SALT, Lake, local
from .test_silver_incremental import silver_state

pytestmark = pytest.mark.slow

J1, J2 = DAYS[0], DAYS[1]
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
        salted(row.value): row.value.split(" ", 1)[0]
        for row in bronze_rows(published, "weblogs").select("value").collect()
    }
    weblogs = published.table("weblogs").where(F.col("ingestion_date") == J1).limit(50)

    assert "ip" not in weblogs.columns
    for row in weblogs.collect():
        ip = lines[row.log_id]
        assert row.ip_hash == salted(ip)
        assert row.ip_prefix == ip.rsplit(".", 1)[0] + ".0/24"


@pytest.mark.parametrize("source", ["orders", "customers", "weblogs"])
def test_no_silver_value_is_the_unsalted_hash_of_a_bronze_line(published, source):
    bronze = read_table(
        published.spark, f"{published.cfg.paths.bronze}/{source}", bronze_schema(SOURCES[source])
    )
    lines = {row._record_hash for row in bronze.select("_record_hash").collect()}
    table = published.table(source)
    strings = [f.name for f in table.schema.fields if f.dataType.simpleString() == "string"]
    values = {value for row in table.select(*strings).collect() for value in row}

    assert values.isdisjoint(lines)


def test_an_ip_cannot_be_recovered_from_silver_without_the_salt(published):
    weblogs = published.table("weblogs")
    dash = lambda column: F.coalesce(F.col(column).cast("string"), F.lit("-"))  # noqa: E731
    network = F.regexp_replace("ip_prefix", r"\.0/24$", "")
    guesses = weblogs.withColumn("octet", F.explode(F.sequence(F.lit(0), F.lit(255)))).withColumn(
        "ip", F.concat_ws(".", network, F.col("octet").cast("string"))
    )
    line = F.concat(
        F.col("ip"),
        F.lit(" - "),
        dash("customer_id"),
        F.lit(" ["),
        F.date_format("event_ts", "dd/MMM/yyyy:HH:mm:ss Z"),
        F.lit('] "'),
        F.concat_ws(" ", "method", "path", "protocol"),
        F.lit('" '),
        F.col("http_status").cast("string"),
        F.lit(" "),
        dash("bytes_sent"),
        F.lit(' "'),
        dash("referer"),
        F.lit('" "'),
        F.col("user_agent"),
        F.lit('"'),
    )
    oracle = (F.sha2(line, 256) == F.col("log_id")) | (F.sha2("ip", 256) == F.col("ip_hash"))

    assert weblogs.count() > 0
    assert guesses.where(oracle).select("log_id").distinct().count() == 0


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
    salt = salt_frame(published.spark, published.cfg.pseudonymization.salt)
    records = classified(published.spark, published.cfg.paths, source, date.fromisoformat(J1), salt)
    rows = pipeline.build(records.where(F.col(DISPOSITION) == "clean"))

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


def salted_lake(lake, salt: str, env: Env) -> Lake:
    """``lake`` run with ``salt`` in ``env``."""
    cfg = replace(lake.cfg, env=env, pseudonymization=Pseudonymization(env, Secret(salt)))
    return Lake(lake.spark, cfg, lake.drops)


@pytest.mark.parametrize("salt", [DEV_SALT, "salt-of-15-char", " " * 24])
def test_a_weak_salt_is_refused_on_the_cluster_before_any_write(fresh_lake, salt):
    lake = salted_lake(fresh_lake, salt, Env.CLUSTER)

    with pytest.raises(SaltError):
        lake.run("silver", J1)

    assert lake.audit()[-1].status == "FAILED"
    assert not local(lake.cfg.paths.silver).exists()


def test_another_salt_than_silvers_is_refused_before_any_write(fresh_lake):
    fresh_lake.run("silver", J1)
    before = silver_state(fresh_lake)
    lake = salted_lake(fresh_lake, f"another-{SALT}", Env.CLUSTER)

    with pytest.raises(SaltError, match="docs/runbook/silver.md"):
        lake.run("silver", J2)

    assert silver_state(fresh_lake) == before
    fingerprint = local(fresh_lake.cfg.paths.ops) / "pseudonymization" / "salt_fingerprint.json"
    assert SALT not in fingerprint.read_text(encoding="utf-8")
