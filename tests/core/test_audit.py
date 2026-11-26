import json
import logging
from dataclasses import replace
from datetime import UTC, date, datetime

import pytest

from gtdp.audit import Layer, audit_path, audited_run
from gtdp.log import configure_logging

pytestmark = pytest.mark.slow

COLUMNS = {
    "run_id",
    "job_name",
    "layer",
    "source",
    "ingestion_date",
    "env",
    "status",
    "started_at",
    "ended_at",
    "duration_s",
    "rows_in",
    "rows_out",
    "rows_rejected",
    "rows_duplicated",
    "yarn_app_id",
    "git_sha",
    "error_message",
    "metrics_json",
    "run_date",
}


def audit_rows(spark, cfg):
    return spark.read.parquet(audit_path(cfg)).collect()


def test_successful_run_appends_one_complete_row(spark, lake_config):
    cfg = replace(lake_config, git_sha="abc123")
    before = datetime.now(UTC)

    with audited_run(spark, cfg, "bronze", Layer.BRONZE, "orders", date(2026, 1, 2)) as run:
        run.counters.rows_in = 10
        run.counters.rows_out = 7
        run.counters.rows_rejected = 2
        run.counters.rows_duplicated = 1
        run.metrics["files"] = 3

    [row] = audit_rows(spark, cfg)
    assert set(row.asDict()) == COLUMNS
    assert row.run_id == run.run_id
    assert (row.job_name, row.layer, row.source, row.env) == ("bronze", "bronze", "orders", "local")
    assert row.ingestion_date == date(2026, 1, 2)
    assert row.status == "SUCCESS"
    assert (row.rows_in, row.rows_out, row.rows_rejected, row.rows_duplicated) == (10, 7, 2, 1)
    assert json.loads(row.metrics_json) == {"files": 3}
    assert row.yarn_app_id == spark.sparkContext.applicationId
    assert row.git_sha == "abc123"
    assert row.error_message is None
    assert row.run_date == before.date()
    assert row.started_at <= row.ended_at
    assert row.duration_s >= 0


def test_failed_run_appends_a_failed_row_and_reraises(spark, lake_config):
    with (
        pytest.raises(ValueError, match="bad input"),
        audited_run(spark, lake_config, "silver", Layer.SILVER) as run,
    ):
        run.counters.rows_in = 5
        raise ValueError("bad input")

    [row] = audit_rows(spark, lake_config)
    assert row.status == "FAILED"
    assert row.error_message == "ValueError: bad input"
    assert row.rows_in == 5
    assert row.source is None
    assert row.ingestion_date is None


def test_each_run_adds_its_own_row(spark, lake_config):
    run_ids = set()
    for _ in range(2):
        with audited_run(spark, lake_config, "gold", Layer.GOLD) as run:
            run_ids.add(run.run_id)

    assert {row.run_id for row in audit_rows(spark, lake_config)} == run_ids
    assert len(run_ids) == 2


@pytest.fixture
def unwritable_audit(lake_config, tmp_path):
    """A configuration whose ops area is a regular file, so the audit write fails."""
    ops = tmp_path / "ops-is-a-file"
    ops.write_text("")
    return replace(lake_config, paths=replace(lake_config.paths, ops=ops.as_uri()))


def test_audit_write_failure_never_masks_the_job_error(spark, unwritable_audit, caplog):
    with (
        pytest.raises(KeyError, match="the job error"),
        audited_run(spark, unwritable_audit, "silver", Layer.SILVER),
    ):
        raise KeyError("the job error")

    assert "could not write the audit row" in caplog.text


def test_audit_write_failure_after_success_is_logged_not_raised(spark, unwritable_audit, caplog):
    with caplog.at_level(logging.ERROR), audited_run(spark, unwritable_audit, "gold", Layer.GOLD):
        pass

    assert "could not write the audit row" in caplog.text


def test_logs_inside_the_run_carry_its_id_and_job(spark, lake_config, stderr_lines):
    configure_logging()
    with audited_run(spark, lake_config, "gold", Layer.GOLD) as run:
        logging.getLogger("gtdp.jobs.gold").info("inside")

    [inside] = [entry for entry in stderr_lines() if entry["message"] == "inside"]
    assert (inside["run_id"], inside["job"]) == (run.run_id, "gold")
