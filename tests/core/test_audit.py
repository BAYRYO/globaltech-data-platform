import json
import logging
from dataclasses import replace
from datetime import UTC, date, datetime

import pytest

from gtdp.audit import Counters, Layer, audit_path, audited_run
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


@pytest.mark.parametrize("value", [3.0, True, "3"])
def test_a_counter_takes_only_an_int_or_none(value):
    counters = Counters(rows_in=3)
    counters.rows_in = None

    with pytest.raises(TypeError):
        counters.rows_out = value


def test_a_float_counter_fails_the_run_and_its_row_says_so(spark, lake_config):
    with pytest.raises(TypeError), audited_run(spark, lake_config, "silver", Layer.SILVER) as run:
        run.counters.rows_out = spark.range(4).selectExpr("avg(id) * 2").first()[0]

    [row] = audit_rows(spark, lake_config)
    assert row.status == "FAILED"
    assert row.error_message.startswith("TypeError")


def test_metric_keys_that_are_not_str_are_stored_as_str(spark, lake_config):
    with audited_run(spark, lake_config, "silver", Layer.SILVER) as run:
        run.metrics[date(2026, 1, 2)] = 5
        run.metrics[3] = "x"

    [row] = audit_rows(spark, lake_config)
    assert row.status == "SUCCESS"
    assert json.loads(row.metrics_json) == {"2026-01-02": 5, "3": "x"}


def test_a_failed_run_with_a_tuple_metric_key_keeps_its_failed_row(spark, lake_config):
    with (
        pytest.raises(ValueError, match="boom"),
        audited_run(spark, lake_config, "silver", Layer.SILVER) as run,
    ):
        run.metrics[("orders", "J2")] = 1
        raise ValueError("boom")

    [row] = audit_rows(spark, lake_config)
    assert row.status == "FAILED"
    assert json.loads(row.metrics_json) == {"('orders', 'J2')": 1}


def test_metrics_that_json_cannot_hold_still_give_one_row(spark, lake_config):
    with audited_run(spark, lake_config, "silver", Layer.SILVER) as run:
        run.counters.rows_in = 4
        run.metrics["by_day"] = {date(2026, 1, 2): 4}

    [row] = audit_rows(spark, lake_config)
    assert (row.status, row.rows_in) == ("SUCCESS", 4)
    assert "audit_error" in json.loads(row.metrics_json)


def test_a_counter_beyond_the_long_range_still_gives_one_row(spark, lake_config):
    with audited_run(spark, lake_config, "bronze", Layer.BRONZE) as run:
        run.counters.rows_in = 2**63

    [row] = audit_rows(spark, lake_config)
    assert (row.status, row.rows_in) == ("SUCCESS", None)
    assert "audit_error" in json.loads(row.metrics_json)
