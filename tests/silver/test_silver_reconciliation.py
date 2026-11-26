"""KAN-27 and KAN-49: every record of a day is clean, rejected or duplicated, the audit says how
many, and each count equals what the generator's manifest predicts under the disposition table."""

import json
from datetime import date

import pytest

from gtdp.silver.reconcile import compare, expected, manifest_expected, table_measures
from gtdp.silver.report import contents, price_history, reconciliation

from .lakes import DAYS, bronze_lake


def silver_runs(lake) -> dict[str, object]:
    """The latest successful silver audit row of each day."""
    rows = [r for r in lake.audit() if r.job_name == "silver" and r.status == "SUCCESS"]
    return {str(r.ingestion_date): r for r in rows}


@pytest.mark.parametrize("day", DAYS)
def test_every_measure_equals_the_manifest(published, day):
    measured = json.loads(silver_runs(published)[day].metrics_json)["sources"]

    mismatches = {
        source: compare(counts, measured[source]["measures"])
        for source, counts in manifest_expected(published.manifest(day)).items()
    }

    assert mismatches == {source: [] for source in measured}


@pytest.mark.parametrize("day", DAYS)
def test_the_audit_row_holds_the_identity(published, day):
    run = silver_runs(published)[day]
    sources = json.loads(run.metrics_json)["sources"]

    assert run.rows_in == run.rows_out + run.rows_rejected + run.rows_duplicated
    for report in sources.values():
        assert report["rows_in"] == report["clean"] + report["rejected"] + report["duplicated"]
    assert run.rows_in == sum(f["rows"] for f in published.manifest(day)["files"])


@pytest.mark.parametrize("day", DAYS)
def test_the_tables_hold_what_the_audit_counts(published, day):
    sources = json.loads(silver_runs(published)[day].metrics_json)["sources"]

    for source, report in sources.items():
        shown = table_measures(
            published.spark, published.cfg.paths.silver, source, date.fromisoformat(day)
        )
        assert {name: report["measures"].get(name, 0) for name in shown} == shown


def test_a_reject_keeps_its_line_and_reasons(published):
    rejects = published.rejects("orders").collect()
    landed = {
        line
        for day in DAYS
        for line in (published.drops / day / f"orders/orders_{day.replace('-', '')}.csv")
        .read_text(encoding="utf-8")
        .splitlines()[1:]
    }

    assert rejects
    assert all(row.raw_record in landed for row in rejects)
    assert all(row.reject_reasons for row in rejects)
    assert {row._run_id for row in rejects} <= {r.run_id for r in published.audit()}


@pytest.mark.slow
@pytest.mark.parametrize("seed", [7, 2026])
def test_other_seeds_reconcile_with_their_manifests(spark, tmp_path, seed):
    lake = bronze_lake(spark, tmp_path, seed)
    for day in DAYS:
        run = lake.run("silver", day)
        for source, counts in manifest_expected(lake.manifest(day)).items():
            assert compare(counts, run.metrics["sources"][source]["measures"]) == [], (seed, day)


def test_a_code_missing_from_the_disposition_table_is_an_error():
    with pytest.raises(ValueError, match="NEW_ANOMALY"):
        expected("orders", 10, {"NEW_ANOMALY": 1})


def test_a_conflicting_version_rejects_two_records():
    counts = expected("products", 100, {"CONFLICTING_VERSION": 3, "DUPLICATE_EXACT": 1})

    assert counts["rejected.CONFLICTING_VERSION"] == 6
    assert counts["clean"] == 100 - 6 - 1


def test_the_report_compares_manifest_audit_and_tables_without_mismatch(published):
    lines = reconciliation(published.spark, published.cfg)
    statuses = {line[-1] for line in lines}
    versions, priced = price_history(published.spark, published.cfg)

    assert statuses <= {"ok", "info"}
    assert {(line[0], line[2]) for line in lines if line[-1] == "info"} == {
        ("customers", "flagged.FUTURE_SIGNUP")
    }
    tables = {
        (row.table, row.ingestion_date): row.rows
        for row in contents(published.spark, published.cfg).collect()
    }

    assert versions.count() >= 2
    assert priced.where("applicable_unit_price IS NULL").count() == 0
    assert (
        tables[("orders", DAYS[1])]
        == published.table("orders").where(f"ingestion_date = '{DAYS[1]}'").count()
    )
    assert tables[("customers", None)] == published.table("customers").count()
