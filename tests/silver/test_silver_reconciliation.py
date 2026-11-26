"""KAN-27 and KAN-49: every record of a day is clean, rejected or duplicated, the audit says how
many, and each count equals what the generator's manifest predicts under the disposition table."""

import hashlib
import json
import re
import shutil
from datetime import date

import pytest
from pyspark.sql import functions as F

from gtdp.io import read_table
from gtdp.schemas import SOURCES, bronze_schema
from gtdp.silver.pipelines import rejects_path
from gtdp.silver.products import violations, with_applicable_price
from gtdp.silver.reconcile import compare, expected, manifest_expected, table_measures
from gtdp.silver.report import contents, price_history, print_report, reconciliation

from .lakes import DAYS, SALT, bronze_lake, local

PERSONAL_DATA = re.compile(
    r"[^@\s|]+@[^@\s|]+\.[a-z]{2,}"  # an email
    r"|\+\d{8,}"  # a phone number
    r"|\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"  # an IPv4 address
)


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
def test_other_seeds_reconcile_and_hold_what_gold_relies_on(spark, tmp_path, seed):
    lake = bronze_lake(spark, tmp_path, seed)
    for day in DAYS:
        run = lake.run("silver", day)
        for source, counts in manifest_expected(lake.manifest(day)).items():
            assert compare(counts, run.metrics["sources"][source]["measures"]) == [], (seed, day)

    orders, products = lake.table("orders"), lake.table("products")
    priced = with_applicable_price(orders, products)
    assert orders.count() == orders.select("transaction_id").distinct().count() == priced.count()
    assert priced.where(F.col("product_sk").isNull()).count() == 0
    assert orders.join(lake.table("customers"), "customer_id", "left_anti").count() == 0
    assert orders.where(F.col("discount_pct").isNull()).count() == 0
    assert sum(violations(products).values()) == 0


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


def test_the_report_prints_no_personal_data(published, capsys):
    bronze = read_table(
        published.spark,
        f"{published.cfg.paths.bronze}/customers",
        bronze_schema(SOURCES["customers"]),
    )
    person = bronze.where(F.col("email").contains("@") & F.col("phone").isNotNull()).first()

    status = print_report(published.spark, published.cfg, customer_id=person.customer_id)
    printed = capsys.readouterr().out

    assert status == 0
    assert person.customer_id in printed
    assert [p for p in (person.full_name, person.email, person.phone) if p in printed] == []
    assert PERSONAL_DATA.findall(printed) == []
    email_hash = hashlib.sha256(f"{SALT}{person.email.strip().lower()}".encode()).hexdigest()
    assert email_hash in printed


def test_the_report_flags_a_count_the_manifest_or_the_tables_disagree_with(fresh_lake):
    lake, day = fresh_lake, DAYS[0]
    lake.run("silver", day)
    landing = local(lake.cfg.paths.landing)
    manifest_file = landing / "_manifests" / f"ingestion_date={day}" / "_MANIFEST.json"
    manifest = json.loads(manifest_file.read_text())
    [orders] = [entry for entry in manifest["files"] if entry["source"] == "orders"]
    orders["anomalies"]["NULL_CUSTOMER"] += 1
    manifest_file.write_text(json.dumps(manifest))

    def mismatches():
        return {
            (source, measure): counts
            for source, at, measure, *counts, status in reconciliation(lake.spark, lake.cfg)
            if at == day and status == "MISMATCH"
        }

    found = mismatches()
    assert set(found) == {
        ("orders", "rejected.NULL_CUSTOMER"),
        ("orders", "rejected"),
        ("orders", "clean"),
    }
    predicted, audited, shown = found[("orders", "rejected.NULL_CUSTOMER")]
    assert predicted == audited + 1 == shown + 1

    orders["anomalies"]["NULL_CUSTOMER"] -= 1
    manifest_file.write_text(json.dumps(manifest))
    shutil.rmtree(local(rejects_path(lake.cfg.paths.silver, "orders")) / f"ingestion_date={day}")

    found = mismatches()
    assert ("orders", "rejected") in found
    assert {key[0] for key in found} == {"orders"}
    assert all(shown == 0 < audited == predicted for predicted, audited, shown in found.values())


def test_the_report_explains_a_classification_that_its_outputs_read_from_memory(published, capsys):
    status = print_report(published.spark, published.cfg, explained=("orders", date(2026, 1, 2)))
    printed = capsys.readouterr().out

    assert status == 0
    before = printed.split("== classification of orders 2026-01-02, before persist")[1]
    plan, outputs = before.split("== rejects, reading the persisted classification")
    rejects_plan, clean_plan = outputs.split("== clean rows, reading the persisted classification")
    assert "FileScan parquet" in plan
    assert "InMemoryTableScan" not in plan
    assert "InMemoryTableScan" in rejects_plan
    assert "InMemoryTableScan" in clean_plan


def test_the_report_fails_while_a_landed_day_is_incomplete(fresh_lake, capsys):
    lake = fresh_lake
    lake.run("silver", DAYS[0])
    lake.run("silver", DAYS[1])

    status = print_report(lake.spark, lake.cfg)
    printed = capsys.readouterr().out

    assert status == 1
    assert f"incomplete: ['{DAYS[2]}']" in printed
