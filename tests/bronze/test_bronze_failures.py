import json

import pytest
from pyspark.sql import functions as F

from gtdp.bronze.landing import LandingError
from gtdp.bronze.transform import ReconciliationError
from gtdp.io import TableLockedError

pytestmark = pytest.mark.slow

J2 = "2026-01-02"
ORDERS = "orders/orders_20260102.csv"


def orders_entry(manifest):
    [entry] = [e for e in manifest["files"] if e["path"] == ORDERS]
    return entry


def failed_row(lake):
    [row] = lake.audit()
    assert row.status == "FAILED"
    return row


def test_a_file_whose_lines_differ_from_its_manifest_fails_before_any_write(lake):
    manifest = lake.land(J2)
    orders_entry(manifest)["rows"] += 1
    lake.write_manifest(J2, manifest)

    with pytest.raises(ReconciliationError, match=ORDERS):
        lake.run(J2)

    row = failed_row(lake)
    assert row.error_message.startswith("ReconciliationError: ")
    assert ORDERS in row.error_message
    counts = json.loads(row.metrics_json)["files"][ORDERS]
    assert counts["manifest_rows"] == counts["landing_lines"] + 1
    assert counts["rows_written"] is None
    assert not lake.partition("orders", J2).exists()
    assert not lake.partition("customers", J2).exists()


def test_a_line_the_csv_reader_would_drop_fails_before_any_write(lake):
    manifest = lake.land(J2)
    path = lake.landing_file(J2, ORDERS)
    header, rest = path.read_text().split("\n", 1)
    path.write_text(f"{header}\n\n{rest}")
    orders_entry(manifest)["rows"] += 1
    lake.write_manifest(J2, manifest)

    with pytest.raises(ReconciliationError, match="read"):
        lake.run(J2)

    counts = json.loads(failed_row(lake).metrics_json)["files"][ORDERS]
    assert counts["manifest_rows"] == counts["landing_lines"] == counts["rows_read"] + 1
    assert not lake.partition("orders", J2).exists()


def test_a_failed_replay_leaves_the_published_partition_as_it_was(lake):
    manifest = lake.land(J2)
    lake.run(J2)
    before = sorted(row._record_hash for row in lake.table("orders").collect())
    orders_entry(manifest)["rows"] -= 1
    lake.write_manifest(J2, manifest)

    with pytest.raises(ReconciliationError):
        lake.run(J2)

    assert sorted(row._record_hash for row in lake.table("orders").collect()) == before


@pytest.mark.parametrize("change", ["extra file", "missing file"])
def test_landing_files_must_be_exactly_those_of_the_manifest(lake, change):
    lake.land(J2)
    path = lake.landing_file(J2, ORDERS)
    if change == "extra file":
        path.with_name("orders_late.csv").write_text(path.read_text())
    else:
        path.unlink()

    with pytest.raises(LandingError, match="does not match the manifest"):
        lake.run(J2)

    assert failed_row(lake).error_message.startswith("LandingError: ")


def test_a_day_without_its_manifest_is_not_ingested(lake):
    lake.land(J2)
    lake.manifest_file(J2).unlink()

    with pytest.raises(FileNotFoundError, match="_MANIFEST.json"):
        lake.run(J2)

    assert failed_row(lake).status == "FAILED"


def test_a_table_locked_by_another_run_fails_the_run_at_once(lake):
    lake.land(J2)
    lock = lake.partition("orders", J2).parents[1] / "_orders.lock"
    lock.parent.mkdir(parents=True)
    lock.write_text('{"run_id": "20261126T000000Z-other"}')

    with pytest.raises(TableLockedError, match="20261126T000000Z-other"):
        lake.run(J2, source="orders")

    assert failed_row(lake).error_message.startswith("TableLockedError: ")
    assert lock.exists()
    assert not lake.partition("orders", J2).exists()


def test_source_ingests_that_source_only(lake):
    manifest = lake.land(J2)

    lake.run(J2, source="customers")

    [entry] = [e for e in manifest["files"] if e["source"] == "customers"]
    assert lake.table("customers").count() == entry["rows"]
    assert not lake.partition("orders", J2).exists()
    [row] = lake.audit()
    assert (row.source, row.status) == ("customers", "SUCCESS")
    assert list(json.loads(row.metrics_json)["files"]) == [entry["path"]]


def test_an_unknown_source_fails(lake):
    lake.land(J2)

    with pytest.raises(ValueError, match="unknown source 'sensors'"):
        lake.run(J2, source="sensors")


def test_a_column_the_contract_does_not_know_is_kept_and_reported(lake):
    manifest = lake.land(J2)
    path = lake.landing_file(J2, ORDERS)
    header, *rows = path.read_text().splitlines()
    path.write_text("\n".join([f"{header},coupon", *(f"{row},X1" for row in rows)]) + "\n")

    run = lake.run(J2, source="orders")

    coupons = lake.table("orders").where(F.col("_corrupt_record").isNull()).select("coupon")
    assert {row.coupon for row in coupons.collect()} == {"X1"}
    assert lake.table("orders").count() == orders_entry(manifest)["rows"]
    [row] = lake.audit()
    assert row.run_id == run.run_id
    assert json.loads(row.metrics_json)["unknown_columns"] == {"orders": ["coupon"]}
