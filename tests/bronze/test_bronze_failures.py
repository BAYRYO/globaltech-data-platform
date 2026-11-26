import hashlib
import json

import pytest
from pyspark.sql import functions as F

from gtdp.bronze.landing import LandingError
from gtdp.bronze.transform import ReconciliationError
from gtdp.io import TableLockedError

pytestmark = pytest.mark.slow

J1, J2 = "2026-01-01", "2026-01-02"
ORDERS = "orders/orders_20260102.csv"
CUSTOMERS = "customers/customers_20260102.csv"
SOURCES = ("orders", "customers", "products", "weblogs")


def entry_of(manifest, key):
    [entry] = [e for e in manifest["files"] if e["path"] == key]
    return entry


def failed_row(lake):
    [row] = lake.audit()
    assert row.status == "FAILED"
    return row


def nothing_written(lake, day):
    return not any(lake.partition(source, day).exists() for source in SOURCES)


def test_a_file_whose_lines_differ_from_its_manifest_fails_before_any_write(lake):
    manifest = lake.land(J2)
    entry_of(manifest, ORDERS)["rows"] += 1
    lake.write_manifest(J2, manifest)

    with pytest.raises(ReconciliationError, match=ORDERS):
        lake.run(J2)

    row = failed_row(lake)
    assert row.error_message.startswith("ReconciliationError: ")
    assert ORDERS in row.error_message
    counts = json.loads(row.metrics_json)["files"][ORDERS]
    assert counts["manifest_rows"] == counts["landing_lines"] + 1
    assert counts["rows_written"] is None
    assert nothing_written(lake, J2)


def test_a_line_the_csv_reader_would_drop_fails_before_any_write(lake):
    manifest = lake.land(J2)
    header, rest = lake.landing_file(J2, ORDERS).read_text().split("\n", 1)
    lake.rewrite(
        J2, manifest, ORDERS, f"{header}\n\n{rest}".encode(), entry_of(manifest, ORDERS)["rows"] + 1
    )

    with pytest.raises(ReconciliationError, match="read"):
        lake.run(J2)

    counts = json.loads(failed_row(lake).metrics_json)["files"][ORDERS]
    assert counts["manifest_rows"] == counts["landing_lines"] == counts["rows_read"] + 1
    assert nothing_written(lake, J2)


def changed_price(data):
    header, first, rest = data.split(b"\n", 2)
    fields = first.split(b",")
    fields[5] = b"99999.99"
    return b"\n".join([header, b",".join(fields), rest])


@pytest.mark.parametrize(
    "tamper",
    [changed_price, lambda data: data[:-5]],
    ids=["price-changed", "cut-inside-last-line"],
)
def test_a_file_whose_bytes_differ_from_its_manifest_sha256_fails_before_any_write(lake, tamper):
    manifest = lake.land(J2)
    path = lake.landing_file(J2, ORDERS)
    path.write_bytes(tamper(path.read_bytes()))
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    expected = entry_of(manifest, ORDERS)["sha256"]

    with pytest.raises(LandingError, match=ORDERS):
        lake.run(J2)

    message = failed_row(lake).error_message
    assert message.startswith("LandingError: ")
    assert f"{ORDERS}: sha256 {actual}, manifest {expected}" in message
    assert nothing_written(lake, J2)


@pytest.mark.parametrize(
    ("line", "problem"),
    [
        ("C900001,Hélène Müller".encode("latin-1"), "not UTF-8 (line 3)"),
        (b"C900001,nul\x00char", "NUL byte, which the CSV reader takes for a quote (line 3)"),
    ],
    ids=["latin-1", "nul-byte"],
)
def test_a_file_that_the_readers_would_alter_fails_before_any_write(lake, line, problem):
    manifest = lake.land(J2)
    header, first, rest = lake.landing_file(J2, CUSTOMERS).read_bytes().split(b"\n", 2)
    tail = first.split(b",", 2)[2]
    data = b"\n".join([header, first, line + b"," + tail, rest])
    lake.rewrite(J2, manifest, CUSTOMERS, data, entry_of(manifest, CUSTOMERS)["rows"] + 1)

    with pytest.raises(LandingError, match="line 3"):
        lake.run(J2)

    assert f"{CUSTOMERS}: {problem}" in failed_row(lake).error_message
    assert nothing_written(lake, J2)


def test_a_failed_replay_leaves_the_published_partition_as_it_was(lake):
    manifest = lake.land(J2)
    lake.run(J2)
    before = sorted(row._record_hash for row in lake.table("orders").collect())
    entry_of(manifest, ORDERS)["rows"] -= 1
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
    header, *rows = lake.landing_file(J2, ORDERS).read_text().splitlines()
    text = "\n".join([f"{header},coupon", *(f"{row},X1" for row in rows)]) + "\n"
    lake.rewrite(J2, manifest, ORDERS, text.encode())

    run = lake.run(J2, source="orders")

    coupons = lake.table("orders").where(F.col("_corrupt_record").isNull()).select("coupon")
    assert {row.coupon for row in coupons.collect()} == {"X1"}
    assert lake.table("orders").count() == entry_of(manifest, ORDERS)["rows"]
    [row] = lake.audit()
    assert row.run_id == run.run_id
    assert json.loads(row.metrics_json)["unknown_columns"] == {"orders": ["coupon"]}
