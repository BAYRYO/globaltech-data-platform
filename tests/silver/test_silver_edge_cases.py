"""KAN-50: a day without data for some sources, and one record landed in two files of a day."""

import hashlib
import json
from datetime import date

import pytest
from pyspark.sql import functions as F

from gtdp.io import read_table
from gtdp.schemas import SOURCES, Format, bronze_schema
from gtdp.silver import days
from gtdp.silver.pipelines import PIPELINES, rejects_path, table_path
from gtdp.silver.reconcile import compare, manifest_expected, read_manifest
from gtdp.silver.report import reconciliation

from .lakes import DAYS, local

pytestmark = pytest.mark.slow

J1, J2, J3 = DAYS


def manifest_file(lake, day):
    return local(lake.cfg.paths.landing) / "_manifests" / f"ingestion_date={day}" / "_MANIFEST.json"


def empty_landed(lake, day, source):
    """Land ``source`` for ``day`` without data lines (a CSV file keeps its header), with a
    manifest entry of no row and no anomaly, and ingest it in bronze again."""
    header = 0 if SOURCES[source].format is Format.TEXT else 1
    lake.change_landed(day, source, lambda lines: lines[:header])
    manifest = json.loads(manifest_file(lake, day).read_text())
    for entry in manifest["files"]:
        if entry["source"] == source:
            entry["anomalies"] = {}
    manifest_file(lake, day).write_text(json.dumps(manifest))


def land_another_file(lake, day, key, lines, anomalies):
    """Land one more file ``key`` of ``lines`` for ``day``, list it in the manifest and ingest its
    source in bronze again."""
    source, name = key.split("/")
    data = "".join(f"{line}\n" for line in lines).encode()
    (local(lake.cfg.paths.landing) / source / f"ingestion_date={day}" / name).write_bytes(data)
    manifest = json.loads(manifest_file(lake, day).read_text())
    manifest["files"].append(
        {
            "source": source,
            "path": key,
            "rows": len(lines) - 1,
            "sha256": hashlib.sha256(data).hexdigest(),
            "anomalies": anomalies,
        }
    )
    manifest_file(lake, day).write_text(json.dumps(manifest))
    lake.run("bronze", day, source)


def partition_files(path, day):
    return {item.name for item in (local(path) / f"ingestion_date={day}").iterdir()}


def test_a_day_without_data_for_some_sources_is_published_empty_and_complete(fresh_lake):
    lake, silver = fresh_lake, fresh_lake.cfg.paths.silver
    emptied = ("customers", "orders", "weblogs")
    lake.run("silver", J1)
    customers_of_j1 = sorted(lake.table("customers").collect())
    for source in emptied:
        empty_landed(lake, J2, source)

    run = lake.run("silver", J2)

    assert [str(d) for d in days.complete_days(lake.spark, silver)] == [J1, J2]
    assert sorted(days.published_sources(lake.spark, silver, date.fromisoformat(J2))) == sorted(
        PIPELINES
    )
    reports = run.metrics["sources"]
    for source in emptied:
        counts = {name: reports[source][name] for name in ("rows_in", "clean", "rejected")}
        assert counts == {"rows_in": 0, "clean": 0, "rejected": 0}, source
        assert (reports[source]["duplicated"], reports[source]["measures"]) == (0, {}), source
        assert reports[source]["rejects_written"] == 0, source
        assert partition_files(rejects_path(silver, source), J2) >= {"_SUCCESS"}, source
    for source in ("orders", "weblogs"):
        assert reports[source]["rows_written"] == 0, source
        assert lake.table(source).where(F.col("ingestion_date") == J2).count() == 0, source
        assert "_SUCCESS" in partition_files(table_path(silver, source), J2), source
    assert sorted(lake.table("customers").collect()) == customers_of_j1
    audit = lake.audit()[-1]
    products = sum(f["rows"] for f in lake.manifest(J2)["files"] if f["source"] == "products")
    assert (audit.status, audit.rows_in) == ("SUCCESS", products)
    assert audit.rows_in == audit.rows_out + audit.rows_rejected + audit.rows_duplicated
    lines = reconciliation(lake.spark, lake.cfg)
    assert {line[-1] for line in lines if line[1] in (J1, J2)} <= {"ok", "info"}
    assert {line[-1] for line in lines if line[1] == J3} == {"no run"}
    shown = {(line[0], line[2]): line[3:6] for line in lines if line[1] == J2}
    for source in emptied:
        assert shown[(source, "rows_in")] == (0, 0, None), source
        assert shown[(source, "rejected")] == (0, 0, 0), source
    assert shown[("orders", "clean")] == shown[("weblogs", "clean")] == (0, 0, 0)


def test_a_record_landed_in_two_files_of_a_day_is_published_once(fresh_lake):
    lake = fresh_lake
    customer = lake.landed_lines(J1, "customers")[1].split(",")[0]
    line = f"T900000010,2026-01-01T12:00:00,{customer},P00042,1,10.00,web,COMPLETED"
    lake.append_lines(J2, "orders", [line])
    header = lake.landed_lines(J2, "orders")[0]
    land_another_file(
        lake, J2, "orders/orders_20260102_resent.csv", [header, line], {"DUPLICATE_EXACT": 1}
    )
    lake.run("silver", J1)

    run = lake.run("silver", J2)

    bronze = read_table(
        lake.spark, f"{lake.cfg.paths.bronze}/orders", bronze_schema(SOURCES["orders"])
    ).where(F.col("transaction_id") == "T900000010")
    copies = bronze.select("_source_file", "_record_hash").collect()
    assert len({row._source_file for row in copies}) == len(copies) == 2
    assert len({row._record_hash for row in copies}) == 1
    expected = manifest_expected(
        read_manifest(lake.spark, lake.cfg.paths.landing, date(2026, 1, 2))
    )
    assert compare(expected["orders"], run.metrics["sources"]["orders"]["measures"]) == []
    assert lake.table("orders").where(F.col("transaction_id") == "T900000010").count() == 1
