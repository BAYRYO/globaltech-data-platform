"""KAN-50: a landed file without data lines. A CSV file with its header, or a log file without
lines, is an empty extract: its partition is written empty and the audit counts 0. A CSV file
without its header line is refused: nothing says which source's columns it holds."""

import json

import pytest
from pyspark.sql import functions as F

from gtdp.bronze.landing import LandingError
from gtdp.bronze.report import unmatched
from gtdp.io import read_table
from gtdp.schemas import SOURCES, bronze_schema

pytestmark = pytest.mark.slow

J2 = "2026-01-02"


def key_of(manifest, source):
    [entry] = [e for e in manifest["files"] if e["source"] == source]
    return entry["path"]


def header_line(lake, key):
    return lake.landing_file(J2, key).read_bytes().split(b"\n", 1)[0]


@pytest.mark.parametrize(
    ("source", "content"),
    [
        ("orders", "header"),
        ("customers", "header without line break"),
        ("products", "header"),
        ("weblogs", "no byte"),
    ],
)
def test_a_file_without_data_lines_is_ingested_as_an_empty_partition(lake, source, content):
    manifest = lake.land(J2)
    key = key_of(manifest, source)
    data = {
        "header": header_line(lake, key) + b"\n",
        "header without line break": header_line(lake, key),
        "no byte": b"",
    }[content]
    lake.rewrite(J2, manifest, key, data, rows=0)

    lake.run(J2)

    [row] = lake.audit()
    others = sum(e["rows"] for e in manifest["files"] if e["source"] != source)
    assert (row.status, row.rows_in, row.rows_out) == ("SUCCESS", others, others)
    metrics = json.loads(row.metrics_json)
    assert metrics["files"][key] == {
        "source": source,
        "manifest_rows": 0,
        "landing_lines": 0,
        "rows_read": 0,
        "unknown_columns": [],
        "rows_written": 0,
        "corrupt_records": 0,
    }
    assert metrics["sources"][source] == {"rows_written": 0, "corrupt_records": 0}
    assert metrics["removed_sources"] == []
    names = {path.name for path in lake.partition(source, J2).iterdir()}
    assert "_SUCCESS" in names
    assert not [name for name in names if not name.startswith(("_", "."))]
    table = read_table(
        lake.spark, f"{lake.cfg.paths.bronze}/{source}", bronze_schema(SOURCES[source])
    )
    assert table.where(F.col("ingestion_date") == J2).count() == 0
    for other in SOURCES:
        if other != source:
            assert lake.table(other).count() == manifest_rows(manifest, other), other


@pytest.mark.parametrize("source", ["orders", "customers", "products"])
def test_the_header_of_an_empty_csv_file_is_its_only_line_missing_from_bronze(lake, source):
    manifest = lake.land(J2)
    key = key_of(manifest, source)
    lake.rewrite(J2, manifest, key, header_line(lake, key) + b"\n", rows=0)
    lake.run(J2)
    paths = lake.cfg.paths

    report = {
        row.source: row
        for row in unmatched(lake.spark, paths.landing, paths.bronze).collect()
        if row.ingestion_date.isoformat() == J2
    }

    found = report[source]
    assert (found.landing_lines, found.bronze_rows) == (1, 0)
    assert (found.landing_not_in_bronze, found.bronze_not_in_landing) == (1, 0)


@pytest.mark.parametrize("source", ["orders", "customers", "products"])
def test_a_csv_file_without_any_byte_is_refused_before_any_write(lake, source):
    manifest = lake.land(J2)
    key = key_of(manifest, source)
    lake.rewrite(J2, manifest, key, b"", rows=0)

    with pytest.raises(LandingError, match=f"{key}: no header line"):
        lake.run(J2)

    [row] = lake.audit()
    assert row.status == "FAILED"
    assert row.error_message.startswith(f"LandingError: {key}: no header line")
    assert json.loads(row.metrics_json)["files"][key]["landing_lines"] == 0
    assert not any(lake.partition(s, J2).exists() for s in SOURCES)


def manifest_rows(manifest, source):
    return sum(e["rows"] for e in manifest["files"] if e["source"] == source)
