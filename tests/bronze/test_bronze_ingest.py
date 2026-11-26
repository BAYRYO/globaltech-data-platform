import hashlib
import json
from collections import Counter

import pytest
from pyspark.sql import functions as F

from gtdp.audit import audit_path
from gtdp.bronze.report import discounts, file_counts, partitions, runs, unmatched

pytestmark = pytest.mark.slow

DAYS = ("2026-01-01", "2026-01-02", "2026-01-03")


@pytest.fixture(scope="module")
def ingested(module_lake):
    """J1, J2 and J3 landed, then ingested one day after the other."""
    manifests = {day: module_lake.land(day) for day in DAYS}
    runs = {day: module_lake.run(day) for day in DAYS}
    return module_lake, manifests, runs


def landed_files(manifests):
    return [(day, entry) for day, manifest in manifests.items() for entry in manifest["files"]]


def line_hashes(path, has_header):
    lines = path.read_bytes().decode("utf-8").split("\n")
    assert lines.pop() == "", f"{path} does not end with a line break"
    return Counter(hashlib.sha256(line.encode()).hexdigest() for line in lines[has_header:])


def test_each_day_and_source_is_a_parquet_partition_with_the_manifest_row_count(ingested):
    lake, manifests, _ = ingested

    for day, entry in landed_files(manifests):
        partition = lake.partition(entry["source"], day)
        data_files = [p.name for p in partition.iterdir() if not p.name.startswith(("_", "."))]
        assert data_files, partition
        assert all(name.endswith(".parquet") for name in data_files), data_files
        rows = lake.table(entry["source"]).where(F.col("ingestion_date") == day)
        assert rows.count() == entry["rows"], (day, entry["path"])


def test_every_landing_line_is_in_bronze_once_with_its_sha256_as_record_hash(ingested):
    lake, manifests, _ = ingested

    for day, entry in landed_files(manifests):
        source_file = lake.landing_uri(day, entry["path"])
        hashes = Counter(
            row._record_hash
            for row in lake.table(entry["source"])
            .where(F.col("_source_file") == source_file)
            .select("_record_hash")
            .collect()
        )
        expected = line_hashes(lake.landing_file(day, entry["path"]), entry["source"] != "weblogs")
        assert hashes == expected, (day, entry["path"])


def test_only_malformed_rows_are_corrupt_and_they_keep_their_raw_line(ingested):
    lake, manifests, _ = ingested

    for day, entry in landed_files(manifests):
        corrupt = (
            lake.table(entry["source"])
            .where((F.col("ingestion_date") == day) & F.col("_corrupt_record").isNotNull())
            .collect()
        )
        assert len(corrupt) == entry["anomalies"].get("MALFORMED_ROW", 0), (day, entry["path"])
        lines = set(lake.landing_file(day, entry["path"]).read_text().splitlines())
        assert {row._corrupt_record for row in corrupt} <= lines


def test_padded_values_are_kept_as_they_are(ingested):
    lake, _, _ = ingested

    emails = [row.email for row in lake.table("customers").select("email").collect() if row.email]

    assert any(email != email.strip() for email in emails)


def test_rows_carry_their_landing_file_and_the_run_that_wrote_them(ingested):
    lake, manifests, runs = ingested
    started = {row.run_id: row.started_at for row in lake.audit()}

    for day, entry in landed_files(manifests):
        meta = (
            lake.table(entry["source"])
            .where(F.col("ingestion_date") == day)
            .select("_source_file", "_run_id", "_ingested_at")
            .distinct()
            .collect()
        )
        run_id = runs[day].run_id
        assert [tuple(row) for row in meta] == [
            (lake.landing_uri(day, entry["path"]), run_id, started[run_id])
        ]


def test_the_audit_row_reconciles_each_file_with_its_manifest(ingested):
    lake, manifests, runs = ingested
    rows = {row.run_id: row for row in lake.audit()}

    for day, manifest in manifests.items():
        row = rows[runs[day].run_id]
        total = sum(entry["rows"] for entry in manifest["files"])
        assert (row.job_name, row.layer, row.status) == ("bronze", "bronze", "SUCCESS")
        assert (row.rows_in, row.rows_out) == (total, total)
        metrics = json.loads(row.metrics_json)
        for entry in manifest["files"]:
            counts = metrics["files"][entry["path"]]
            assert counts["manifest_rows"] == entry["rows"]
            assert counts["landing_lines"] == counts["rows_read"] == counts["rows_written"]
            assert counts["corrupt_records"] == entry["anomalies"].get("MALFORMED_ROW", 0)
        assert set(metrics["sources"]) == {"orders", "customers", "products", "weblogs"}
        assert metrics["unknown_columns"] == {source: [] for source in metrics["sources"]}


def test_j3_orders_carry_discount_pct_that_earlier_days_read_as_null(ingested):
    lake, manifests, _ = ingested

    orders = lake.table("orders")
    per_day = {
        row.ingestion_date.isoformat(): (row.rows, row.with_discount)
        for row in orders.groupBy("ingestion_date")
        .agg(F.count(F.lit(1)).alias("rows"), F.count("discount_pct").alias("with_discount"))
        .collect()
    }

    assert per_day["2026-01-01"][1] == per_day["2026-01-02"][1] == 0
    j3_rows, j3_discounts = per_day["2026-01-03"]
    [j3] = [e for e in manifests["2026-01-03"]["files"] if e["source"] == "orders"]
    assert j3_rows == j3["rows"]
    # Rows resent from earlier days carry an empty discount_pct; malformed rows have no 9th field.
    assert 0 < j3_discounts < j3_rows


def test_the_report_counts_the_orders_holding_a_discount_per_day(ingested):
    lake, manifests, _ = ingested
    expected = {}
    for day in DAYS:
        [entry] = [e for e in manifests[day]["files"] if e["source"] == "orders"]
        header, *lines = lake.landing_file(day, entry["path"]).read_text().splitlines()
        if "discount_pct" not in header.split(","):
            expected[day] = (entry["rows"], 0)
            continue
        position = header.split(",").index("discount_pct")
        fields = [line.split(",") for line in lines]
        discounted = sum(1 for f in fields if len(f) > position and f[position])
        expected[day] = (entry["rows"], discounted)

    rows = discounts(lake.spark, lake.cfg.paths.bronze).collect()

    assert {r.ingestion_date.isoformat(): (r.rows, r.with_discount_pct) for r in rows} == expected
    assert 0 < expected[DAYS[2]][1] < expected[DAYS[2]][0]


def test_the_report_counts_each_partition_and_finds_only_csv_headers_missing(ingested):
    lake, manifests, runs = ingested
    paths = lake.cfg.paths
    summary = {
        (r.source, r.ingestion_date.isoformat()): r
        for r in partitions(lake.spark, paths.bronze).collect()
    }
    unmatched_rows = {
        (r.source, r.ingestion_date.isoformat()): r
        for r in unmatched(lake.spark, paths.landing, paths.bronze).collect()
    }

    assert (
        summary.keys()
        == unmatched_rows.keys()
        == {(e["source"], d) for d, e in landed_files(manifests)}
    )
    for day, entry in landed_files(manifests):
        key, is_csv = (entry["source"], day), entry["source"] != "weblogs"
        part, diff = summary[key], unmatched_rows[key]
        assert (part.rows, part.corrupt, part.files, part.run_id) == (
            entry["rows"],
            entry["anomalies"].get("MALFORMED_ROW", 0),
            1,
            runs[day].run_id,
        )
        header = lake.landing_file(day, entry["path"]).read_text().split("\n", 1)[0]
        assert (diff.landing_lines, diff.bronze_rows) == (entry["rows"] + is_csv, entry["rows"])
        assert (diff.landing_not_in_bronze, diff.bronze_not_in_landing) == (int(is_csv), 0)
        expected = [hashlib.sha256(header.encode()).hexdigest()] if is_csv else []
        assert (diff.landing_not_in_bronze_hashes or []) == expected
        assert not diff.bronze_not_in_landing_hashes


def test_the_report_lists_the_counts_of_each_run_and_file(ingested):
    lake, manifests, runs_by_day = ingested

    rows = file_counts(runs(lake.spark, audit_path(lake.cfg))).collect()

    expected = {
        (runs_by_day[day].run_id, entry["path"]): entry["rows"]
        for day, entry in landed_files(manifests)
    }
    assert {(row.run_id, row.file): row.rows_written for row in rows} == expected
    assert all(row.manifest_rows == row.landing_lines == row.rows_read for row in rows)
