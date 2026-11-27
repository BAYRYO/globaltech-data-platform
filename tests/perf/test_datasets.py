"""The benchmark tables hold the same rows in every layout, and the queries agree across them."""

import json
import shutil
from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from gtdp.datagen.__main__ import main as datagen
from gtdp.io import read_table
from gtdp.perf import datasets as ds
from gtdp.perf.mr_compare import QUALITY_CODES
from gtdp.silver.products import with_applicable_price

pytestmark = pytest.mark.slow
DAYS = ("2026-01-01", "2026-01-02", "2026-01-03")
SOURCES = ("orders", "customers", "products")


@pytest.fixture(scope="module")
def prepared(spark, tmp_path_factory):
    """The ``ci`` profile landed as on HDFS (``<source>/ingestion_date=<day>/``), prepared."""
    out = tmp_path_factory.mktemp("generated")
    assert datagen(["generate", "--profile", "ci", "--seed", "42", "--out", str(out)]) == 0
    drops, landing = out / "ci" / "landing", out / "landing"
    for day in DAYS:
        for source in SOURCES:
            shutil.copytree(drops / day / source, landing / source / f"ingestion_date={day}")
    root = out / "perf"
    counts = ds.prepare(spark, landing.as_uri(), root.as_uri())
    return drops, root.as_uri(), counts


def test_every_layout_holds_the_same_rows(prepared):
    _, _, counts = prepared
    for source in SOURCES:
        assert len({counts[f"{layout.name}/{source}"] for layout in ds.LAYOUTS}) == 1


def test_the_orders_are_the_lines_that_pass_the_lot_6_rules(prepared):
    drops, _, counts = prepared
    lines = rejected = 0
    for day in DAYS:
        [csv] = (drops / day / "orders").iterdir()
        lines += len(csv.read_text(encoding="utf-8").splitlines()) - 1
        manifest = json.loads((drops / day / "_MANIFEST.json").read_text(encoding="utf-8"))
        [orders] = [f for f in manifest["files"] if f["source"] == "orders"]
        rejected += sum(n for code, n in orders["anomalies"].items() if code in QUALITY_CODES)
    assert counts["csv/orders"] == lines - rejected


def test_the_revenue_query_gives_the_same_rows_on_every_layout(spark, prepared):
    _, root, _ = prepared
    results = set()
    for layout in ds.LAYOUTS:
        tables = [ds.read(spark, root, layout, name) for name in ds.SCHEMAS]
        rows = ds.revenue_by_day(*tables, date(2025, 6, 1), date(2025, 6, 30)).collect()
        assert rows
        results.add(tuple(sorted(tuple(row) for row in rows)))
    assert len(results) == 1


def test_the_partitioned_layout_has_one_directory_per_order_date(spark, prepared):
    _, root, _ = prepared
    orders = ds.read(spark, root, ds.LAYOUT["parquet_part"], "orders")
    plan = orders.where(F.col("order_date") == date(2025, 6, 1))._jdf.queryExecution()
    assert "PartitionFilters: [isnotnull(order_date" in plan.executedPlan().toString()


def test_each_version_ends_where_the_next_one_starts(spark, prepared):
    _, root, counts = prepared
    versions = read_table(spark, f"{root}/versions", ds.VERSIONS).collect()
    by_product = {}
    for version in sorted(versions, key=lambda v: (v.product_id, v.valid_from)):
        by_product.setdefault(version.product_id, []).append(version)
    for chain in by_product.values():
        assert [v.valid_to for v in chain[:-1]] == [v.valid_from for v in chain[1:]]
        assert chain[-1].valid_to == datetime(9999, 12, 31)
    orders = ds.read(spark, root, ds.LAYOUT["parquet"], "orders")
    assert (
        with_applicable_price(orders, spark.createDataFrame(versions, ds.VERSIONS)).count()
        == counts["parquet/orders"]
    )
