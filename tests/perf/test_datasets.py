"""The benchmark tables hold the same rows in every layout, and the queries agree across them."""

import json
import shutil
import subprocess
from argparse import Namespace
from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrameReader
from pyspark.sql import functions as F

from gtdp.io import read_table
from gtdp.perf import bench
from gtdp.perf import datasets as ds
from gtdp.perf.mr_compare import QUALITY_CODES
from gtdp.silver.products import with_applicable_price

pytestmark = pytest.mark.slow
DAYS = ("2026-01-01", "2026-01-02", "2026-01-03")


class LocalHdfs:
    """``hdfs dfs -mkdir -p`` and ``hdfs dfs -put -f`` on the local disk."""

    def run(self, command, check=False, **_):
        args = command[1:]
        if args[:3] == ["dfs", "-mkdir", "-p"]:
            Path(args[3]).mkdir(parents=True, exist_ok=True)
        elif args[:3] == ["dfs", "-put", "-f"]:
            for path in args[3:-1]:
                shutil.copy(path, args[-1])
        else:
            raise AssertionError(f"unexpected command {command}")
        return subprocess.CompletedProcess(command, 0)


@pytest.fixture(scope="module")
def prepared(spark, tmp_path_factory):
    """The ``ci`` profile landed by ``bench land`` (``<source>/ingestion_date=<day>/``), then
    prepared, with the folders land wrote and the paths prepare read."""
    out = tmp_path_factory.mktemp("bench")
    landing, root = out / "landing", (out / "perf").as_uri()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(bench.subprocess, "run", LocalHdfs().run)
        written = bench.land(Namespace(profile="ci", seed=42, landing=str(landing)))
    read, text = [], DataFrameReader.text
    with pytest.MonkeyPatch.context() as patch:

        def recorded(self, paths, *args, **kwargs):
            read.append(paths)
            return text(self, paths, *args, **kwargs)

        patch.setattr(DataFrameReader, "text", recorded)
        counts = ds.prepare(spark, landing.as_uri(), root)
    return landing, root, counts, written, read


def test_land_puts_every_source_that_prepare_reads(prepared):
    """docs/performance.md#reproduire lands the data with bench.sh land, then runs prepare."""
    landing, _, _, written, read = prepared
    landed = {Path(folder).parent.name for folder in written}
    assert landed == {*ds.LANDED, "_manifests"}
    assert sorted(read) == sorted(f"{landing.as_uri()}/{source}" for source in ds.LANDED)
    assert {Path(folder).name for folder in written} == {f"ingestion_date={d}" for d in DAYS}


def test_every_layout_holds_the_same_rows(prepared):
    counts = prepared[2]
    for source in ds.SCHEMAS:
        assert len({counts[f"{layout.name}/{source}"] for layout in ds.LAYOUTS}) == 1


def test_the_orders_are_the_lines_that_pass_the_lot_6_rules(prepared):
    landing, _, counts = prepared[:3]
    lines = rejected = 0
    for day in DAYS:
        [csv] = (landing / "orders" / f"ingestion_date={day}").iterdir()
        lines += len(csv.read_text(encoding="utf-8").splitlines()) - 1
        manifest = landing / "_manifests" / f"ingestion_date={day}" / "_MANIFEST.json"
        manifest = json.loads(manifest.read_text(encoding="utf-8"))
        [orders] = [f for f in manifest["files"] if f["source"] == "orders"]
        rejected += sum(n for code, n in orders["anomalies"].items() if code in QUALITY_CODES)
    assert counts["csv/orders"] == lines - rejected


def test_the_revenue_query_gives_the_same_rows_on_every_layout(spark, prepared):
    root = prepared[1]
    results = set()
    for layout in ds.LAYOUTS:
        tables = [ds.read(spark, root, layout, name) for name in ds.SCHEMAS]
        rows = ds.revenue_by_day(*tables, date(2025, 6, 1), date(2025, 6, 30)).collect()
        assert rows
        results.add(tuple(sorted(tuple(row) for row in rows)))
    assert len(results) == 1


def test_the_partitioned_layout_has_one_directory_per_order_date(spark, prepared):
    root = prepared[1]
    orders = ds.read(spark, root, ds.LAYOUT["parquet_part"], "orders")
    plan = orders.where(F.col("order_date") == date(2025, 6, 1))._jdf.queryExecution()
    assert "PartitionFilters: [isnotnull(order_date" in plan.executedPlan().toString()


def test_each_version_ends_where_the_next_one_starts(spark, prepared):
    root, counts = prepared[1:3]
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
