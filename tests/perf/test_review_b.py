"""Review B of PR #20 (tools, code, reproducibility).

The first three tests fail on 9d1da61: each one is a finding of the review. The tests after them
pass: they are the evidence that ``salted_applicable_price`` returns what gold's
``with_applicable_price`` returns, on the edge cases gold handles and on the ci data.

Adapted in the fix round where a decision of the lead changed the mechanism, not the intent:
the storage tool now checks its run area and sets its policy, so the fake hdfs answers those
commands too (decision 2); the documented landing is now one command, ``bench.sh land``, instead
of a shell loop (decision 1), and ``test_datasets.py::test_land_puts_every_source_that_prepare_
reads`` proves that this command lands what ``prepare`` reads.
"""

import random
import re
import shutil
from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from gtdp.config import load_config
from gtdp.datagen.__main__ import main as datagen
from gtdp.perf import datasets as ds
from gtdp.perf import storage
from gtdp.perf.bench import Run, markdown, summarize
from gtdp.perf.revenue_by_country import customer_countries
from gtdp.perf.skew import salted_applicable_price
from gtdp.silver.products import with_applicable_price

from .test_storage import FakeHdfs

REPO = Path(__file__).resolve().parents[2]
KAN52 = ("/benchmarks/ec", "/benchmarks/rep3")


def test_rerunning_the_storage_benchmark_leaves_the_kan52_files_alone(monkeypatch, tmp_path):
    """docs/performance.md#reproduire runs storage.sh again and asks to keep /benchmarks/ec and
    /benchmarks/rep3 for KAN-52, whose block layout is committed in docs/perf-evidence/storage."""
    fake = FakeHdfs(
        existing=[f"{folder}/a.csv" for folder in KAN52], read_s={"rep3": [1, 1], "ec": [1, 1]}
    )
    monkeypatch.setattr(storage.subprocess, "run", fake.run)
    monkeypatch.setattr(storage.time, "perf_counter", fake.perf_counter)
    storage.main(["--source", "/scratch/a.csv", "--runs", "1", "--out", str(tmp_path)])
    rewrites = [c for c in fake.commands if "-cp" in c and c[-1].rstrip("/") in KAN52]
    assert rewrites == []
    assert any("-cp" in c for c in fake.commands)


def test_the_documented_landing_holds_every_source_that_prepare_reads():
    """docs/performance.md#reproduire lands the data with bench.sh land, then runs bench.sh
    prepare, which reads <landing>/products, and storage.sh --small .../products/...csv; the
    campaign of docs/mapreduce-vs-spark.md lands its ci and full inputs the same way."""
    steps = (REPO / "docs" / "performance.md").read_text(encoding="utf-8").split("## Reproduire")[1]
    assert steps.index("scripts/perf/bench.sh land") < steps.index("scripts/perf/bench.sh prepare")
    landing = load_config("cluster").perf.landing.removeprefix("hdfs://namenode:8020")
    cited = re.findall(r"--(?:source|small) (\S+)", steps)
    assert cited
    for path in cited:
        layout = rf"{landing}/(\w+)/landing/(\w+)/ingestion_date=[\d-]+/\S+"
        profile, source = re.fullmatch(layout, path).groups()
        assert profile == "full" or f"bench.sh land --profile {profile}" in steps
        assert source in ds.LANDED
    campaign = (REPO / "docs" / "mapreduce-vs-spark.md").read_text(encoding="utf-8")
    campaign = campaign.split("## Campagne de mesure")[1].split("### Résultats")[0]
    assert not re.findall(r"for s in ([a-z ]+); do", campaign)
    for profile in ("ci", "full"):
        assert f"scripts/perf/bench.sh land --profile {profile}" in campaign
    assert campaign.index("bench.sh land") < campaign.index("scripts/mapreduce/compare.sh")


def run(variant: str, number: int) -> Run:
    return Run(variant, number, f"x/{variant}/{number}", 0, 1.0, "0 other", [1], {}, {}, {})


def test_a_run_that_another_application_overlapped_shows_in_the_report():
    """The tool records overlaps (runs.json ``overlapping``) but report.md, the table the doc
    copies, and the exit code do not show them: an overlapped run enters the median silently."""
    runs = [run("csv", n) for n in (0, 1, 2)]
    runs.append(Run("csv", 3, "x/csv/3", 0, 9.0, "0 other", [1], overlapping=["application_7"]))
    assert "application_7" in markdown(summarize(runs))


ORDERS = "transaction_id STRING, product_id STRING, event_time TIMESTAMP, quantity INT"
END = datetime(9999, 12, 31)


def at(day: int, hour: int = 0) -> datetime:
    return datetime(2025, 3, day, hour)


EDGE_ORDERS = [
    ("at-valid-from", "P1", at(10), 1),
    ("at-valid-to", "P1", at(20), 1),
    ("before-first", "P1", at(1), 1),
    ("inside", "P1", at(15, 12), 2),
    ("overlap", "P2", at(12), 1),
    ("no-version", "P9", at(12), 1),
    ("null-product", None, at(12), 1),
    ("null-time", "P1", None, 1),
    ("dup", "P1", at(11), 1),
    ("dup", "P1", at(11), 1),
    ("null-valid-to", "P3", at(12), 1),
]
EDGE_VERSIONS = [
    ("P1", 1, Decimal("5.00"), at(10), at(20)),
    ("P1", 2, Decimal("6.00"), at(20), END),
    ("P2", 3, Decimal("7.00"), at(1), at(15)),
    ("P2", 4, Decimal("8.00"), at(10), END),
    (None, 5, Decimal("9.00"), at(1), END),
    ("P3", 6, Decimal("1.00"), at(1), None),
]


def multiset(frame) -> Counter:
    return Counter(tuple(row) for row in frame.collect())


@pytest.mark.parametrize("salts", [1, 2, 3, 7, 16])
def test_salting_keeps_every_edge_case_of_the_gold_join(spark, salts):
    orders = spark.createDataFrame(EDGE_ORDERS, ORDERS)
    versions = spark.createDataFrame(EDGE_VERSIONS, ds.VERSIONS)
    plain = with_applicable_price(orders, versions)
    salted = salted_applicable_price(orders, versions, salts)
    assert salted.columns == plain.columns
    assert salted.schema == plain.schema
    assert multiset(salted) == multiset(plain)
    priced = {r.transaction_id: r.product_sk for r in plain.collect()}
    assert (priced["at-valid-from"], priced["at-valid-to"], priced["before-first"]) == (1, 2, None)


@pytest.mark.parametrize("seed", range(4))
def test_salting_keeps_the_gold_join_on_random_frames(spark, seed):
    rng = random.Random(seed)
    start = datetime(2025, 1, 1)
    orders = [
        (
            f"T{i}",
            rng.choice(["P1", "P2", "P3", None]),
            start + timedelta(hours=rng.randrange(400)),
            1,
        )
        for i in range(300)
    ]
    versions = []
    for sk, product in enumerate(["P1", "P1", "P2", "P2", "P2", "P3"]):
        begin = start + timedelta(hours=rng.randrange(300))
        versions.append(
            (product, sk, Decimal("1.00"), begin, begin + timedelta(hours=rng.randrange(1, 200)))
        )
    o = spark.createDataFrame(orders, ORDERS)
    v = spark.createDataFrame(versions, ds.VERSIONS)
    for salts in (1, 5, 16):
        assert multiset(salted_applicable_price(o, v, salts)) == multiset(
            with_applicable_price(o, v)
        )


@pytest.mark.slow
def test_salting_keeps_the_gold_join_on_the_ci_data(spark, tmp_path):
    out = tmp_path / "generated"
    assert datagen(["generate", "--profile", "ci", "--seed", "42", "--out", str(out)]) == 0
    landing = out / "landing"
    for day in ("2026-01-01", "2026-01-02", "2026-01-03"):
        for source in ("orders", "customers", "products"):
            shutil.copytree(out / "ci" / "landing" / day / source, landing / source / day)
    text = {s: spark.read.text(str(landing / s)) for s in ("orders", "customers", "products")}
    orders = ds.valid_orders(text["orders"], customer_countries(text["customers"]))
    versions = ds.scd2(ds.product_versions(text["products"]))
    plain = multiset(with_applicable_price(orders, versions))
    assert sum(plain.values()) == orders.count()
    assert multiset(salted_applicable_price(orders, versions, 16)) == plain
