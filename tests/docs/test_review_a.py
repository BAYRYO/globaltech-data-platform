"""Review A of PR #21 (KAN-54): what the dictionary says against what the code does.

Each test runs the code on a concrete case and checks that the dictionary states the result. The
reviewer's versions asserted the dictionary's claim on the code and failed; here the code's
behaviour is the expected value, and the dictionary sentence that describes it is asserted too.
"""

import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from gold.silver_lake import J1, frame
from gtdp.bronze.transform import ReconciliationError
from gtdp.config import load_config
from gtdp.gold import dimensions, kpis
from gtdp.gold.kpis import KPIS, Star
from gtdp.gold.tables import DIM_CUSTOMER, DIM_PRODUCT, FACT_SALES, FACT_WEB_EVENTS
from gtdp.io import read_table
from gtdp.jobs import bronze as bronze_job
from gtdp.jobs import run as run_py
from gtdp.schemas import SILVER_ORDERS, SILVER_REJECTS
from gtdp.silver import products as silver_products
from silver.lakes import DAYS, SALT, Lake, drops, lake_config, local

pytestmark = pytest.mark.slow

ROOT = Path(__file__).resolve().parents[2]
DICTIONARY = ROOT / "docs" / "data-dictionary"


def section(page: str, heading: str) -> str:
    """The text of ``page`` from the line ``heading`` to the next heading of the same level."""
    text = (DICTIONARY / page).read_text(encoding="utf-8")
    level = heading.split(" ")[0]
    start = text.index(f"\n{heading}\n")
    end = re.search(rf"\n{level} ", text[start + 1 :])
    return text[start : start + 1 + end.start()] if end else text[start:]


def row(text: str, first_cell: str) -> list[str]:
    """The cells of the first table row of ``text`` whose first cell is ``first_cell``."""
    line = next(line for line in text.splitlines() if line.startswith(f"| {first_cell} |"))
    return [cell.strip() for cell in re.split(r"(?<!\\)\|", line)[1:-1]]


def test_f3_each_dim_product_version_has_its_own_name(spark):
    """gold.md said « Nom de la version initiale »: each version keeps the name it was sent
    with, as silver historises it (``HISTORISED`` of ``gtdp.silver.products``)."""
    clean = spark.createDataFrame(
        [
            ("P1", "Lamp", "Lighting", Decimal("10.00"), datetime(2025, 1, 1), J1, "h1"),
            ("P1", "Lamp Pro", "Lighting", Decimal("10.00"), datetime(2025, 6, 1), J1, "h2"),
        ],
        "product_id string, product_name string, category string, unit_price_eur decimal(10,2),"
        " updated_at timestamp, ingestion_date date, _record_hash string",
    )

    dim_product = dimensions.products(silver_products.table(clean)).orderBy("version").collect()

    assert [(v.version, v.product_name) for v in dim_product] == [(1, "Lamp"), (2, "Lamp Pro")]
    rule = row(section("gold.md", "## `gold/dim_product`"), "`product_name`")[2]
    assert "Nom de la version" in rule and "initiale" not in rule


def test_f4_run_py_writes_an_audit_row_for_each_job_it_starts_only(tmp_path):
    """ops.md said « Chaque exécution de `run.py` écrit une ligne ». ``run.py`` returns 2 before
    the audited run when the configuration is invalid: no row. A configuration error raised by
    the job itself also returns 2, after the audited run has written its row."""
    ops = tmp_path / "ops"

    code = run_py.main(
        ["smoke", "--env", "local", "--set", f"paths.ops={ops}", "--set", "gold.top_n=0"]
    )

    assert code == 2
    assert not list(ops.glob("audit/run_date=*/run_id=*"))

    text = section("ops.md", "## `ops/audit`")
    assert "code 2" in text and "aucune ligne" in text and "`list`" in text


def test_f4_a_configuration_error_of_the_job_writes_a_failed_row(spark, tmp_path, monkeypatch):
    """The gold job raises ``ConfigError`` without ``--date``: ``run.py`` returns 2 and the
    audit has a ``FAILED`` row. ``build_spark`` and ``spark.stop`` are the test's session."""
    lake = tmp_path / "lake"
    monkeypatch.setattr(run_py, "build_spark", lambda cfg, name: spark)
    monkeypatch.setattr(type(spark), "stop", lambda self: None)

    code = run_py.main(["gold", "--env", "local", "--set", f"paths.ops={lake}/ops"])

    assert code == 2
    [audit] = read_table(spark, f"{lake}/ops/audit").collect()
    assert (audit.job_name, audit.status) == ("gold", "FAILED")
    assert audit.error_message == "ConfigError: gold needs --date"


def test_f5_a_failed_read_back_leaves_the_bronze_partition_written(spark, tmp_path, monkeypatch):
    """bronze.md said every check runs « avant toute écriture » and a failing file writes
    nothing. The read-back runs after the write: the partition stays."""
    monkeypatch.setenv("GTDP_PSEUDO_SALT", SALT)
    lake = Lake(spark, lake_config(tmp_path), drops(tmp_path / "generated", 42))
    lake.land(DAYS[0])
    monkeypatch.setattr(
        bronze_job, "read_table", lambda *args, **kwargs: read_table(*args, **kwargs).limit(0)
    )

    with pytest.raises(ReconciliationError, match="written 0"):
        lake.run("bronze", DAYS[0], "products")

    partition = local(lake.cfg.paths.bronze) / "products" / f"ingestion_date={DAYS[0]}"
    assert list(partition.glob("*.parquet"))
    checks = (DICTIONARY / "bronze.md").read_text(encoding="utf-8")
    assert row(checks, "Relecture")[1] == "après l'écriture"
    assert "la partition reste écrite" in checks


def test_f6_the_median_price_ratio_is_over_ratios_rounded_to_13_decimals(spark):
    """gold.md said « en `double` »: the ratio of two ``decimal(10,2)`` is a ``decimal(23,13)``,
    rounded to 13 decimals before the cast."""
    sale = {
        "transaction_id": "T1",
        "product_sk": "P1-19000101000000",
        "unit_price_invoiced": Decimal("1.00"),
        "applicable_unit_price": Decimal("3.00"),
        "order_date": date(2025, 12, 15),
    }
    sales = frame(spark, FACT_SALES.schema, [sale])
    products = frame(
        spark, DIM_PRODUCT.schema, [{"product_sk": "P1-19000101000000", "category": "A"}]
    )

    (result,) = kpis.price_deviation(sales, products).collect()

    assert result.median_price_ratio == 0.3333333333333
    formula = row(section("gold.md", "### `gold/kpi_price_deviation`"), "`median_price_ratio`")
    assert "`decimal(23,13)`" in formula[1] and "13 décimales" in formula[1]


def test_f7_each_indicator_names_its_grain_among_its_columns(spark):
    """gold.md gives the key of a ``kpi_*`` as its « Grain » row: every indicator has one,
    and it names columns of the indicator."""
    empty = [
        spark.createDataFrame([], schema)
        for schema in (
            FACT_SALES.schema,
            FACT_WEB_EVENTS.schema,
            DIM_CUSTOMER.schema,
            DIM_PRODUCT.schema,
            SILVER_ORDERS,
            SILVER_REJECTS,
        )
    ]
    settings = load_config("local").gold
    wrong = {}
    for name, build in KPIS.items():
        grains = [
            line
            for line in section("gold.md", f"### `gold/{name}`").splitlines()
            if line.startswith("| Grain |")
        ]
        columns = set(build(Star(*empty), settings).columns)
        named = [
            re.match(r"\w+", chunk).group()
            for line in grains
            for chunk in re.findall(r"`([^`]+)`", line.split(" ; ")[0])
        ]
        if len(grains) != 1 or not named or not set(named) <= columns:
            wrong[name] = grains
    assert not wrong, f"no Grain row, or one naming columns the indicator lacks: {wrong}"


WRITERS = (
    ("/user/etl/smoke", "scripts/cluster/smoke/run.sh", "WORK=/user/etl/smoke"),
    ("/apps/spark/", "docker/conf/spark/spark-defaults.conf", "8020/apps/spark/spark-jars"),
    ("ops/mr_vs_spark/mapreduce", "src/gtdp/perf/mr_compare.py", "ops}/mr_vs_spark/mapreduce"),
)
"""(path written, file of the repository that writes it, the text of that file naming it)."""


def test_f8_the_inventory_lists_every_path_the_platform_writes():
    """README.md: « Tout ce que la plateforme écrit, avec le code qui l'écrit ». The three
    paths were in neither table."""
    inventory = section("README.md", "## Inventaire")
    for path, writer, written in WRITERS:
        assert written in (ROOT / writer).read_text(encoding="utf-8")
        assert f"`{path}" in inventory, f"{writer} writes {path}, absent from the inventory"
    assert "`ops/mr_vs_spark/mapreduce/`" in (DICTIONARY / "ops.md").read_text(encoding="utf-8")
