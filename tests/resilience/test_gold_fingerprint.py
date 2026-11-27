import importlib.util
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

from gtdp.gold.kpis import KPIS
from gtdp.gold.tables import DIM_CUSTOMER, FACT_SALES, STAR

REPO = Path(__file__).resolve().parents[2]
SALES = "transaction_id string, revenue int, coupon string, order_date date"

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def fingerprint():
    spec = importlib.util.spec_from_file_location(
        "gold_fingerprint", REPO / "scripts" / "resilience" / "gold_fingerprint.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def checksums(fingerprint, spark, schema, *rows):
    return [fingerprint.checksum(spark.createDataFrame([row], schema)) for row in rows]


def test_a_value_moved_to_a_null_neighbour_column_changes_the_checksum(fingerprint, spark):
    before, after = checksums(
        fingerprint, spark, "id int, a int, b int", (7, 1, None), (7, None, 1)
    )
    assert before != after


def test_a_customer_whose_region_moved_to_segment_changes_the_checksum(fingerprint, spark):
    customer = ("C1", "FR", "EMEA", None, None, None, [])
    moved = ("C1", "FR", None, "EMEA", None, None, [])
    before, after = checksums(fingerprint, spark, DIM_CUSTOMER.schema, customer, moved)
    assert before != after


def test_a_null_and_the_text_null_have_different_checksums(fingerprint, spark):
    absent, text = checksums(fingerprint, spark, "id int, coupon string", (1, None), (1, "null"))
    assert absent != text


def test_a_microsecond_changes_the_checksum(fingerprint, spark):
    early, late = checksums(
        fingerprint,
        spark,
        "id int, at timestamp",
        (1, datetime(2026, 1, 3, 10, 0, 0, 1)),
        (1, datetime(2026, 1, 3, 10, 0, 0, 2)),
    )
    assert early != late


def test_the_checksum_does_not_depend_on_row_order(fingerprint, spark):
    rows = [(1, "a", None), (2, None, "b"), (3, "c", "c")]
    schema = "id int, a string, b string"
    forward = spark.createDataFrame(rows, schema).repartition(3)
    backward = spark.createDataFrame(rows[::-1], schema).coalesce(1)
    assert fingerprint.checksum(forward) == fingerprint.checksum(backward)
    assert fingerprint.checksum(forward)["rows"] == 3


def write_gold(spark, root: Path, sales: list[tuple]) -> str:
    """A gold of one-row tables, with ``fact_sales`` partitioned by ``order_date``."""
    for name in [table.name for table in STAR] + list(KPIS):
        if name != FACT_SALES.name:
            spark.createDataFrame([(1, name)], "id int, name string").write.parquet(
                str(root / name)
            )
    frame = spark.createDataFrame(sales, SALES)
    frame.write.partitionBy("order_date").parquet(str(root / FACT_SALES.name))
    return str(root)


def test_the_fingerprint_names_the_fact_sales_partition_that_differs(fingerprint, spark, tmp_path):
    sales = [
        ("T1", 10, None, date(2026, 1, 2)),
        ("T2", 20, "WELCOME", date(2026, 1, 2)),
        ("T3", 30, None, date(2026, 1, 3)),
    ]
    moved = [*sales[:2], ("T3", None, "30", date(2026, 1, 3))]
    same = fingerprint.fingerprint(spark, write_gold(spark, tmp_path / "a", sales))
    again = fingerprint.fingerprint(spark, write_gold(spark, tmp_path / "b", sales[::-1]))
    other = fingerprint.fingerprint(spark, write_gold(spark, tmp_path / "c", moved))

    assert same == again
    assert len(same["tables"]) == len(STAR) + len(KPIS)
    assert same["fact_sales_partitions"]["2026-01-02"]["rows"] == 2
    differing = {name for name in same["tables"] if same["tables"][name] != other["tables"][name]}
    assert differing == {FACT_SALES.name}
    partitions = same["fact_sales_partitions"], other["fact_sales_partitions"]
    assert partitions[0]["2026-01-02"] == partitions[1]["2026-01-02"]
    assert partitions[0]["2026-01-03"] != partitions[1]["2026-01-03"]
