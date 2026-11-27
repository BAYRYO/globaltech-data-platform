import ast
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from gtdp.io import INGESTION_DATE, read_table
from gtdp.publish import SchemaMismatchError, publish

pytestmark = pytest.mark.slow

JOBS = Path(__file__).resolve().parents[2] / "src" / "gtdp" / "jobs"
D1, D2 = date(2026, 1, 1), date(2026, 1, 2)
SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField(INGESTION_DATE, DateType()),
    ]
)


def rows(spark, *days: date, schema: str = "id int, name string, ingestion_date date"):
    return spark.createDataFrame([(i, f"n{i}", day) for i, day in enumerate(days)], schema)


def test_a_partitioned_table_gets_the_named_partitions_replaced(spark, tmp_path):
    path = str(tmp_path / "table")
    publish(rows(spark, D1, D2), path, SCHEMA, (INGESTION_DATE,), [D1, D2])

    publish(rows(spark, D2, D2), path, SCHEMA, (INGESTION_DATE,), [D2])

    table = read_table(spark, path, SCHEMA)
    assert sorted(r[INGESTION_DATE] for r in table.collect()) == [D1, D2, D2]
    assert sorted(p.name for p in Path(path).iterdir() if "=" in p.name) == [
        f"{INGESTION_DATE}={D1}",
        f"{INGESTION_DATE}={D2}",
    ]


def test_a_table_without_partition_column_is_replaced_whole(spark, tmp_path):
    path = str(tmp_path / "table")
    publish(rows(spark, D1, D1), path, SCHEMA)

    publish(rows(spark, D2), path, SCHEMA)

    assert [r[INGESTION_DATE] for r in read_table(spark, path, SCHEMA).collect()] == [D2]
    assert not [p for p in Path(path).iterdir() if "=" in p.name]


@pytest.mark.parametrize(
    ("columns", "named"),
    [
        ("id int, name string, ingestion_date date, extra string", "not declared ['extra']"),
        ("id int, ingestion_date date", "missing ['name']"),
        ("id bigint, name string, ingestion_date date", "id: bigint instead of int"),
        ("name string, id int, ingestion_date date", "order"),
    ],
)
def test_rows_without_the_declared_columns_are_refused_before_any_write(
    spark, tmp_path, columns, named
):
    path = tmp_path / "table"
    frame = spark.createDataFrame([], columns)

    with pytest.raises(SchemaMismatchError, match=named):
        publish(frame, str(path), SCHEMA, (INGESTION_DATE,), [D1])

    assert not path.exists()


def test_columns_in_another_order_pass_where_the_order_does_not_count(spark, tmp_path):
    path = str(tmp_path / "table")
    frame = rows(spark, D1, schema="name string, id int, ingestion_date date")

    publish(frame, path, SCHEMA, (INGESTION_DATE,), [D1], ordered=False)

    assert read_table(spark, path, SCHEMA).count() == 1


def test_partitions_named_for_a_table_without_partition_column_are_refused(spark, tmp_path):
    path = tmp_path / "table"

    with pytest.raises(ValueError, match="no partition column"):
        publish(rows(spark, D1), str(path), SCHEMA, (), [D1])

    assert not path.exists()


@pytest.mark.parametrize("job", ["bronze", "silver", "gold"])
def test_the_batch_jobs_write_lake_tables_through_publish_only(job):
    """``publish`` checks the declared columns and takes the partition columns from the
    registry; a direct write would skip both."""
    tree = ast.parse((JOBS / f"{job}.py").read_text(encoding="utf-8"))
    direct = [
        f"line {node.lineno}: {node.func.id}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"write_partitions", "replace_table"}
    ]
    assert not direct, f"jobs/{job}.py writes around publish: {direct}"
