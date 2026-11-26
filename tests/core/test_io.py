import os
from datetime import date

import pytest

from gtdp.io import list_partition_values, path_exists, replace_table, write_partitions

pytestmark = pytest.mark.slow

SCHEMA = "id int, ingestion_date string"


def rows(spark, path):
    return sorted(tuple(r) for r in spark.read.parquet(path).collect())


def test_write_partitions_twice_gives_the_same_table_and_keeps_other_days(spark, tmp_path):
    path = str(tmp_path / "orders")
    first = spark.createDataFrame([(1, "2026-01-01"), (2, "2026-01-02")], SCHEMA)
    replay = spark.createDataFrame([(3, "2026-01-02"), (4, "2026-01-02")], SCHEMA)
    write_partitions(first, path, ["ingestion_date"])

    write_partitions(replay, path, ["ingestion_date"])
    once = rows(spark, path)
    write_partitions(replay, path, ["ingestion_date"])

    assert rows(spark, path) == once
    jan_1, jan_2 = date(2026, 1, 1), date(2026, 1, 2)
    assert once == [(1, jan_1), (3, jan_2), (4, jan_2)]


def test_write_partitions_refuses_to_overwrite_a_whole_table(spark, tmp_path):
    df = spark.createDataFrame([(1, "2026-01-01")], SCHEMA)

    with pytest.raises(ValueError, match="partition column"):
        write_partitions(df, str(tmp_path / "orders"), [])


def test_replace_table_swaps_in_the_new_content(spark, tmp_path):
    path = str(tmp_path / "gold" / "dim")
    replace_table(spark.createDataFrame([(1, "a")], SCHEMA), path)

    replace_table(spark.createDataFrame([(2, "b")], SCHEMA), path)

    assert rows(spark, path) == [(2, "b")]
    assert os.listdir(tmp_path / "gold") == ["dim"]


def test_replace_table_accepts_a_dataframe_read_from_the_same_table(spark, tmp_path):
    path = str(tmp_path / "products_scd2")
    replace_table(spark.createDataFrame([(1, "a")], SCHEMA), path)
    previous = spark.read.parquet(path)

    replace_table(previous.union(spark.createDataFrame([(2, "b")], SCHEMA)), path)

    assert rows(spark, path) == [(1, "a"), (2, "b")]


def test_crashed_replace_keeps_the_old_table_and_the_next_run_converges(spark, tmp_path):
    path = str(tmp_path / "gold" / "dim")
    replace_table(spark.createDataFrame([(1, "a")], SCHEMA), path)
    failing = spark.range(4).selectExpr(
        "CAST(IF(id = 3, raise_error('crash mid-write'), id) AS INT) AS id", "'x' AS ingestion_date"
    )

    with pytest.raises(Exception, match="crash mid-write"):
        replace_table(failing, path)
    assert rows(spark, path) == [(1, "a")]

    replace_table(spark.createDataFrame([(2, "b")], SCHEMA), path)
    assert rows(spark, path) == [(2, "b")]
    assert os.listdir(tmp_path / "gold") == ["dim"]


def test_path_exists(spark, tmp_path):
    (tmp_path / "present").mkdir()

    assert path_exists(spark, str(tmp_path / "present"))
    assert not path_exists(spark, str(tmp_path / "absent"))


def test_list_partition_values_reads_partition_directories(spark, tmp_path):
    path = str(tmp_path / "orders")
    df = spark.createDataFrame([(1, "2026-01-02"), (2, "2026-01-01"), (3, "a b:c")], SCHEMA)
    write_partitions(df, path, ["ingestion_date"])

    assert list_partition_values(spark, path, "ingestion_date") == [
        "2026-01-01",
        "2026-01-02",
        "a b:c",
    ]
    assert list_partition_values(spark, path, "other") == []
    assert list_partition_values(spark, str(tmp_path / "absent"), "ingestion_date") == []
