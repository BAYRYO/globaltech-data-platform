import logging
import os
from datetime import date

import pytest

from gtdp.io import (
    INGESTION_DATE,
    LaterPartitionError,
    ensure_no_later_partition,
    latest_partition_after,
    list_partition_values,
    path_exists,
    replace_table,
    write_partitions,
)

pytestmark = pytest.mark.slow

SCHEMA = "id int, ingestion_date string"
JAN_1, JAN_2, JAN_3 = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)


def rows(spark, path):
    return sorted(tuple(r) for r in spark.read.parquet(path).collect())


def test_write_partitions_twice_gives_the_same_table_and_keeps_other_days(spark, tmp_path):
    path = str(tmp_path / "orders")
    first = spark.createDataFrame([(1, "2026-01-01"), (2, "2026-01-02")], SCHEMA)
    replay = spark.createDataFrame([(3, "2026-01-02"), (4, "2026-01-02")], SCHEMA)
    write_partitions(first, path, {INGESTION_DATE: [JAN_1, JAN_2]})

    write_partitions(replay, path, {INGESTION_DATE: [JAN_2]})
    once = rows(spark, path)
    write_partitions(replay, path, {INGESTION_DATE: [JAN_2]})

    assert rows(spark, path) == once
    assert once == [(1, JAN_1), (3, JAN_2), (4, JAN_2)]


def test_named_partition_without_rows_is_emptied(spark, tmp_path):
    path = str(tmp_path / "rejects")
    write_partitions(
        spark.createDataFrame([(1, "2026-01-01"), (2, "2026-01-02")], SCHEMA),
        path,
        {INGESTION_DATE: [JAN_1, JAN_2]},
    )

    write_partitions(spark.createDataFrame([], SCHEMA), path, {INGESTION_DATE: [JAN_2]})

    assert rows(spark, path) == [(1, JAN_1)]


def test_only_the_named_combination_of_columns_is_replaced(spark, tmp_path):
    path = str(tmp_path / "rejects")
    schema = "id int, source string, ingestion_date string"
    write_partitions(
        spark.createDataFrame([(1, "orders", "2026-01-02"), (2, "web:logs", "2026-01-02")], schema),
        path,
        {"source": ["orders", "web:logs"], INGESTION_DATE: [JAN_2]},
    )

    write_partitions(
        spark.createDataFrame([], schema), path, {"source": ["web:logs"], INGESTION_DATE: [JAN_2]}
    )

    assert rows(spark, path) == [(1, "orders", JAN_2)]


def test_write_partitions_refuses_to_overwrite_a_whole_table(spark, tmp_path):
    df = spark.createDataFrame([(1, "2026-01-01")], SCHEMA)

    with pytest.raises(ValueError, match="partition column"):
        write_partitions(df, str(tmp_path / "orders"), {})


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
    write_partitions(df, path, {INGESTION_DATE: ["2026-01-02", "2026-01-01", "a b:c"]})

    assert list_partition_values(spark, path, INGESTION_DATE) == [
        "2026-01-01",
        "2026-01-02",
        "a b:c",
    ]
    assert list_partition_values(spark, path, "other") == []
    assert list_partition_values(spark, str(tmp_path / "absent"), INGESTION_DATE) == []


@pytest.fixture
def days_1_and_3(spark, tmp_path):
    path = str(tmp_path / "silver_orders")
    df = spark.createDataFrame([(1, "2026-01-01"), (3, "2026-01-03")], SCHEMA)
    write_partitions(df, path, {INGESTION_DATE: [JAN_1, JAN_3]})
    return path


def test_latest_partition_after(spark, days_1_and_3, tmp_path):
    assert latest_partition_after(spark, days_1_and_3, INGESTION_DATE, JAN_2) == "2026-01-03"
    assert latest_partition_after(spark, days_1_and_3, INGESTION_DATE, JAN_3) is None
    assert latest_partition_after(spark, str(tmp_path / "absent"), INGESTION_DATE, JAN_1) is None


def test_producing_a_day_before_an_existing_later_day_is_refused(spark, days_1_and_3):
    with pytest.raises(LaterPartitionError, match="ingestion_date=2026-01-03"):
        ensure_no_later_partition(spark, days_1_and_3, INGESTION_DATE, JAN_2)


def test_replaying_the_latest_day_or_forcing_is_allowed(spark, days_1_and_3, caplog):
    ensure_no_later_partition(spark, days_1_and_3, INGESTION_DATE, JAN_3)

    with caplog.at_level(logging.WARNING):
        ensure_no_later_partition(spark, days_1_and_3, INGESTION_DATE, JAN_2, force=True)

    assert "2026-01-03" in caplog.text
