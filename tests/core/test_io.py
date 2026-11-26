import logging
import os
import shutil
from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from gtdp.config import load_config
from gtdp.io import (
    INGESTION_DATE,
    LaterPartitionError,
    ensure_no_later_partition,
    latest_partition_after,
    list_partition_values,
    path_exists,
    read_table,
    replace_table,
    write_partitions,
)

pytestmark = pytest.mark.slow

REPO = Path(__file__).resolve().parents[2]

SCHEMA = "id int, ingestion_date string"
TABLE = StructType([StructField("id", IntegerType()), StructField(INGESTION_DATE, DateType())])
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
    assert os.listdir(tmp_path) == ["orders"]


def test_a_cached_read_of_the_table_sees_the_replaced_partition(spark, tmp_path):
    path = str(tmp_path / "orders")
    first = spark.createDataFrame([(1, "2026-01-01"), (2, "2026-01-02")], SCHEMA)
    write_partitions(first, path, {INGESTION_DATE: [JAN_1, JAN_2]})
    spark.read.parquet(path).cache().count()

    write_partitions(
        spark.createDataFrame([(3, "2026-01-02")], SCHEMA), path, {INGESTION_DATE: [JAN_2]}
    )

    assert rows(spark, path) == [(1, JAN_1), (3, JAN_2)]


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


VERSIONS = "id int, version string"
VERSIONS_TABLE = StructType(
    [StructField("id", IntegerType()), StructField("version", StringType())]
)


@pytest.fixture
def interrupted_swap(spark, tmp_path):
    """products_scd2 after a crash between the two renames of replace_table.

    Run 1 published v1. Run 2 wrote its complete output, moved the v1 table aside, and died.
    """
    gold = tmp_path / "gold"
    path = str(gold / "products_scd2")
    replace_table(spark.createDataFrame([(1, "v1"), (2, "v1")], VERSIONS), path)
    run_2 = spark.createDataFrame([(1, "v1"), (2, "v1"), (3, "v2")], VERSIONS)
    run_2.write.parquet(str(gold / "_products_scd2.tmp"))
    os.rename(gold / "products_scd2", gold / "_products_scd2.old")
    return path


def test_the_run_after_an_interrupted_swap_keeps_the_history(spark, interrupted_swap, tmp_path):
    previous = read_table(spark, interrupted_swap, VERSIONS_TABLE)
    run_3 = previous.union(spark.createDataFrame([(4, "v3")], VERSIONS))

    replace_table(run_3, interrupted_swap)

    assert rows(spark, interrupted_swap) == [(1, "v1"), (2, "v1"), (3, "v2"), (4, "v3")]
    assert os.listdir(tmp_path / "gold") == ["products_scd2"]


def test_path_exists_finishes_an_interrupted_swap(spark, interrupted_swap):
    assert path_exists(spark, interrupted_swap)
    assert rows(spark, interrupted_swap) == [(1, "v1"), (2, "v1"), (3, "v2")]


def test_an_incomplete_staged_table_gives_the_old_table_back(spark, tmp_path):
    gold = tmp_path / "gold"
    path = str(gold / "dim")
    replace_table(spark.createDataFrame([(1, "v1")], VERSIONS), path)
    os.rename(gold / "dim", gold / "_dim.old")
    (gold / "_dim.tmp").mkdir()

    assert sorted(tuple(r) for r in read_table(spark, path).collect()) == [(1, "v1")]


def test_a_cached_read_of_the_table_sees_the_replaced_content(spark, tmp_path):
    path = str(tmp_path / "gold" / "dim")
    replace_table(spark.createDataFrame([(1, "v1")], VERSIONS), path)
    spark.read.parquet(path).cache().count()

    replace_table(spark.createDataFrame([(2, "v2")], VERSIONS), path)

    assert rows(spark, path) == [(2, "v2")]


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


def test_a_table_whose_only_day_had_no_row_reads_as_empty(spark, tmp_path):
    path = str(tmp_path / "rejects")
    write_partitions(spark.createDataFrame([], SCHEMA), path, {INGESTION_DATE: [JAN_1]})
    assert read_table(spark, path, TABLE).collect() == []

    one = spark.createDataFrame([(1, "2026-01-01")], SCHEMA)
    write_partitions(one, path, {INGESTION_DATE: [JAN_1]})
    write_partitions(spark.createDataFrame([], SCHEMA), path, {INGESTION_DATE: [JAN_1]})
    assert read_table(spark, path, TABLE).collect() == []


def test_a_missing_table_reads_as_empty_with_its_schema(spark, tmp_path):
    empty = read_table(spark, str(tmp_path / "silver_orders"), TABLE)

    assert empty.collect() == []
    assert empty.schema == TABLE


def test_reading_an_empty_table_without_its_schema_is_an_error(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        read_table(spark, str(tmp_path / "absent"))


def test_read_table_merges_the_file_schemas_when_asked(spark, tmp_path):
    path = str(tmp_path / "bronze")
    day_1 = spark.createDataFrame([(1, "2026-01-01")], SCHEMA)
    day_2 = spark.createDataFrame(
        [(2, 0.1, "2026-01-02")], "id int, discount_pct double, ingestion_date string"
    )
    write_partitions(day_1, path, {INGESTION_DATE: [JAN_1]})
    write_partitions(day_2, path, {INGESTION_DATE: [JAN_2]})

    merged = read_table(spark, path, merge_schema=True)

    assert sorted(merged.columns) == ["discount_pct", "id", INGESTION_DATE]


BRONZE = "transaction_id string, amount int, ingestion_date string"
SILVER = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("amount", IntegerType()),
        StructField(INGESTION_DATE, DateType()),
    ]
)


def make_bronze(spark, path, data):
    df = spark.createDataFrame(data, BRONZE)
    write_partitions(df, path, {INGESTION_DATE: sorted({row[2] for row in data})})


def silver_day(spark, lake, day, force=False):
    """ADR-0005 reduced to its shape: validate day D, drop keys known before D, rewrite D."""
    bronze, silver, rejects = (str(lake / name) for name in ("bronze", "silver", "rejects"))
    ensure_no_later_partition(spark, silver, INGESTION_DATE, day, force=force)
    deposit = read_table(spark, bronze, SILVER).where(F.col(INGESTION_DATE) == F.lit(day))
    known = read_table(spark, silver, SILVER).where(F.col(INGESTION_DATE) < F.lit(day))
    valid = (
        deposit.where("amount >= 0")
        .dropDuplicates(["transaction_id"])
        .join(known.select("transaction_id"), "transaction_id", "left_anti")
    )
    write_partitions(valid, silver, {INGESTION_DATE: [day]})
    write_partitions(deposit.where("amount < 0"), rejects, {INGESTION_DATE: [day]})


def ids(spark, path):
    return sorted(row.transaction_id for row in read_table(spark, path, SILVER).collect())


def test_a_first_day_without_any_valid_row_does_not_break_the_next_day(spark, tmp_path):
    make_bronze(
        spark, str(tmp_path / "bronze"), [("t1", -1, "2026-01-01"), ("t2", 3, "2026-01-02")]
    )

    silver_day(spark, tmp_path, JAN_1)
    silver_day(spark, tmp_path, JAN_2)

    assert ids(spark, str(tmp_path / "silver")) == ["t2"]
    assert ids(spark, str(tmp_path / "rejects")) == ["t1"]


def test_rejects_stay_readable_after_a_day_without_reject(spark, tmp_path):
    make_bronze(spark, str(tmp_path / "bronze"), [("t1", 1, "2026-01-01")])

    silver_day(spark, tmp_path, JAN_1)

    assert ids(spark, str(tmp_path / "rejects")) == []
    assert ids(spark, str(tmp_path / "silver")) == ["t1"]


@pytest.fixture
def published(spark, tmp_path):
    """Silver with days 1 and 2 published, day 2 holding a duplicated row."""
    path = str(tmp_path / "silver")
    df = spark.createDataFrame([(1, "2026-01-01"), (2, "2026-01-02"), (2, "2026-01-02")], SCHEMA)
    write_partitions(df, path, {INGESTION_DATE: [JAN_1, JAN_2]})
    return path


PUBLISHED = [(1, JAN_1), (2, JAN_2), (2, JAN_2)]


def test_a_failed_replay_keeps_the_published_partition(spark, published, tmp_path):
    failing = spark.range(4).selectExpr(
        "CAST(IF(id = 3, raise_error('executor lost'), id) AS INT) AS id",
        "'2026-01-02' AS ingestion_date",
    )

    with pytest.raises(Exception, match="executor lost"):
        write_partitions(failing, published, {INGESTION_DATE: [JAN_2]})

    assert rows(spark, published) == PUBLISHED
    assert os.listdir(tmp_path) == ["silver"]


def test_a_dataframe_without_the_partition_column_keeps_the_published_partition(spark, published):
    no_partition_column = spark.createDataFrame([(5,)], "id int")

    with pytest.raises(AnalysisException):
        write_partitions(no_partition_column, published, {INGESTION_DATE: [JAN_2]})

    assert rows(spark, published) == PUBLISHED


def test_a_partition_can_be_rewritten_from_its_own_rows(spark, published):
    day_2 = spark.read.parquet(published).where(F.col(INGESTION_DATE) == F.lit(JAN_2))

    write_partitions(day_2.dropDuplicates(), published, {INGESTION_DATE: [JAN_2]})

    assert rows(spark, published) == [(1, JAN_1), (2, JAN_2)]


def test_a_row_outside_the_named_partitions_is_refused_and_nothing_is_published(
    spark, published, tmp_path
):
    stray = spark.createDataFrame([(9, "2026-01-01"), (3, "2026-01-03")], SCHEMA)

    with pytest.raises(ValueError, match="ingestion_date=2026-01-01"):
        write_partitions(stray, published, {INGESTION_DATE: [JAN_3]})

    assert rows(spark, published) == PUBLISHED
    assert list_partition_values(spark, published, INGESTION_DATE) == ["2026-01-01", "2026-01-02"]
    assert os.listdir(tmp_path) == ["silver"]


@pytest.mark.parametrize(
    "values", ["2026-01-02", [datetime(2026, 1, 2)]], ids=["bare-string", "datetime"]
)
def test_partition_values_that_name_other_directories_are_refused(spark, published, values):
    with pytest.raises(TypeError):
        write_partitions(spark.createDataFrame([], SCHEMA), published, {INGESTION_DATE: values})

    assert rows(spark, published) == PUBLISHED
    assert list_partition_values(spark, published, INGESTION_DATE) == ["2026-01-01", "2026-01-02"]


def test_a_later_day_without_new_row_still_blocks_an_earlier_replay(spark, tmp_path):
    bronze = [("a", 1, "2026-01-01"), ("b", 1, "2026-01-02"), ("b", 1, "2026-01-03")]
    make_bronze(spark, str(tmp_path / "bronze"), bronze)
    for day in (JAN_1, JAN_2, JAN_3):  # day 3 only re-sends b, so silver day 3 gets no row
        silver_day(spark, tmp_path, day)

    with pytest.raises(LaterPartitionError, match="ingestion_date=2026-01-03"):
        silver_day(spark, tmp_path, JAN_2)


def test_a_lake_under_a_space_and_an_accent_is_written_where_configured(spark, tmp_path):
    lake = tmp_path / "Hélène Dupont" / "lake"
    cfg = load_config("local", overrides={"paths.bronze": str(lake / "bronze")})

    df = spark.createDataFrame([(1, "2026-01-02")], SCHEMA)
    write_partitions(df, cfg.paths.bronze, {INGESTION_DATE: [JAN_2]})

    assert (lake / "bronze" / "ingestion_date=2026-01-02").is_dir()
    assert [tuple(r) for r in read_table(spark, cfg.paths.bronze).collect()] == [(1, JAN_2)]


def test_a_checkout_under_a_space_reads_its_landing_files(spark, tmp_path):
    checkout = tmp_path / "projet bigdata"
    shutil.copytree(REPO / "conf", checkout / "conf")
    landing = checkout / "data" / "lake" / "landing"
    landing.mkdir(parents=True)
    (landing / "orders.csv").write_text("id\n1\n")

    cfg = load_config("local", checkout / "conf")

    assert spark.read.option("header", True).csv(cfg.paths.landing).count() == 1
