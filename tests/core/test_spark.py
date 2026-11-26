import pytest

pytestmark = pytest.mark.slow


def test_overwrite_replaces_only_the_partitions_written(spark, tmp_path):
    path = str(tmp_path / "table")
    spark.createDataFrame([("a", 1), ("b", 1)], "d string, v int").write.partitionBy("d").parquet(
        path
    )

    spark.createDataFrame([("b", 2)], "d string, v int").write.mode("overwrite").partitionBy(
        "d"
    ).parquet(path)

    rows = sorted(spark.read.parquet(path).collect(), key=lambda r: r.d)
    assert [(r.d, r.v) for r in rows] == [("a", 1), ("b", 2)]


def test_timestamps_are_read_in_utc(spark):
    [row] = spark.sql("SELECT unix_timestamp(TIMESTAMP'2026-01-01 00:00:00') AS epoch").collect()

    assert row.epoch == 1_767_225_600
