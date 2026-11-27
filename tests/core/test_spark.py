import pytest

pytestmark = pytest.mark.slow


def test_timestamps_are_read_in_utc(spark):
    [row] = spark.sql("SELECT unix_timestamp(TIMESTAMP'2026-01-01 00:00:00') AS epoch").collect()

    assert row.epoch == 1_767_225_600


def test_the_read_rebase_mode_does_not_change_dates_spark_3_wrote(spark, tmp_path):
    """ADR-0002 sets the write rebase modes only: Spark 3 names its version in the files it
    writes and reads them back unconverted under every read mode (review B, minor 2)."""
    path = str(tmp_path / "ancient")
    spark.sql(
        "SELECT DATE'1500-01-01' AS d, TIMESTAMP'1800-06-01 12:00:00' AS t, DATE'1582-10-10' AS g"
    ).write.parquet(path)
    keys = [f"spark.sql.parquet.{mode}RebaseModeInRead" for mode in ("datetime", "int96")]
    before = {key: spark.conf.get(key, None) for key in keys}
    read = {}
    try:
        for mode in ("CORRECTED", "EXCEPTION", "LEGACY"):
            for key in keys:
                spark.conf.set(key, mode)
            read[mode] = spark.read.parquet(path).selectExpr("string(d)", "string(t)", "string(g)")
            read[mode] = [tuple(row) for row in read[mode].collect()]
    finally:
        for key, value in before.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)

    assert read["EXCEPTION"] == read["LEGACY"] == read["CORRECTED"]
    assert read["CORRECTED"] == [("1500-01-01", "1800-06-01 12:00:00", "1582-10-10")]
