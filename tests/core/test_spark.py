import pytest

pytestmark = pytest.mark.slow


def test_timestamps_are_read_in_utc(spark):
    [row] = spark.sql("SELECT unix_timestamp(TIMESTAMP'2026-01-01 00:00:00') AS epoch").collect()

    assert row.epoch == 1_767_225_600
