"""The revenue-by-country-spark job: landed days up to --date in, the MR output format out."""

import shutil
from datetime import UTC, date, datetime

import pytest

from gtdp.audit import Layer, Run
from gtdp.jobs.registry import JobContext
from gtdp.jobs.revenue_by_country import revenue_by_country_spark

DAYS = ("2026-01-01", "2026-01-02", "2026-01-03")


@pytest.fixture
def landed(tmp_path, ci_landing):
    """The ci drops under the lake's landing, as land.sh publishes them."""
    for day in DAYS:
        for source in ("orders", "customers"):
            target = tmp_path / "lake" / "landing" / source / f"ingestion_date={day}"
            shutil.copytree(ci_landing / day / source, target)
    return tmp_path / "lake"


def run_job(spark, cfg, day: str) -> Run:
    run = Run("test", "revenue-by-country-spark", Layer.PERF, None, date.fromisoformat(day), now())
    revenue_by_country_spark(JobContext(spark, cfg, run))
    return run


def now() -> datetime:
    return datetime.now(UTC)


def output_lines(lake, table: str) -> list[str]:
    files = (lake / "ops" / "mr_vs_spark" / "spark" / table).glob("part-*")
    return sorted(line for path in files for line in path.read_text(encoding="utf-8").splitlines())


def test_the_job_writes_the_mapreduce_output_of_the_landed_days(
    spark, lake_config, landed, ci_drop, mapreduce_pipe
):
    run = run_job(spark, lake_config, "2026-01-03")

    expected = mapreduce_pipe(ci_drop)
    assert output_lines(landed, "revenue") == expected.revenue
    assert output_lines(landed, "counters") == sorted(
        "{}\t{}\t{}".format(*key.split("."), n) for key, n in expected.counters.items()
    )
    quality = sum(n for key, n in expected.counters.items() if key.startswith("Quality."))
    assert run.counters.rows_rejected == quality
    assert run.counters.rows_out == len(expected.revenue)


def test_the_job_reads_no_day_after_date(spark, lake_config, landed, ci_landing):
    run = run_job(spark, lake_config, "2026-01-01")

    j1 = next((ci_landing / "2026-01-01" / "orders").iterdir())
    assert run.counters.rows_in == j1.read_bytes().count(b"\n") - 1


def test_the_job_refuses_a_date_without_orders(spark, lake_config, landed):
    with pytest.raises(FileNotFoundError, match="ingestion_date=2026-01-04"):
        run_job(spark, lake_config, "2026-01-04")
