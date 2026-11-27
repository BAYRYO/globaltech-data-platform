"""Lot 6, Spark side of the MapReduce comparison: revenue per country from raw landing orders."""

import logging
from datetime import date

from pyspark.sql import SparkSession

from gtdp.audit import Layer
from gtdp.config import ConfigError
from gtdp.io import INGESTION_DATE, list_partition_values
from gtdp.jobs.registry import JobContext, job
from gtdp.perf.revenue_by_country import (
    classify_orders,
    counters,
    customer_countries,
    revenue_lines,
    summarize,
)

logger = logging.getLogger(__name__)

OUTPUT = "mr_vs_spark/spark"
"""Under ``paths.ops``: ``revenue/`` and ``counters/``, text files in the format of the MR job."""


@job(
    "revenue-by-country-spark",
    Layer.PERF,
    "Revenue per country and invalid-line counters of the orders landed up to --date (Lot 6)",
)
def revenue_by_country_spark(ctx: JobContext) -> None:
    if ctx.run.ingestion_date is None:
        raise ConfigError("--date is required: orders and customers landed up to it are read")
    landing, day = ctx.cfg.paths.landing, ctx.run.ingestion_date
    order_dirs = landed_partitions(ctx.spark, f"{landing}/orders", day, require_day=True)
    customer_dirs = landed_partitions(ctx.spark, f"{landing}/customers", day)
    orders = ctx.spark.read.text(order_dirs)
    countries = customer_countries(ctx.spark.read.text(customer_dirs))
    summary = summarize(classify_orders(orders, countries)).collect()

    totals, revenue = counters(summary), revenue_lines(summary)
    output = f"{ctx.cfg.paths.ops}/{OUTPUT}"
    _write_lines(ctx.spark, revenue, f"{output}/revenue")
    _write_lines(
        ctx.spark,
        sorted(f"{group}\t{name}\t{count}" for (group, name), count in totals.items()),
        f"{output}/counters",
    )
    ctx.run.counters.rows_in = sum(n for (_, name), n in totals.items() if name != "HEADER")
    ctx.run.counters.rows_rejected = sum(
        n for (group, _), n in totals.items() if group == "Quality"
    )
    ctx.run.counters.rows_out = len(revenue)
    ctx.run.metrics["counters"] = {f"{group}.{name}": n for (group, name), n in totals.items()}
    logger.info(
        "revenue by country written",
        extra={
            "yarn_app_id": ctx.spark.sparkContext.applicationId,
            "output": output,
            "order_files": orders.inputFiles(),
            "customer_dirs": customer_dirs,
        },
    )


def landed_partitions(
    spark: SparkSession, table: str, day: date, *, require_day: bool = False
) -> list[str]:
    """The ``ingestion_date`` partition directories of ``table`` up to ``day``, oldest first."""
    days = [d for d in list_partition_values(spark, table, INGESTION_DATE) if d <= day.isoformat()]
    if not days or (require_day and days[-1] != day.isoformat()):
        raise FileNotFoundError(f"nothing landed in {table} for {INGESTION_DATE}={day}")
    return [f"{table}/{INGESTION_DATE}={d}" for d in days]


def _write_lines(spark: SparkSession, lines: list[str], path: str) -> None:
    frame = spark.createDataFrame([(line,) for line in lines], "value STRING")
    frame.coalesce(1).write.mode("overwrite").text(path)
