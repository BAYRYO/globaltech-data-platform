"""Smoke job: proves Spark, the lake filesystem and the audit table work end to end."""

from pyspark.sql import functions as F

from gtdp.audit import Layer
from gtdp.io import replace_table
from gtdp.jobs.registry import JobContext, job

SMOKE_ROWS = 1000


@job("smoke", Layer.OPS, "Count 1000 generated rows into 10 buckets under <paths.ops>/smoke")
def smoke(ctx: JobContext) -> None:
    path = f"{ctx.cfg.paths.ops}/smoke"
    numbers = ctx.spark.range(SMOKE_ROWS, numPartitions=4)
    buckets = (
        numbers.groupBy((F.col("id") % 10).alias("bucket"))
        .count()
        .withColumn("run_id", F.lit(ctx.run.run_id))
    )
    replace_table(buckets, path)
    ctx.run.counters.rows_in = SMOKE_ROWS
    ctx.run.counters.rows_out = ctx.spark.read.parquet(path).count()
