"""The dimensions of the star schema, rebuilt whole from silver by each run."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gtdp.gold.tables import DIM_CUSTOMER, DIM_DATE, DIM_PRODUCT, date_key
from gtdp.silver.quality import conform


def customers(silver: DataFrame) -> DataFrame:
    """``silver/customers`` (SCD type 1, already pseudonymised): one row per customer."""
    return conform(silver, DIM_CUSTOMER.schema)


def products(versions: DataFrame) -> DataFrame:
    """``silver/products_scd2``: one row per catalog version, keyed by ``product_sk``."""
    return conform(versions, DIM_PRODUCT.schema)


def calendar(days: DataFrame) -> DataFrame:
    """Every day from the first to the last ``date`` of ``days``, gaps included; ``days`` may
    repeat dates. ``day_of_week`` is ISO: Monday 1 to Sunday 7."""
    bounds = days.agg(F.min("date").alias("first"), F.max("date").alias("last"))
    every_day = bounds.select(
        F.explode(F.sequence("first", "last", F.expr("INTERVAL 1 DAY"))).alias("date")
    )
    day = F.col("date")
    return conform(
        every_day.select(
            date_key(day).alias("date_key"),
            day,
            F.year(day).alias("year"),
            F.quarter(day).alias("quarter"),
            F.month(day).alias("month"),
            F.weekofyear(day).alias("iso_week"),
            (F.weekday(day) + 1).alias("day_of_week"),
            (F.weekday(day) >= 5).alias("is_weekend"),
        ),
        DIM_DATE.schema,
    )
