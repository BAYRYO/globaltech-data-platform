"""KAN-31: the J3 schema change (``discount_pct``) processed together with J1 and J2, and an
unknown extra column reported in the audit (ADR-0007)."""

import pytest
from pyspark.sql import functions as F

from .lakes import DAYS, local

pytestmark = pytest.mark.slow

J1, J2, J3 = DAYS


def test_orders_have_a_discount_of_zero_before_j3_and_the_files_value_from_j3(published):
    by_day = {
        str(row.ingestion_date): row
        for row in published.table("orders")
        .groupBy("ingestion_date")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.count("discount_pct").alias("with_discount"),
            F.sum((F.col("discount_pct") > 0).cast("int")).alias("discounted"),
        )
        .collect()
    }

    assert all(by_day[day].with_discount == by_day[day].rows for day in DAYS)
    assert (by_day[J1].discounted, by_day[J2].discounted) == (0, 0)
    assert by_day[J3].discounted > 0


def test_revenue_is_never_null_and_equals_quantity_times_price_before_j3(published):
    orders = published.table("orders").withColumn(
        "revenue", F.col("quantity") * F.col("unit_price_eur") * (1 - F.col("discount_pct"))
    )

    assert orders.where(F.col("revenue").isNull()).count() == 0
    assert (
        orders.where(F.col("ingestion_date") < J3)
        .where(F.col("revenue") != F.col("quantity") * F.col("unit_price_eur"))
        .count()
        == 0
    )


def test_an_unknown_column_is_reported_and_ignored(fresh_lake):
    lake = fresh_lake
    landed = (
        local(lake.cfg.paths.landing) / "orders" / f"ingestion_date={J3}" / "orders_20260103.csv"
    )
    header, *lines = landed.read_text(encoding="utf-8").splitlines()
    landed.write_text("\n".join([f"{header},coupon_code", *[f"{line},X" for line in lines]]) + "\n")
    lake.run("bronze", J3, "orders")

    runs = [lake.run("silver", day, "orders") for day in DAYS]

    assert [run.metrics["unknown_columns"] for run in runs] == [
        {"orders": []},
        {"orders": []},
        {"orders": ["coupon_code"]},
    ]
    assert "coupon_code" not in lake.table("orders").columns
    assert runs[2].counters.rows_in == len(lines)
