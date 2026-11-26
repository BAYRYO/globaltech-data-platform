"""KAN-32 to KAN-36 on the generated ci drops of seed 42: silver then gold, day after day."""

import json

import pytest
from pyspark.sql import functions as F

from gtdp.gold.kpis import KPIS
from gtdp.gold.tables import FACT_SALES, table_path
from gtdp.io import read_table

pytestmark = pytest.mark.slow


def fact_sales(lake):
    return read_table(lake.spark, table_path(lake.cfg.paths.gold, "fact_sales"), FACT_SALES.schema)


def gold_runs(lake) -> list:
    return [row for row in lake.audit() if row.job_name == "gold"]


def test_every_silver_order_is_in_fact_sales_on_its_date(generated):
    def per_date(frame):
        return dict(frame.groupBy("order_date").count().collect())

    silver = generated.table("orders").withColumn("order_date", F.to_date("event_time"))

    assert per_date(fact_sales(generated)) == per_date(silver)
    assert fact_sales(generated).count() == silver.count()


def test_late_orders_join_the_published_orders_of_their_date(generated):
    j2 = json.loads(gold_runs(generated)[1].metrics_json)
    late = [day for day in j2["order_dates"] if day < "2026-01-01"]
    silver = generated.table("orders").where(F.to_date("event_time").isin(late))
    drops = {str(row.ingestion_date) for row in silver.select("ingestion_date").collect()}

    assert late
    assert {"2026-01-01", "2026-01-02"} <= drops
    assert fact_sales(generated).where(F.col("order_date").isin(late)).count() == silver.count()


def test_p00042_orders_take_the_catalog_price_of_their_date(generated):
    history = (
        generated.table("products").where(F.col("product_id") == "P00042").orderBy("version")
    ).collect()
    sales = fact_sales(generated).where(F.col("product_id") == "P00042").collect()

    def price_at(moment):
        return next(v.unit_price_eur for v in history if v.valid_from <= moment < v.valid_to)

    change = history[1].valid_from
    assert {row.event_time < change for row in sales} == {True, False}
    assert all(row.applicable_unit_price == price_at(row.event_time) for row in sales)
    assert history[0].unit_price_eur != history[1].unit_price_eur


def test_each_gold_run_checks_the_star_and_builds_every_indicator(generated):
    runs = gold_runs(generated)
    metrics = [json.loads(run.metrics_json) for run in runs]

    assert [run.status for run in runs] == ["SUCCESS"] * 3
    assert all(run.rows_in == run.rows_out > 0 for run in runs)
    assert all(set(m["checks"].values()) == {0} for m in metrics)
    assert all(set(m["build_violations"].values()) == {0} for m in metrics)
    assert set(metrics[-1]["kpi_rows"]) == set(KPIS)
    assert all(metrics[-1]["kpi_rows"].values())
