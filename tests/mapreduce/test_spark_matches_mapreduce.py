"""The Spark transformations give the MapReduce result, line for line, on the same files."""

from gtdp.perf.revenue_by_country import (
    classify_orders,
    counters,
    customer_countries,
    revenue_lines,
    summarize,
)


def spark_output(spark, drop) -> tuple[list[str], dict[str, int]]:
    orders = spark.read.text([str(path) for path in drop.orders])
    customers = spark.read.text([str(path) for path in drop.customers])
    summary = summarize(classify_orders(orders, customer_countries(customers))).collect()
    totals = {f"{group}.{name}": n for (group, name), n in counters(summary).items()}
    return revenue_lines(summary), totals


def test_spark_gives_the_expected_revenue_and_counters(spark, edge_drop):
    assert spark_output(spark, edge_drop) == (edge_drop.revenue, edge_drop.counters)


def test_spark_and_mapreduce_agree_on_the_generated_drops(spark, ci_drop, mapreduce_pipe):
    expected = mapreduce_pipe(ci_drop)

    assert spark_output(spark, ci_drop) == (expected.revenue, expected.counters)


def test_the_rules_hold_with_ansi_mode_on(spark, edge_drop):
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        assert spark_output(spark, edge_drop) == (edge_drop.revenue, edge_drop.counters)
    finally:
        spark.conf.unset("spark.sql.ansi.enabled")
