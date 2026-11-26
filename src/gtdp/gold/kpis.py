"""The gold indicators: each one a pure function of the star, rebuilt whole by each run.

Revenue counts ``COMPLETED`` lines only, at the catalog price applicable on the order date
(``revenue_eur``). Ratios are doubles, null when their denominator is 0.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from gtdp.config import GoldSettings
from gtdp.gold.dimensions import calendar
from gtdp.gold.facts import order_date
from gtdp.gold.tables import ORDER_DATE
from gtdp.io import INGESTION_DATE
from gtdp.schemas import REJECT_REASONS

COMPLETED, CANCELLED, RETURNED = "COMPLETED", "CANCELLED", "RETURNED"
WINDOW_DAYS = 7
FUNNEL = (("view", "views"), ("cart", "carts"), ("purchase", "purchases"))
"""Event type of each step of the conversion funnel, and the column counting it."""
ORDER_BEFORE_SIGNUP = "ORDER_BEFORE_SIGNUP"
PRICE_RATIO_ACCURACY = 10_000


@dataclass(frozen=True)
class Star:
    """What the indicators read: the published star, and the silver tables of data quality."""

    sales: DataFrame
    web_events: DataFrame
    customers: DataFrame
    products: DataFrame
    orders: DataFrame
    """``silver/orders``, every ``ingestion_date``."""
    rejects: DataFrame
    """``silver/rejects`` of every source."""


def _completed(sales: DataFrame) -> DataFrame:
    return sales.where(F.col("status") == COMPLETED)


def _count_of(condition: Column) -> Column:
    return F.sum(condition.cast("int"))


def _ratio(numerator: str, denominator: str) -> Column:
    return (F.col(numerator) / F.col(denominator)).cast("double")


def revenue_daily(sales: DataFrame) -> DataFrame:
    """Completed orders, quantity and revenue per order date and country."""
    return (
        _completed(sales)
        .groupBy(ORDER_DATE, "country")
        .agg(
            F.count(F.lit(1)).alias("orders"),
            F.sum("quantity").alias("quantity"),
            F.sum("revenue_eur").alias("revenue_eur"),
        )
    )


def top_products_by_country(sales: DataFrame, products: DataFrame, n: int) -> DataFrame:
    """The products of ranks 1 to ``n`` by revenue in each country, over the whole history.

    ``rank`` is a ``dense_rank`` over the country's products by revenue: products with the same
    revenue share a rank and are all kept, so a country lists ``n`` products or more, and the
    ranks have no gap. The result depends on the revenues only, never on the order in which Spark
    reads the rows. Name and category are those of the product's current version.
    """
    totals = (
        _completed(sales)
        .groupBy("country", "product_id")
        .agg(
            F.sum("revenue_eur").alias("revenue_eur"),
            F.sum("quantity").alias("quantity"),
            F.count(F.lit(1)).alias("orders"),
        )
    )
    by_revenue = Window.partitionBy("country").orderBy(F.col("revenue_eur").desc())
    ranked = totals.withColumn("rank", F.dense_rank().over(by_revenue)).where(F.col("rank") <= n)
    current = products.where("is_current").select("product_id", "product_name", "category")
    columns = ("country", "rank", "product_id", "product_name", "category", "revenue_eur")
    return ranked.join(current, "product_id", "left").select(*columns, "quantity", "orders")


def revenue_moving_avg(sales: DataFrame) -> DataFrame:
    """Daily revenue per country and its average over the last 7 days, the day included.

    The calendar is complete: every day from the first to the last order date, for every country
    with orders. A day without completed order counts as 0, so the 7 rows of a window always span
    7 calendar days. The first 6 days of the calendar have fewer rows before them: their average
    is over ``window_days`` days, 1 to 6.
    """
    daily = revenue_daily(sales).select(ORDER_DATE, "country", "revenue_eur")
    days = calendar(sales.select(F.col(ORDER_DATE).alias("date"))).select(
        F.col("date").alias(ORDER_DATE)
    )
    filled = (
        days.crossJoin(sales.select("country").distinct())
        .join(daily, [ORDER_DATE, "country"], "left")
        .select(ORDER_DATE, "country", F.coalesce("revenue_eur", F.lit(0)).alias("revenue_eur"))
    )
    window = Window.partitionBy("country").orderBy(ORDER_DATE).rowsBetween(1 - WINDOW_DAYS, 0)
    return filled.select(
        "*",
        F.round(F.avg("revenue_eur").over(window), 2).alias("revenue_ma7_eur"),
        F.count(F.lit(1)).over(window).alias("window_days"),
    )


def price_deviation(sales: DataFrame, products: DataFrame) -> DataFrame:
    """Median ratio of the invoiced unit price to the applicable catalog price, per month and
    category of the version applied, over every line whatever its status."""
    ratio = (F.col("unit_price_invoiced") / F.col("applicable_unit_price")).cast("double")
    return (
        sales.join(products.select("product_sk", "category"), "product_sk")
        .groupBy(F.trunc(ORDER_DATE, "month").alias("month"), "category")
        .agg(
            F.count(F.lit(1)).alias("lines"),
            F.percentile_approx(ratio, 0.5, PRICE_RATIO_ACCURACY).alias("median_price_ratio"),
        )
    )


def cancel_return_rate(sales: DataFrame, products: DataFrame) -> DataFrame:
    """Share of cancelled and of returned lines per channel and category of the version applied."""
    status = F.col("status")
    totals = (
        sales.join(products.select("product_sk", "category"), "product_sk")
        .groupBy("channel", "category")
        .agg(
            F.count(F.lit(1)).alias("lines"),
            _count_of(status == CANCELLED).alias("cancelled"),
            _count_of(status == RETURNED).alias("returned"),
        )
    )
    return totals.select(
        "*",
        _ratio("cancelled", "lines").alias("cancel_rate"),
        _ratio("returned", "lines").alias("return_rate"),
    )


def avg_basket(sales: DataFrame, customers: DataFrame) -> DataFrame:
    """Average completed order per country and customer segment.

    A basket is one transaction: the contract has no basket identifier grouping several lines.
    ``avg_basket_eur`` is at the applicable catalog price, ``avg_invoiced_eur`` at the invoiced
    price.
    """
    segments = customers.select("customer_id", "segment")
    return (
        _completed(sales)
        .join(segments, "customer_id", "left")
        .groupBy("country", "segment")
        .agg(
            F.count(F.lit(1)).alias("orders"),
            F.round(F.avg("revenue_eur"), 2).alias("avg_basket_eur"),
            F.round(F.avg("invoiced_amount_eur"), 2).alias("avg_invoiced_eur"),
            F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        )
    )


def http_errors_hourly(web_events: DataFrame) -> DataFrame:
    """Requests, 404 and 500 responses, and their shares, per hour of the logs."""
    status = F.col("http_status")
    totals = web_events.groupBy(F.date_trunc("hour", "event_ts").alias("event_hour")).agg(
        F.count(F.lit(1)).alias("requests"),
        _count_of(status == 404).alias("http_404"),
        _count_of(status == 500).alias("http_500"),
    )
    return totals.select(
        "*",
        _ratio("http_404", "requests").alias("rate_404"),
        _ratio("http_500", "requests").alias("rate_500"),
    )


def conversion_funnel(web_events: DataFrame) -> DataFrame:
    """Product views, cart additions and purchases per device, and the rate of each step.

    Only served requests count (``http_status`` below 400): a 404 product page or a failed
    checkout is no step of the funnel.
    """
    served = web_events.where(F.col("http_status") < 400)
    steps = served.groupBy("device").agg(
        *[_count_of(F.col("event_type") == event).alias(name) for event, name in FUNNEL]
    )
    return steps.select(
        "*",
        _ratio("carts", "views").alias("view_to_cart"),
        _ratio("purchases", "carts").alias("cart_to_purchase"),
        _ratio("purchases", "views").alias("view_to_purchase"),
    )


def data_quality(rejects: DataFrame, orders: DataFrame, customers: DataFrame) -> DataFrame:
    """Records per drop (``ingestion_date``), source and code: the silver rejects per reason, and
    the orders kept although dated before their customer's signup.

    A record rejected for two reasons counts once under each.
    """
    rejected = rejects.select(
        INGESTION_DATE,
        "source",
        F.explode(REJECT_REASONS).alias("code"),
        F.lit("rejected").alias("disposition"),
    )
    early = (
        orders.join(customers.select("customer_id", "signup_date"), "customer_id")
        .where(order_date() < F.col("signup_date"))
        .select(
            INGESTION_DATE,
            F.lit("orders").alias("source"),
            F.lit(ORDER_BEFORE_SIGNUP).alias("code"),
            F.lit("kept").alias("disposition"),
        )
    )
    return (
        rejected.unionByName(early)
        .groupBy(INGESTION_DATE, "source", "code", "disposition")
        .agg(F.count(F.lit(1)).alias("rows"))
    )


Kpi = Callable[[Star, GoldSettings], DataFrame]

KPIS: Mapping[str, Kpi] = MappingProxyType(
    {
        "kpi_revenue_daily": lambda star, _: revenue_daily(star.sales),
        "kpi_top_products_country": lambda star, settings: top_products_by_country(
            star.sales, star.products, settings.top_n
        ),
        "kpi_revenue_moving_avg": lambda star, _: revenue_moving_avg(star.sales),
        "kpi_price_deviation": lambda star, _: price_deviation(star.sales, star.products),
        "kpi_cancel_return_rate": lambda star, _: cancel_return_rate(star.sales, star.products),
        "kpi_avg_basket": lambda star, _: avg_basket(star.sales, star.customers),
        "kpi_http_errors_hourly": lambda star, _: http_errors_hourly(star.web_events),
        "kpi_conversion_funnel": lambda star, _: conversion_funnel(star.web_events),
        "kpi_data_quality": lambda star, _: data_quality(star.rejects, star.orders, star.customers),
    }
)
"""Every indicator table, by name, and how to build it."""
