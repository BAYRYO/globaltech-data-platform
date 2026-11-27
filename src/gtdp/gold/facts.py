"""The facts of the star schema: order lines at the price applicable on their date, web events."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from gtdp.gold.tables import AMOUNT_EUR, FACT_SALES, FACT_WEB_EVENTS, ORDER_DATE, date_key
from gtdp.silver.products import with_applicable_price
from gtdp.silver.quality import conform


def order_date(event_time: str = "event_time") -> Column:
    """The UTC day of an order, the partition of ``fact_sales``."""
    return F.to_date(F.col(event_time))


def _line_amount(unit_price: str) -> Column:
    net = F.col("quantity") * F.col(unit_price) * (F.lit(1) - F.col("discount_pct"))
    return F.round(net, 2).cast(AMOUNT_EUR)


def sales(orders: DataFrame, products: DataFrame) -> DataFrame:
    """One row per order line of ``orders`` (``silver/orders`` rows), priced from ``products``
    (``dim_product``).

    ``applicable_unit_price`` is the catalog price of the version valid at ``event_time``
    (``valid_from <= event_time < valid_to``): ``revenue_eur`` is computed at that price,
    ``invoiced_amount_eur`` at the price invoiced on the line, both after ``discount_pct``.
    The join keeps every order: one without a version has a null ``product_sk``, one matching
    several versions repeats. ``build_violations`` counts these rows. The customer's attributes
    are current values (SCD 1): they stay in ``dim_customer``, joined by the indicators.
    """
    return conform(
        with_applicable_price(orders, products).select(
            "*",
            F.col("unit_price_eur").alias("unit_price_invoiced"),
            _line_amount("applicable_unit_price").alias("revenue_eur"),
            _line_amount("unit_price_eur").alias("invoiced_amount_eur"),
            order_date().alias(ORDER_DATE),
            date_key(order_date()).alias("date_key"),
        ),
        FACT_SALES.schema,
    )


def build_violations(sales_rows: DataFrame, orders: int) -> dict[str, int]:
    """Rows of ``sales(...)`` built from ``orders`` order lines that break the grain, per kind:
    no catalog version valid at the order's time, the rows beyond the first of an order valid in
    several versions. A customer missing from ``dim_customer`` is an orphan of ``fact_sales``,
    which ``checks.violations`` counts."""
    row = sales_rows.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("product_sk").isNull().cast("int")).alias("no_version"),
    ).first()
    rows = row["rows"] if row else 0
    return {
        "no_version": int(row["no_version"] or 0) if row else 0,
        "several_versions": rows - orders,
    }


def web_events(weblogs: DataFrame) -> DataFrame:
    """One row per request of ``silver/weblogs``; silver already derived the event type, the
    product and the device from the method, the path and the user agent."""
    return conform(
        weblogs.select("*", date_key(F.to_date("event_ts")).alias("date_key")),
        FACT_WEB_EVENTS.schema,
    )
