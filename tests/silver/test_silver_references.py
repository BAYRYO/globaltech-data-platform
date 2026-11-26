"""KAN-27 and KAN-29: an order is checked against the dimensions silver published for its day, so
every silver order has a silver customer and exactly one price."""

import shutil

import pytest
from pyspark.sql import functions as F

from gtdp.jobs import silver as silver_job
from gtdp.silver import days
from gtdp.silver.products import violations, with_applicable_price

from .lakes import DAYS, local

pytestmark = pytest.mark.slow

J1, J2 = DAYS[0], DAYS[1]
NEW_CUSTOMER = (
    "{id},Jean Martin,jean.martin@example.com,+33600000000,FR-IDF,SMB,2026-01-01,"
    "2026-01-01T09:00:00"
)


def order(transaction_id: str, customer_id: str, product_id: str = "P00042") -> str:
    return f"{transaction_id},2026-01-01T12:00:00,{customer_id},{product_id},1,10.00,web,COMPLETED"


def reasons(lake, transaction_id: str) -> list[list[str]]:
    rejected = lake.rejects("orders").where(F.col("raw_record").startswith(f"{transaction_id},"))
    return [row.reject_reasons for row in rejected.collect()]


def kept(lake, transaction_id: str) -> int:
    return lake.table("orders").where(F.col("transaction_id") == transaction_id).count()


def test_an_order_of_a_product_without_a_clean_version_is_rejected(fresh_lake):
    lake = fresh_lake
    customer = lake.landed_lines(J1, "customers")[1].split(",")[0]
    lake.append_lines(
        J2,
        "products",
        [
            "P09999,Lighting P09999,Lighting,abc,2026-01-01T10:00:00",
            "P09998,Lighting P09998,Lighting,10.00,2026-01-01T10:00:00",
            "P09998,Lighting P09998,Lighting,12.00,2026-01-01T10:00:00",
        ],
    )
    lake.append_lines(
        J2,
        "orders",
        [order("T900000001", customer, "P09999"), order("T900000002", customer, "P09998")],
    )

    for day in (J1, J2):
        lake.run("silver", day)

    products = lake.table("products")
    priced = with_applicable_price(lake.table("orders"), products)
    assert priced.where(F.col("product_sk").isNull()).count() == 0
    assert sum(violations(products).values()) == 0
    assert reasons(lake, "T900000001") == [["UNKNOWN_PRODUCT"]]
    assert reasons(lake, "T900000002") == [["UNKNOWN_PRODUCT"]]


def test_an_order_of_a_customer_seen_only_in_a_rejected_record_is_rejected(fresh_lake):
    lake = fresh_lake
    malformed = NEW_CUSTOMER.format(id="C900001") + ",extra"
    lake.append_lines(J2, "customers", [malformed])
    lake.append_lines(J2, "orders", [order("T900000003", "C900001")])

    for day in (J1, J2):
        lake.run("silver", day)

    orders, customers = lake.table("orders"), lake.table("customers")
    assert orders.join(customers, "customer_id", "left_anti").count() == 0
    assert reasons(lake, "T900000003") == [["UNKNOWN_CUSTOMER"]]


def test_orders_wait_for_the_dimensions_of_their_day(fresh_lake):
    lake = fresh_lake
    lake.append_lines(J2, "customers", [NEW_CUSTOMER.format(id="C900002")])
    lake.append_lines(J2, "orders", [order("T900000004", "C900002")])
    shutil.rmtree(local(lake.cfg.paths.bronze) / "customers" / f"ingestion_date={J2}")
    lake.run("silver", J1)

    with pytest.raises(silver_job.DayOrderError, match="customers"):
        lake.run("silver", J2, "orders")
    lake.run("bronze", J2, "customers")
    lake.run("silver", J2, "customers")
    with pytest.raises(silver_job.DayOrderError, match="products"):
        lake.run("silver", J2, "orders")
    lake.run("silver", J2)

    assert (kept(lake, "T900000004"), reasons(lake, "T900000004")) == (1, [])


def test_a_customer_replayed_for_a_day_makes_its_orders_due_again(fresh_lake):
    lake = fresh_lake
    lake.append_lines(J2, "orders", [order("T900000005", "C900003")])
    lake.run("silver", J1)
    lake.run("silver", J2)
    rejected_first = reasons(lake, "T900000005")

    lake.append_lines(J2, "customers", [NEW_CUSTOMER.format(id="C900003")])
    lake.run("silver", J2, "customers")
    complete_before_orders = days.complete_days(lake.spark, lake.cfg.paths.silver)
    lake.run("silver", J2, "orders")

    assert rejected_first == [["UNKNOWN_CUSTOMER"]]
    assert [str(day) for day in complete_before_orders] == [J1]
    assert (kept(lake, "T900000005"), reasons(lake, "T900000005")) == (1, [])
    assert [str(day) for day in days.complete_days(lake.spark, lake.cfg.paths.silver)] == [J1, J2]
