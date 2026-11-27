"""KAN-59: a well-formed date before 1900-01-01 gets the disposition of its registry, and the day
is published. Parquet holds such a date like any other: version 1 of a product starts on
1900-01-01, so nothing earlier is plausible, but nothing earlier stops a run either."""

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from gtdp.silver import days

from .lakes import DAYS

pytestmark = pytest.mark.slow

J1 = DAYS[0]
ANCIENT = "1899-12-31T23:59:59"
LOG = '198.18.12.34 - - [{}] "GET /search?q=lamp HTTP/1.1" 200 512 "-" "Mozilla/5.0 (desktop)"'


def line(lake, source: str, record: dict[str, str]) -> str:
    """``record`` as a line of the landed file of ``source`` for J1, in its header's order."""
    header = lake.landed_lines(J1, source)[0].split(",")
    return ",".join(record[column] for column in header)


def known_customer(lake) -> str:
    return lake.landed_lines(J1, "customers")[1].split(",")[0]


def customer(customer_id: str, signup_date: str, updated_at: str) -> dict[str, str]:
    return {
        "customer_id": customer_id,
        "full_name": "Ada Ancient",
        "email": f"{customer_id.lower()}@example.com",
        "phone": "+33600000000",
        "region": "FR-IDF",
        "segment": "SMB",
        "signup_date": signup_date,
        "updated_at": updated_at,
    }


def published(lake, run, source: str, code: str) -> None:
    """The run published J1 whole and counted one ``code`` for ``source``."""
    assert [str(d) for d in days.complete_days(lake.spark, lake.cfg.paths.silver)] == [J1]
    assert run.metrics["sources"][source]["measures"][code] == 1


@pytest.mark.parametrize(
    ("source", "build"),
    [
        (
            "orders",
            lambda lake: line(
                lake,
                "orders",
                {
                    "transaction_id": "T999999999",
                    "event_time": ANCIENT,
                    "customer_id": known_customer(lake),
                    "product_id": "P00042",
                    "quantity": "1",
                    "unit_price_eur": "10.00",
                    "channel": "web",
                    "status": "COMPLETED",
                },
            ),
        ),
        (
            "products",
            lambda lake: line(
                lake,
                "products",
                {
                    "product_id": "P99990",
                    "product_name": "Lighting P99990",
                    "category": "Lighting",
                    "unit_price_eur": "10.00",
                    "updated_at": ANCIENT,
                },
            ),
        ),
        ("weblogs", lambda lake: LOG.format("31/Dec/1899:23:59:59 +0000")),
    ],
)
def test_an_event_before_1900_is_rejected_and_the_day_published(fresh_lake, source, build):
    lake = fresh_lake
    ancient = build(lake)
    lake.append_lines(J1, source, [ancient])

    run = lake.run("silver", J1)

    published(lake, run, source, "rejected.ANCIENT_TIMESTAMP")
    rejected = lake.rejects(source).where(F.col("raw_record") == ancient).collect()
    assert [row.reject_reasons for row in rejected] == [["ANCIENT_TIMESTAMP"]]


@pytest.mark.parametrize(
    ("record", "flag", "column", "value"),
    [
        (
            customer("C999901", "1500-01-01", "2026-01-01T08:00:00"),
            "ANCIENT_SIGNUP",
            "signup_date",
            date(1500, 1, 1),
        ),
        (
            customer("C999902", "2024-02-27", ANCIENT),
            "ANCIENT_UPDATED_AT",
            "updated_at",
            datetime(1899, 12, 31, 23, 59, 59),
        ),
    ],
)
def test_a_customer_date_before_1900_is_flagged_kept_and_read_back(
    fresh_lake, record, flag, column, value
):
    lake = fresh_lake
    lake.append_lines(J1, "customers", [line(lake, "customers", record)])

    run = lake.run("silver", J1)

    published(lake, run, "customers", f"flagged.{flag}")
    (row,) = lake.table("customers").where(F.col("customer_id") == record["customer_id"]).collect()
    assert (row.quality_flags, row[column]) == ([flag], value)
