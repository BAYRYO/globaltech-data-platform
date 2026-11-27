"""Customers: rules that flag rather than reject, and ``silver/customers`` (SCD type 1).

A damaged email, region or date does not make a customer and its revenue disappear: the record
stays, flagged. Only a record without ``customer_id``, or malformed, is rejected.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from gtdp.io import INGESTION_DATE
from gtdp.schemas import RECORD_HASH, SILVER_CUSTOMERS, SOURCES
from gtdp.silver.parse import ancient, blank, day_first_date, iso_date, timestamp
from gtdp.silver.pseudonymize import salted
from gtdp.silver.quality import Action, Entity, Rule, conform, raw

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"
UNKNOWN_COUNTRY = "UNKNOWN"
REJECT, FLAG = Action.REJECT, Action.FLAG


def _email() -> Column:
    return F.lower(F.trim(raw("email")))


def _columns() -> dict[str, Column]:
    region = raw("region")
    signup = raw("signup_date")
    return {
        "customer_id": raw("customer_id"),
        # A missing or malformed email has no hash: its flag says why.
        "email_hash": F.when(_email().rlike(EMAIL_PATTERN), salted(_email())),
        "region": F.when(~blank(region), region),
        "country": F.when(blank(region), F.lit(UNKNOWN_COUNTRY)).otherwise(
            F.substring_index(region, "-", 1)
        ),
        "segment": raw("segment"),
        "signup_date": F.coalesce(iso_date(signup), day_first_date(signup)),
        "updated_at": timestamp(raw("updated_at")),
    }


ENTITY = Entity(
    SOURCES["customers"],
    _columns,
    (
        Rule("MISSING_KEY", REJECT, lambda: blank(raw("customer_id"))),
        Rule("MISSING_EMAIL", FLAG, lambda: blank(raw("email"))),
        Rule("BAD_EMAIL", FLAG, lambda: ~blank(raw("email")) & ~_email().rlike(EMAIL_PATTERN)),
        Rule("MISSING_REGION", FLAG, lambda: blank(raw("region"))),
        Rule("BAD_DATE", FLAG, lambda: iso_date(raw("signup_date")).isNull()),
        Rule("FUTURE_SIGNUP", FLAG, lambda: F.col("signup_date") > F.col(INGESTION_DATE)),
        Rule("ANCIENT_SIGNUP", FLAG, lambda: ancient(F.col("signup_date"))),
        # Kept: the record still says the customer exists. It ranks last in the SCD1 choice.
        Rule("BAD_UPDATED_AT", FLAG, lambda: F.col("updated_at").isNull()),
        Rule("ANCIENT_UPDATED_AT", FLAG, lambda: ancient(F.col("updated_at"))),
    ),
    # A key duplicate restates a record with its email in another case or padded with spaces.
    key=("customer_id", "updated_at"),
)


def table(clean: DataFrame) -> DataFrame:
    """The latest clean record of each ``customer_id``: its email hashed, its name and phone
    dropped. A record without a readable ``updated_at`` wins only when its customer has no other;
    then the latest day, then the lowest ``_record_hash``, decides."""
    latest = Window.partitionBy("customer_id").orderBy(
        F.col("updated_at").desc_nulls_last(), F.col(INGESTION_DATE).desc(), F.col(RECORD_HASH)
    )
    current = clean.withColumn("_rank", F.row_number().over(latest)).where(F.col("_rank") == 1)
    return conform(current, SILVER_CUSTOMERS)
