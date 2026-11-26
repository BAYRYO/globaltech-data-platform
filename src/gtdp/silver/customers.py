"""Customers: rules that flag rather than reject, and ``silver/customers`` (SCD type 1).

A damaged email, region or date does not make a customer and its revenue disappear: the record
stays, flagged. Only a record without ``customer_id``, or malformed, is rejected.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from gtdp.io import INGESTION_DATE
from gtdp.schemas import QUALITY_FLAGS, RECORD_HASH, SILVER_CUSTOMERS, SOURCES
from gtdp.silver.parse import blank, day_first_date, iso_date, timestamp
from gtdp.silver.pseudonymize import with_salted_hashes
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
        "email": _email(),
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
    ),
    # A key duplicate restates a record with its email in another case or padded with spaces.
    key=("customer_id", "updated_at"),
)


def table(clean: DataFrame, salt: DataFrame) -> DataFrame:
    """The latest clean record of each ``customer_id``, its email hashed, its name and phone
    dropped. A flagged email has no hash."""
    latest = Window.partitionBy("customer_id").orderBy(
        F.col("updated_at").desc(), F.col(INGESTION_DATE).desc(), F.col(RECORD_HASH)
    )
    current = clean.withColumn("_rank", F.row_number().over(latest)).where(F.col("_rank") == 1)
    usable = ~F.array_contains(QUALITY_FLAGS, "MISSING_EMAIL") & ~F.array_contains(
        QUALITY_FLAGS, "BAD_EMAIL"
    )
    hashed = with_salted_hashes(current, salt, {"email_hash": F.when(usable, F.col("email"))})
    return conform(hashed, SILVER_CUSTOMERS)
