"""Products: the rules of a catalog version, and ``silver/products_scd2`` (SCD type 2).

The table is rebuilt from every clean product record of bronze up to the day. A record starts a
new version only when a historised attribute changes. Version 1 is valid from 1900-01-01, so
every order, however early, finds exactly one price.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from gtdp.io import INGESTION_DATE
from gtdp.schemas import RECORD_HASH, SILVER_PRODUCTS_SCD2, SOURCES
from gtdp.silver.parse import amount, blank, missing_or_not_positive, timestamp
from gtdp.silver.quality import Action, Entity, Rule, conform, raw, records_sharing

HISTORISED = ("product_name", "category", "unit_price_eur")
FIRST_VALID_FROM = "1900-01-01 00:00:00"
OPEN_VALID_TO = "9999-12-31 00:00:00"
REJECT = Action.REJECT


def _at(text: str) -> Column:
    """A timestamp of the session time zone (UTC), whatever the driver's time zone."""
    return F.lit(text).cast("timestamp")


def _columns() -> dict[str, Column]:
    return {
        "product_id": raw("product_id"),
        "product_name": raw("product_name"),
        "category": raw("category"),
        "unit_price_eur": amount(raw("unit_price_eur")),
        "updated_at": timestamp(raw("updated_at")),
    }


ENTITY = Entity(
    SOURCES["products"],
    _columns,
    (
        Rule("MISSING_KEY", REJECT, lambda: blank(raw("product_id"))),
        Rule("BAD_PRICE", REJECT, lambda: missing_or_not_positive(F.col("unit_price_eur"))),
        Rule("NULL_CATEGORY", REJECT, lambda: blank(raw("category"))),
        Rule("BAD_TIMESTAMP", REJECT, lambda: F.col("updated_at").isNull()),
        # Two different records for one version: no way to tell which is right, both go.
        Rule(
            "CONFLICTING_VERSION",
            REJECT,
            lambda: records_sharing(raw("product_id"), raw("updated_at")) > 1,
        ),
    ),
)


def table(clean: DataFrame, salt: DataFrame) -> DataFrame:
    """The versions of every product in ``clean``; products hold no personal data."""
    restated = Window.partitionBy("product_id", "updated_at").orderBy(
        F.col(INGESTION_DATE).desc(), F.col(RECORD_HASH)
    )
    by_time = Window.partitionBy("product_id").orderBy("updated_at")
    attributes = F.struct(*HISTORISED)
    changes = (
        clean.withColumn("_rank", F.row_number().over(restated))
        .where(F.col("_rank") == 1)
        .withColumn("_previous", F.lag(attributes).over(by_time))
        .where(~F.col("_previous").eqNullSafe(attributes))
    )
    version = F.row_number().over(by_time)
    next_start = F.lead("updated_at").over(by_time)
    versions = changes.select(
        "*",
        version.alias("version"),
        F.when(version == 1, _at(FIRST_VALID_FROM))
        .otherwise(F.col("updated_at"))
        .alias("valid_from"),
        F.coalesce(next_start, _at(OPEN_VALID_TO)).alias("valid_to"),
        next_start.isNull().alias("is_current"),
    )
    keyed = versions.withColumn(
        "product_sk",
        F.concat_ws("-", "product_id", F.date_format("valid_from", "yyyyMMddHHmmss")),
    )
    return conform(keyed, SILVER_PRODUCTS_SCD2)


def violations(versions: DataFrame) -> dict[str, int]:
    """Versions of ``silver/products_scd2`` that break an SCD2 invariant, per invariant."""
    by_start = Window.partitionBy("product_id").orderBy("valid_from")
    next_start = F.coalesce(F.lead("valid_from").over(by_start), _at(OPEN_VALID_TO))
    first = F.row_number().over(by_start) == 1
    broken = {
        "same_valid_from": F.count(F.lit(1)).over(Window.partitionBy("product_id", "valid_from"))
        > 1,
        "empty_interval": F.col("valid_from") >= F.col("valid_to"),
        "overlap_or_gap": F.col("valid_to") != next_start,
        "wrong_is_current": F.col("is_current") != (F.col("valid_to") == _at(OPEN_VALID_TO)),
        "late_first_version": first & (F.col("valid_from") != _at(FIRST_VALID_FROM)),
    }
    flagged = versions.select(*[condition.alias(name) for name, condition in broken.items()])
    row = flagged.agg(*[F.sum(F.col(name).cast("int")).alias(name) for name in broken]).first()
    return {name: int(row[name] or 0) if row else 0 for name in broken}


def with_applicable_price(orders: DataFrame, versions: DataFrame) -> DataFrame:
    """``orders`` with ``product_sk`` and ``applicable_unit_price``, the catalog price of the
    version valid at ``event_time``: ``valid_from <= event_time < valid_to``."""
    version = versions.select(
        F.col("product_id").alias("_version_of"),
        "product_sk",
        F.col("unit_price_eur").alias("applicable_unit_price"),
        "valid_from",
        "valid_to",
    )
    valid = (
        (orders["product_id"] == version["_version_of"])
        & (version["valid_from"] <= orders["event_time"])
        & (orders["event_time"] < version["valid_to"])
    )
    return orders.join(version, valid, "left").drop("_version_of", "valid_from", "valid_to")
