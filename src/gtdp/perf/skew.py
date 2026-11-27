"""Key salting of the gold fact build's interval join, the first KAN-45 correction.

``with_applicable_price`` joins every order line of a product to that product's versions. When one
product holds a fifth of the lines, the shuffle sends them all to one task. Salting splits each
product into ``salts`` sub-keys: an order line takes the sub-key ``xxhash64(event_time) mod
salts``, and the small side, the catalogue versions, is copied once per sub-key. Each line still
meets every version of its product, so the result is the same. The sub-key comes from
``event_time`` because the interval condition reads it anyway: a salt from another column adds
that column to the scan and to the shuffle.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SALT = "_salt"


def salted_applicable_price(orders: DataFrame, versions: DataFrame, salts: int) -> DataFrame:
    """``with_applicable_price(orders, versions)``, with the join key salted ``salts`` ways."""
    if salts < 1:
        raise ValueError(f"salts must be at least 1, got {salts}")
    salted = orders.withColumn(SALT, F.pmod(F.xxhash64("event_time"), F.lit(salts)))
    version = versions.select(
        F.col("product_id").alias("_version_of"),
        "product_sk",
        F.col("unit_price_eur").alias("applicable_unit_price"),
        "valid_from",
        "valid_to",
        F.explode(F.sequence(F.lit(0).cast("long"), F.lit(salts - 1).cast("long"))).alias(
            "_version_salt"
        ),
    )
    valid = (
        (salted["product_id"] == version["_version_of"])
        & (salted[SALT] == version["_version_salt"])
        & (version["valid_from"] <= salted["event_time"])
        & (salted["event_time"] < version["valid_to"])
    )
    return salted.join(version, valid, "left").drop(
        SALT, "_version_of", "_version_salt", "valid_from", "valid_to"
    )
