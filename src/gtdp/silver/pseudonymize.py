"""Salted SHA-256 of personal data (ADR-0004), with the salt kept out of every query plan.

A ``lit(salt)`` prints the salt in ``explain`` and in the event log that the History Server
serves. The salt therefore enters the computation as data: a one-row DataFrame, broadcast and
joined, whose plans show the column name only.
"""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from gtdp.config import Secret

SALT = "_salt"


def salt_frame(spark: SparkSession, salt: Secret) -> DataFrame:
    return spark.createDataFrame(
        [(salt.reveal(),)], StructType([StructField(SALT, StringType(), False)])
    )


def with_salted_hashes(
    frame: DataFrame, salt: DataFrame, values: Mapping[str, Column]
) -> DataFrame:
    """``frame`` plus a column per entry of ``values``: ``sha2(salt || value, 256)`` as hex,
    null where the value is null."""
    salted = frame.crossJoin(F.broadcast(salt))
    hashes = [
        F.sha2(F.concat(F.col(SALT), value), 256).alias(name) for name, value in values.items()
    ]
    return salted.select(*[F.col(column) for column in frame.columns], *hashes)
