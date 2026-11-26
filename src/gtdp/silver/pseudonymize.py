"""Salted SHA-256 of personal data (ADR-0004), with the salt kept out of every query plan.

A ``lit(salt)`` prints the salt in ``explain`` and in the event log that the History Server
serves. The salt therefore enters the computation as data: a one-row DataFrame, broadcast and
joined, whose plans show the column name only.

Every silver column computed from a value holding personal data is salted, the raw log line
included: an unsalted hash of a line rebuilt from the other columns would give the IP back.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from gtdp.config import Secret

SALT = "_salt"


def salt_frame(spark: SparkSession, salt: Secret) -> DataFrame:
    return spark.createDataFrame(
        [(salt.reveal(),)], StructType([StructField(SALT, StringType(), False)])
    )


def with_salt(frame: DataFrame, salt: DataFrame) -> DataFrame:
    """``frame`` plus the ``_salt`` column that ``salted`` reads."""
    return frame.crossJoin(F.broadcast(salt))


def salted(value: Column) -> Column:
    """``sha2(salt || value, 256)`` as hex, null where ``value`` is null."""
    return F.sha2(F.concat(F.col(SALT), value), 256)
