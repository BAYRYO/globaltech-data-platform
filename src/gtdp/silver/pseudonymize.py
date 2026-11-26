"""Salted SHA-256 of personal data (ADR-0004), with the salt kept out of every query plan.

A ``lit(salt)`` prints the salt in ``explain`` and in the event log that the History Server
serves. The salt therefore enters the computation as data: a one-row DataFrame, broadcast and
joined, whose plans show the column name only.

Every silver column computed from a value holding personal data is salted, the raw log line
included: an unsalted hash of a line rebuilt from the other columns would give the IP back.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from gtdp.config import DEV_SALT, Env, Secret

SALT = "_salt"
MIN_SALT_LENGTH = 16
FINGERPRINT_PREFIX = "gtdp-salt-fingerprint"
ROTATION = "docs/runbook/silver.md, « Changer le sel »"


class SaltError(RuntimeError):
    """The salt is unfit for the cluster, or differs from the one silver was built with."""


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


def check_salt(env: Env, salt: Secret) -> None:
    """Refuse, on the cluster, a blank salt, one shorter than ``MIN_SALT_LENGTH`` characters or
    the development salt committed in the repository."""
    if env is not Env.CLUSTER:
        return
    value = salt.reveal()
    if not value.strip():
        raise SaltError("GTDP_PSEUDO_SALT is blank")
    if value == DEV_SALT:
        raise SaltError("GTDP_PSEUDO_SALT is the development salt committed in the repository")
    if len(value) < MIN_SALT_LENGTH:
        raise SaltError(
            f"GTDP_PSEUDO_SALT holds {len(value)} characters, at least {MIN_SALT_LENGTH} needed"
        )


def fingerprint(salt: Secret) -> str:
    """``sha256(FINGERPRINT_PREFIX || salt)``: tells two salts apart and reveals neither."""
    return hashlib.sha256(f"{FINGERPRINT_PREFIX}{salt.reveal()}".encode()).hexdigest()


def fingerprint_path(ops: str) -> str:
    return f"{ops}/pseudonymization/salt_fingerprint.json"


def ensure_fingerprint(spark: SparkSession, ops: str, salt: Secret, run_id: str) -> None:
    """Record the fingerprint of ``salt`` on its first use; ``SaltError`` if another one is
    recorded. Silver hashes everything with one salt: hashes made with another match nothing
    already published."""
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(fingerprint_path(ops))
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    current = fingerprint(salt)
    if not fs.exists(path):
        record = {
            "fingerprint": current,
            "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "run_id": run_id,
        }
        stream = fs.create(path, False)
        try:
            stream.write(bytearray(json.dumps(record).encode()))
        finally:
            stream.close()
        return
    stream = fs.open(path)
    try:
        recorded = json.loads(bytes(stream.readAllBytes()).decode("utf-8"))
    finally:
        stream.close()
    if recorded["fingerprint"] != current:
        raise SaltError(
            f"GTDP_PSEUDO_SALT is not the salt silver was built with, whose fingerprint run "
            f"{recorded.get('run_id')} recorded in {fingerprint_path(ops)}. Export that salt; "
            f"to change it, rebuild silver whole ({ROTATION})"
        )
