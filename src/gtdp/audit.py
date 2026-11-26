"""Audit table: one row per job run under ``<paths.ops>/audit``, the only append-only table."""

from __future__ import annotations

import json
import logging
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from gtdp.config import AppConfig
from gtdp.log import bind

logger = logging.getLogger(__name__)

ERROR_MESSAGE_MAX = 2000


class Layer(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    STREAMING = "streaming"
    OPS = "ops"
    PERF = "perf"


class Status(StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass(slots=True)
class Counters:
    """Row counts the job reports; ``None`` means not measured."""

    rows_in: int | None = None
    rows_out: int | None = None
    rows_rejected: int | None = None
    rows_duplicated: int | None = None


@dataclass(frozen=True)
class Run:
    """What a job knows about its own run. ``metrics`` holds extra key/values for the audit."""

    run_id: str
    job: str
    layer: Layer
    source: str | None
    ingestion_date: date | None
    started_at: datetime
    counters: Counters = field(default_factory=Counters)
    metrics: dict[str, object] = field(default_factory=dict)


# run_date and run_id are not in the files: they are the partition directories.
_ROW_SCHEMA = StructType(
    [
        StructField("job_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("source", StringType(), True),
        StructField("ingestion_date", DateType(), True),
        StructField("env", StringType(), False),
        StructField("status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("ended_at", TimestampType(), False),
        StructField("duration_s", DoubleType(), False),
        StructField("rows_in", LongType(), True),
        StructField("rows_out", LongType(), True),
        StructField("rows_rejected", LongType(), True),
        StructField("rows_duplicated", LongType(), True),
        StructField("yarn_app_id", StringType(), True),
        StructField("git_sha", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("metrics_json", StringType(), False),
    ]
)


def audit_path(cfg: AppConfig) -> str:
    return f"{cfg.paths.ops}/audit"


@contextmanager
def audited_run(
    spark: SparkSession,
    cfg: AppConfig,
    job: str,
    layer: Layer,
    source: str | None = None,
    ingestion_date: date | None = None,
) -> Iterator[Run]:
    """Yield the ``Run`` of ``job``; on exit append its audit row, then re-raise any error.

    A failure to write the audit row is logged and never replaces the job's own exception.
    """
    started_at = datetime.now(UTC)
    run_id = f"{started_at:%Y%m%dT%H%M%SZ}-{secrets.token_hex(4)}"
    run = Run(run_id, job, layer, source, ingestion_date, started_at)
    with bind(run_id=run_id, job=job):
        logger.info("run started", extra={"layer": layer, "source": source})
        try:
            yield run
        except BaseException as exc:
            _append(spark, cfg, run, Status.FAILED, f"{type(exc).__name__}: {exc}")
            raise
        _append(spark, cfg, run, Status.SUCCESS, None)


def _append(
    spark: SparkSession, cfg: AppConfig, run: Run, status: Status, error: str | None
) -> None:
    ended_at = datetime.now(UTC)
    duration_s = round((ended_at - run.started_at).total_seconds(), 3)
    counters = run.counters
    logger.info(
        "run ended",
        extra={"status": status, "duration_s": duration_s, "error": error, **asdict(counters)},
    )
    try:
        row = (
            run.job,
            run.layer.value,
            run.source,
            run.ingestion_date,
            cfg.env.value,
            status.value,
            run.started_at,
            ended_at,
            duration_s,
            counters.rows_in,
            counters.rows_out,
            counters.rows_rejected,
            counters.rows_duplicated,
            spark.sparkContext.applicationId,
            cfg.git_sha,
            error[:ERROR_MESSAGE_MAX] if error else None,
            json.dumps(run.metrics, default=str, sort_keys=True),
        )
        # One directory per run: concurrent jobs never share Spark's _temporary commit directory,
        # which a plain append to the same path would delete under another job.
        target = f"{audit_path(cfg)}/run_date={run.started_at.date()}/run_id={run.run_id}"
        spark.createDataFrame([row], _ROW_SCHEMA).coalesce(1).write.mode("overwrite").parquet(
            target
        )
    except Exception:
        logger.exception("could not write the audit row")
