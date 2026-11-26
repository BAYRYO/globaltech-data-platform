"""Audit table: one row per job run under ``<paths.ops>/audit``, the only append-only table."""

from __future__ import annotations

import json
import logging
import secrets
import signal
import threading
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
from gtdp.io import RUN_ID_PROPERTY
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
    STOPPED = "STOPPED"


class RunStopped(BaseException):
    """A stop signal reached the driver. Not an ``Exception``: a job must not swallow it."""

    def __init__(self, sig: signal.Signals) -> None:
        super().__init__(sig.name)
        self.signal = sig


@dataclass(slots=True)
class Counters:
    """Row counts the job reports; ``None`` means not measured.

    Assigning anything but an ``int`` or ``None`` raises ``TypeError`` in the job that does it.
    """

    rows_in: int | None = None
    rows_out: int | None = None
    rows_rejected: int | None = None
    rows_duplicated: int | None = None

    def __setattr__(self, name: str, value: object) -> None:
        if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
            raise TypeError(f"counter {name} takes an int or None, got {value!r}")
        object.__setattr__(self, name, value)


@dataclass(frozen=True)
class Run:
    """What a job knows about its own run. ``metrics`` holds extra key/values for the audit.

    The audit stores ``metrics`` as JSON. Keys become ``str``, and so do values JSON cannot hold.
    """

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

    The Spark jobs of the run carry the tag ``gtdp-run-<run_id>``, and the thread the Spark local
    property ``RUN_ID_PROPERTY``, which table locks record. ``RunStopped`` cancels the jobs until
    the ``STOPPED`` row is written. A failure to write the audit row is logged and never
    replaces the job's own exception.
    """
    started_at = datetime.now(UTC)
    run_id = f"{started_at:%Y%m%dT%H%M%SZ}-{secrets.token_hex(4)}"
    run = Run(run_id, job, layer, source, ingestion_date, started_at)
    tag = f"gtdp-run-{run_id}"
    with bind(run_id=run_id, job=job):
        logger.info("run started", extra={"layer": layer, "source": source})
        spark.sparkContext.addJobTag(tag)
        spark.sparkContext.setLocalProperty(RUN_ID_PROPERTY, run_id)
        try:
            yield run
        except RunStopped as stop:
            _untag(spark, tag)
            with _cancelling(spark, tag):
                _append(spark, cfg, run, Status.STOPPED, f"stopped by {stop.signal.name}")
            raise
        except BaseException as exc:
            _untag(spark, tag)
            _append(spark, cfg, run, Status.FAILED, f"{type(exc).__name__}: {exc}")
            raise
        _untag(spark, tag)
        _append(spark, cfg, run, Status.SUCCESS, None)


def _untag(spark: SparkSession, tag: str) -> None:
    """Stop tagging the jobs of this thread: the audit write must escape ``_cancelling``."""
    try:
        spark.sparkContext.removeJobTag(tag)
        spark.sparkContext.setLocalProperty(RUN_ID_PROPERTY, None)
    except Exception:
        logger.exception("could not remove the job tag of the run")


@contextmanager
def _cancelling(spark: SparkSession, tag: str) -> Iterator[None]:
    """Cancel the jobs tagged ``tag`` every 0.2 s until the block ends.

    One cancel is not enough: the JVM may still be planning the interrupted action, whose job
    then starts after the cancel and holds the executors that the audit row needs.
    """
    done = threading.Event()

    def cancel() -> None:
        try:
            while True:
                spark.sparkContext.cancelJobsWithTag(tag)
                if done.wait(0.2):
                    return
        except Exception:
            logger.exception("could not cancel the Spark jobs of the run")

    canceller = threading.Thread(target=cancel, name="gtdp-cancel-run", daemon=True)
    canceller.start()
    try:
        yield
    finally:
        done.set()
        canceller.join()


def _append(
    spark: SparkSession, cfg: AppConfig, run: Run, status: Status, error: str | None
) -> None:
    ended_at = datetime.now(UTC)
    duration_s = round((ended_at - run.started_at).total_seconds(), 3)
    counters = asdict(run.counters)
    logger.info(
        "run ended",
        extra={"status": status, "duration_s": duration_s, "error": error, **counters},
    )
    row = {
        "job_name": run.job,
        "layer": run.layer.value,
        "source": run.source,
        "ingestion_date": run.ingestion_date,
        "env": cfg.env.value,
        "status": status.value,
        "started_at": run.started_at,
        "ended_at": ended_at,
        "duration_s": duration_s,
        **counters,
        "git_sha": cfg.git_sha,
        "error_message": error[:ERROR_MESSAGE_MAX] if error else None,
        "metrics_json": _metrics_json(run.metrics),
    }
    # One directory per run: concurrent jobs never share Spark's _temporary commit directory,
    # which a plain append to the same path would delete under another job.
    target = f"{audit_path(cfg)}/run_date={run.started_at.date()}/run_id={run.run_id}"
    try:
        row["yarn_app_id"] = spark.sparkContext.applicationId
        try:
            frame = spark.createDataFrame([row], _ROW_SCHEMA)
        except (TypeError, ValueError) as exc:  # a counter out of the LongType range
            row |= dict.fromkeys(counters) | {"metrics_json": _audit_error("row", exc)}
            frame = spark.createDataFrame([row], _ROW_SCHEMA)
        frame.coalesce(1).write.mode("overwrite").parquet(target)
    except Exception:
        logger.exception("could not write the audit row")


def _metrics_json(metrics: dict[str, object]) -> str:
    try:
        return json.dumps({str(k): v for k, v in metrics.items()}, default=str, sort_keys=True)
    except (TypeError, ValueError) as exc:  # a nested non-str key, a reference cycle
        return _audit_error("metrics", exc)


def _audit_error(part: str, exc: Exception) -> str:
    return json.dumps({"audit_error": f"{part} not stored: {type(exc).__name__}: {exc}"})
