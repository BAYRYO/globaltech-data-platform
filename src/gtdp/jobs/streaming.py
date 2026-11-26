"""Sensor stream jobs: the stream itself, until asked to stop, and its periodic housekeeping."""

import logging
from dataclasses import asdict
from datetime import UTC, datetime

from pyspark.sql.streaming import StreamingQuery

from gtdp.audit import Layer, Run, RunStopped
from gtdp.config import ConfigError, StreamingSettings
from gtdp.io import STALE_AFTER_CONF, path_exists
from gtdp.jobs.registry import JobContext, job
from gtdp.streaming.housekeeping import (
    archive_inputs,
    compact_table,
    purge_archive,
    read_checkpoint,
)
from gtdp.streaming.stream import (
    AGGREGATES,
    ALERTS,
    OUTPUTS,
    REJECTS,
    SensorPaths,
    Totals,
    checkpoint_state,
    collect_progress,
    delete_path,
    make_dirs,
    start_queries,
    stop_queries,
    wait_for_stop,
)

logger = logging.getLogger(__name__)


@job(
    "streaming-sensors",
    Layer.STREAMING,
    "Stream <paths.landing>/sensors/incoming into gold sensor aggregates and alerts until stopped",
)
def streaming_sensors(ctx: JobContext) -> None:
    settings = _settings(ctx)
    spark, paths = ctx.spark, SensorPaths.of(ctx.cfg.paths, ctx.run.job)
    make_dirs(spark, paths.incoming)
    if delete_path(spark, paths.stop_marker):
        logger.warning(
            "removed a stop marker left by an earlier stop request: %s", paths.stop_marker
        )
    for name in paths.tables:
        state = checkpoint_state(spark, paths.checkpoint(name))
        logger.info("query starts", extra={"query": name, **state})

    totals = Totals()
    trigger = {"processingTime": f"{settings.trigger_interval_s} seconds"}
    queries: dict[str, StreamingQuery] = {}
    reason = "query failed"
    try:
        queries = start_queries(spark, paths, settings, totals, trigger)
        reason = wait_for_stop(
            queries, totals, settings, lambda: path_exists(spark, paths.stop_marker)
        )
    except RunStopped as stop:
        reason = f"signal {stop.signal.name}"
        raise
    finally:
        stop_queries(queries)
        collect_progress(queries, totals)
        _report(ctx.run, totals.snapshot(), reason)
    if reason == "stop marker":
        delete_path(spark, paths.stop_marker)


def _report(run: Run, metrics: dict[str, dict[str, int]], reason: str) -> None:
    """Fill the counters and metrics of the audit row, whatever ended the stream."""
    counters = run.counters
    counters.rows_in = metrics[AGGREGATES]["input_rows"]
    counters.rows_out = metrics[AGGREGATES]["rows_written"] + metrics[ALERTS]["rows_written"]
    counters.rows_rejected = metrics[REJECTS]["rows_written"]
    counters.rows_duplicated = metrics[AGGREGATES]["rows_deduplicated"]
    run.metrics.update(stop_reason=reason, queries=metrics)
    logger.info("stream stopped", extra={"stop_reason": reason, "queries": metrics})


@job(
    "streaming-housekeeping",
    Layer.STREAMING,
    "Archive the committed sensor files, purge the archive, compact old stream partitions",
)
def streaming_housekeeping(ctx: JobContext) -> None:
    settings = _settings(ctx)
    spark, paths = ctx.spark, SensorPaths.of(ctx.cfg.paths, "streaming-sensors")
    spark.conf.set(STALE_AFTER_CONF, str(settings.stale_after_s))
    archived = archive_inputs(spark, paths)
    today = datetime.now(UTC).date()
    purged = purge_archive(spark, paths, settings.archive_retention_days, today)
    compacted = {}
    for name, output in OUTPUTS.items():
        state = read_checkpoint(spark, paths.checkpoint(name))
        if state is not None:
            compacted[name] = compact_table(
                spark, paths.tables[name], output, state, settings.stale_after_s
            )
    rows = sum(compaction.rows for compaction in compacted.values())
    ctx.run.counters.rows_in = rows
    ctx.run.counters.rows_out = rows
    ctx.run.metrics.update(
        archived_files=len(archived),
        purged_days=purged,
        compacted={name: asdict(compaction) for name, compaction in compacted.items()},
    )
    logger.info("housekeeping done", extra=dict(ctx.run.metrics))


def _settings(ctx: JobContext) -> StreamingSettings:
    if ctx.cfg.streaming is None:
        raise ConfigError("the configuration has no [streaming] section")
    return ctx.cfg.streaming
