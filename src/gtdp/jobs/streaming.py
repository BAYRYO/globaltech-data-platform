"""Sensor stream job: gold aggregates and alerts from the sensor files, until asked to stop."""

import logging

from gtdp.audit import Layer
from gtdp.config import ConfigError
from gtdp.io import path_exists
from gtdp.jobs.registry import JobContext, job
from gtdp.streaming.stream import (
    AGGREGATES,
    ALERTS,
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
    settings = ctx.cfg.streaming
    if settings is None:
        raise ConfigError("the configuration has no [streaming] section")
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
    queries = start_queries(spark, paths, settings, totals, trigger)
    try:
        reason = wait_for_stop(
            queries, totals, settings, lambda: path_exists(spark, paths.stop_marker)
        )
    finally:
        stop_queries(queries)
    collect_progress(queries, totals)
    if reason == "stop marker":
        delete_path(spark, paths.stop_marker)

    metrics = totals.snapshot()
    counters = ctx.run.counters
    counters.rows_in = metrics[AGGREGATES]["input_rows"]
    counters.rows_out = metrics[AGGREGATES]["rows_written"] + metrics[ALERTS]["rows_written"]
    counters.rows_rejected = metrics[REJECTS]["rows_written"]
    counters.rows_duplicated = metrics[AGGREGATES]["rows_deduplicated"]
    ctx.run.metrics.update(stop_reason=reason, queries=metrics)
    logger.info("stream stopped", extra={"stop_reason": reason, "queries": metrics})
