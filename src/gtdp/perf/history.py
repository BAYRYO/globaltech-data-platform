"""Readings of the Spark History Server REST answers that the Lot 7 benchmarks report.

A benchmark run tags its Spark jobs with a job group; its stages are the stages of those jobs.
The figures are the History Server's own: stage totals from ``/stages`` and task quantiles from
``/stages/<id>/<attempt>/taskSummary?quantiles=0.5,1.0``, the "Summary Metrics" of the Spark UI.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

STAGE_KEYS = (
    "numCompleteTasks",
    "executorRunTime",
    "inputBytes",
    "inputRecords",
    "shuffleReadBytes",
    "shuffleWriteBytes",
    "memoryBytesSpilled",
    "diskBytesSpilled",
)
QUANTILES = (0.5, 1.0)


@dataclass(frozen=True)
class Skew:
    """Median and max of the tasks of one stage: duration (ms) and bytes read (input or shuffle)."""

    tasks: int
    duration_median_ms: float
    duration_max_ms: float
    read_median_bytes: float
    read_max_bytes: float

    @property
    def duration_ratio(self) -> float:
        return ratio(self.duration_max_ms, self.duration_median_ms)

    @property
    def read_ratio(self) -> float:
        return ratio(self.read_max_bytes, self.read_median_bytes)


def ratio(maximum: float, median: float) -> float:
    """``maximum / median``; infinite when the median is 0 and the maximum is not."""
    if median == 0:
        return math.inf if maximum else 1.0
    return maximum / median


def group_stage_ids(jobs: Iterable[Mapping], group: str) -> set[int]:
    """The stage ids of the jobs of ``group`` in a ``/jobs`` answer."""
    return {sid for job in jobs if job.get("jobGroup") == group for sid in job["stageIds"]}


def completed_stages(stages: Iterable[Mapping], ids: set[int]) -> list[Mapping]:
    """The completed attempts of the stages ``ids`` in a ``/stages`` answer; skipped stages, whose
    shuffle output an earlier stage already wrote, did no work."""
    return [s for s in stages if s["stageId"] in ids and s["status"] == "COMPLETE"]


def totals(stages: Iterable[Mapping]) -> dict[str, int]:
    """Sums of ``STAGE_KEYS`` over ``stages``."""
    sums = dict.fromkeys(STAGE_KEYS, 0)
    for stage in stages:
        for key in STAGE_KEYS:
            sums[key] += stage.get(key, 0)
    return sums


def key_stage(stages: Iterable[Mapping], metric: str) -> Mapping:
    """The stage of ``stages`` with the largest ``metric``: ``shuffleReadBytes`` finds the stage
    that joins or aggregates after a shuffle, ``inputBytes`` the stage that scans the table."""
    return max(stages, key=lambda stage: stage.get(metric, 0))


def skew(stage: Mapping, summary: Mapping, metric: str) -> Skew:
    """The ``Skew`` of ``stage`` from its ``taskSummary?quantiles=0.5,1.0`` answer.

    The bytes read are the shuffle bytes read when ``metric`` is ``shuffleReadBytes``, the input
    bytes otherwise. Quantiles are those of the successful tasks, as in the Spark UI.
    """
    if tuple(summary["quantiles"]) != QUANTILES:
        raise ValueError(f"expected quantiles {QUANTILES}, got {summary['quantiles']}")
    duration = summary["duration"]
    read = (
        summary["shuffleReadMetrics"]["readBytes"]
        if metric == "shuffleReadBytes"
        else summary["inputMetrics"]["bytesRead"]
    )
    return Skew(stage["numCompleteTasks"], duration[0], duration[1], read[0], read[1])
