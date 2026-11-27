"""The benchmarks' readings of the Spark History Server REST answers."""

import math

import pytest

from gtdp.perf.history import (
    completed_stages,
    group_stage_ids,
    key_stage,
    ratio,
    skew,
    totals,
)

JOBS = [
    {"jobId": 0, "jobGroup": "skew/baseline/1", "stageIds": [0, 1, 2]},
    {"jobId": 1, "jobGroup": "skew/baseline/1", "stageIds": [3]},
    {"jobId": 2, "jobGroup": "skew/salted-16/1", "stageIds": [4, 5]},
    {"jobId": 3, "stageIds": [6]},
]
STAGES = [
    {"stageId": 0, "attemptId": 0, "status": "COMPLETE", "inputBytes": 900, "shuffleReadBytes": 0},
    {"stageId": 1, "attemptId": 0, "status": "COMPLETE", "inputBytes": 10, "shuffleReadBytes": 0},
    {"stageId": 2, "attemptId": 0, "status": "SKIPPED", "inputBytes": 0, "shuffleReadBytes": 0},
    {
        "stageId": 3,
        "attemptId": 0,
        "status": "COMPLETE",
        "inputBytes": 0,
        "shuffleReadBytes": 500,
        "diskBytesSpilled": 64,
        "numCompleteTasks": 16,
    },
    {"stageId": 4, "attemptId": 0, "status": "COMPLETE", "inputBytes": 900},
]
SUMMARY = {
    "quantiles": [0.5, 1.0],
    "duration": [200.0, 3000.0],
    "executorRunTime": [180.0, 2900.0],
    "inputMetrics": {"bytesRead": [0.0, 0.0], "recordsRead": [0.0, 0.0]},
    "shuffleReadMetrics": {"readBytes": [10.0, 80.0], "readRecords": [1.0, 8.0]},
}


def test_a_group_has_the_stages_of_its_jobs_only():
    assert group_stage_ids(JOBS, "skew/baseline/1") == {0, 1, 2, 3}
    assert group_stage_ids(JOBS, "unknown") == set()


def test_skipped_stages_and_other_groups_are_left_out_of_the_totals():
    own = completed_stages(STAGES, group_stage_ids(JOBS, "skew/baseline/1"))
    assert [s["stageId"] for s in own] == [0, 1, 3]
    summed = totals(own)
    assert summed["inputBytes"] == 910
    assert summed["shuffleReadBytes"] == 500
    assert summed["diskBytesSpilled"] == 64
    assert summed["numCompleteTasks"] == 16


def test_the_key_stage_is_the_largest_by_the_chosen_metric():
    own = completed_stages(STAGES, {0, 1, 3})
    assert key_stage(own, "shuffleReadBytes")["stageId"] == 3
    assert key_stage(own, "inputBytes")["stageId"] == 0


def test_skew_is_max_over_median_of_duration_and_of_the_bytes_read():
    found = skew(STAGES[3], SUMMARY, "shuffleReadBytes")
    assert found.tasks == 16
    assert found.duration_ratio == 15.0
    assert found.read_ratio == 8.0
    assert skew(STAGES[3], SUMMARY, "inputBytes").read_ratio == 1.0


def test_a_zero_median_gives_an_infinite_ratio_unless_the_max_is_zero_too():
    assert ratio(5, 0) == math.inf
    assert ratio(0, 0) == 1.0


def test_other_quantiles_are_refused():
    with pytest.raises(ValueError, match="quantiles"):
        skew(STAGES[3], SUMMARY | {"quantiles": [0.25, 0.5, 0.75]}, "inputBytes")
