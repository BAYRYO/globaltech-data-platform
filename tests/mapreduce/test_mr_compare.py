"""The comparison tool's readings of the REST answers, its checks and its code count."""

from dataclasses import astuple
from decimal import Decimal

import pytest

from gtdp.perf.mr_compare import (
    Cluster,
    Measure,
    Result,
    Task,
    TaskCost,
    checks,
    code_lines,
    differences,
    jobs_window_s,
    main_stage,
    manifest_quality,
    mann_whitney,
    mr_core_seconds,
    mr_counters,
    mr_task,
    overlapping,
    parse_revenue,
    result_counters,
    spark_core_seconds,
    spark_tasks,
    stage_totals,
    task_cost,
    volume_report,
)

RESULT = Result("FR\t10.00\t2\n", {"Lines.COMPLETED": 2, "Quality.BAD_PRICE": 1})


def measure(
    variant: str, result: Result = RESULT, inputs=("/orders/d=1",), lines_read=3, elapsed_s=1.0
):
    return Measure(
        volume="dev",
        variant=variant,
        run=1,
        app_id="app",
        elapsed_s=elapsed_s,
        jobs_s=1.0,
        cpu_s=1.0,
        memory_mb_s=1,
        vcore_s=1,
        core_s=1.0,
        lines_read=lines_read,
        bytes_read=1,
        shuffle_bytes=1,
        task_count=1,
        tasks=[Task(1_000_000, 2.0, 3.0), Task(3_000_000, 4.0, 7.0)],
        inputs=list(inputs),
        result=result,
    )


def test_both_output_formats_parse_to_decimal_revenue_and_line_counts():
    assert parse_revenue("DE\t0.12\t3\nFR\t1767230531.51\t389235\n") == {
        "DE": (Decimal("0.12"), 3),
        "FR": (Decimal("1767230531.51"), 389235),
    }


def test_the_job_counters_keep_only_the_quality_and_lines_groups():
    payload = {
        "jobCounters": {
            "counterGroup": [
                {"counterGroupName": "Quality", "counter": [_counter("BAD_PRICE", 5)]},
                {"counterGroupName": "Lines", "counter": [_counter("HEADER", 1)]},
                {"counterGroupName": "Shuffle Errors", "counter": [_counter("BAD_ID", 0)]},
            ]
        }
    }

    assert result_counters(mr_counters(payload)) == {"Quality.BAD_PRICE": 5, "Lines.HEADER": 1}


def test_stage_totals_add_up_the_completed_stages_only():
    stages = [
        {"status": "COMPLETE", "inputRecords": 10, "inputBytes": 100, "executorCpuTime": 7},
        {"status": "COMPLETE", "inputRecords": 5, "shuffleWriteBytes": 3},
        {"status": "SKIPPED", "inputRecords": 99},
    ]

    totals = stage_totals(stages)

    assert (totals["inputRecords"], totals["inputBytes"], totals["shuffleWriteBytes"]) == (
        15,
        100,
        3,
    )
    assert totals["executorCpuTime"] == 7


def test_the_spark_jobs_window_runs_from_first_submission_to_last_completion():
    jobs = [
        {
            "submissionTime": "2026-11-26T18:29:45.100GMT",
            "completionTime": "2026-11-26T18:29:49.000GMT",
        },
        {
            "submissionTime": "2026-11-26T18:29:44.600GMT",
            "completionTime": "2026-11-26T18:29:45.000GMT",
        },
    ]

    assert jobs_window_s(jobs) == 4.4


def test_the_manifests_predict_the_quality_counters_of_their_orders():
    manifests = [
        {"files": [_orders({"BAD_PRICE": 2, "DUPLICATE_EXACT": 9, "UNKNOWN_PRODUCT": 1})]},
        {
            "files": [
                _orders({"BAD_PRICE": 1}),
                {"source": "customers", "anomalies": {"BAD_DATE": 4}},
            ]
        },
    ]

    assert manifest_quality(manifests) == {"Quality.BAD_PRICE": 3}


def test_an_anomaly_the_manifest_counts_zero_times_is_not_expected():
    manifests = [{"files": [_orders({"BAD_PRICE": 1, "BAD_ENUM": 0})]}]

    assert checks([measure("mapreduce"), measure("spark")], manifest_quality(manifests)) == []


def test_a_one_cent_gap_and_a_missing_counter_are_both_reported():
    other = Result("FR\t10.01\t2\n", {"Lines.COMPLETED": 2})

    assert differences(RESULT, other) == [
        r"output 'FR\t10.01\t2\n' != 'FR\t10.00\t2\n'",
        "revenue FR: (Decimal('10.00'), 2) != (Decimal('10.01'), 2)",
        "counter Quality.BAD_PRICE: 1 != None",
    ]


MR_OUTPUT = "DE\t0.12\t3\nFR\t97.40\t6\n"
OUTPUTS = {
    "the same bytes": MR_OUTPUT,
    "one cent": "DE\t0.13\t3\nFR\t97.40\t6\n",
    "one line": "DE\t0.12\t4\nFR\t97.40\t6\n",
    "a missing country": "FR\t97.40\t6\n",
    "an extra country": MR_OUTPUT + "IT\t1.00\t1\n",
    "a trailing zero dropped": "DE\t0.12\t3\nFR\t97.4\t6\n",
    "an exponent and a sign": "DE\t1.2E-1\t3\nFR\t+97.40\t6\n",
    "reordered lines": "FR\t97.40\t6\nDE\t0.12\t3\n",
    "padded counts": "DE\t0.12\t 3 \nFR\t97.40\t06\n",
    "a duplicate country, wrong first": "DE\t0.12\t3\nFR\t1.00\t1\nFR\t97.40\t6\n",
    "a duplicate line": MR_OUTPUT + "FR\t97.40\t6\n",
    "digits below the cent": "DE\t0.120\t3\nFR\t97.400000\t6\n",
    "no final newline": MR_OUTPUT.removesuffix("\n"),
}


@pytest.mark.parametrize("name", OUTPUTS)
def test_checks_fail_unless_both_outputs_have_the_same_bytes(name):
    spark = Result(OUTPUTS[name], RESULT.counters)
    measures = [measure("mapreduce", Result(MR_OUTPUT, RESULT.counters)), measure("spark", spark)]

    assert bool(checks(measures, None)) == (OUTPUTS[name] != MR_OUTPUT)


def test_a_counter_moved_to_another_reason_fails_the_checks():
    moved = Result(RESULT.output, {"Lines.COMPLETED": 1, "Quality.BAD_PRICE": 2})

    assert checks([measure("mapreduce"), measure("spark", moved)], None) == [
        "spark #1: counter Lines.COMPLETED: 2 != 1",
        "spark #1: counter Quality.BAD_PRICE: 1 != 2",
    ]


def test_checks_pass_when_both_engines_read_and_find_the_same():
    assert checks([measure("mapreduce"), measure("spark", lines_read=9)], None) == []


def test_checks_flag_other_inputs_uncounted_lines_and_manifest_gaps():
    failures = checks(
        [measure("mapreduce", lines_read=4), measure("spark", inputs=["/orders/d=2"])],
        {"Quality.BAD_PRICE": 2},
    )

    assert failures == [
        "mapreduce #1: the counters do not add up to the lines read",
        "spark #1 read ['/orders/d=2'], not ['/orders/d=1']",
        "spark #1: the counters do not add up to the lines read",
        "Quality counters {'Quality.BAD_PRICE': 1} != manifests {'Quality.BAD_PRICE': 2}",
    ]


def test_an_application_overlaps_a_run_unless_it_ended_before_or_started_after():
    apps = [
        {"id": "mine", "startedTime": 100, "finishedTime": 200},
        {"id": "before", "startedTime": 10, "finishedTime": 100},
        {"id": "after", "startedTime": 200, "finishedTime": 300},
        {"id": "during", "startedTime": 150, "finishedTime": 160},
        {"id": "still-running", "startedTime": 50, "finishedTime": 0},
    ]

    assert overlapping(apps, "mine", 100, 200) == ["during", "still-running"]


def test_code_lines_leave_out_blank_lines_comments_and_docstrings(tmp_path):
    source = tmp_path / "job.py"
    source.write_text(
        '"""Module\ndocstring."""\n\n# why\ndef f():\n    """Doc."""\n    return 1  # note\n',
        encoding="utf-8",
    )
    script = tmp_path / "run.sh"
    script.write_text("#!/usr/bin/env bash\n# usage\n\nset -e\necho ok\n", encoding="utf-8")

    assert code_lines(source) == (7, 2)
    assert code_lines(script) == (5, 2)


FULL_RUNS = {"mapreduce": [18.205, 20.419, 19.556], "spark": [18.602, 16.524, 16.459]}
DEV_RUNS = {"mapreduce": [13.625, 14.338, 12.697], "spark": [14.855, 16.352, 12.729]}
"""Elapsed seconds of the six full and six dev runs of 2026-11-26."""


@pytest.mark.parametrize(
    ("runs", "expected"),
    [(FULL_RUNS, (8, 0.10, 0.20)), (DEV_RUNS, (2, 0.90, 0.40))],
    ids=["full", "dev"],
)
def test_mann_whitney_gives_the_exact_p_values_of_the_three_run_samples(runs, expected):
    u, one_sided, two_sided = mann_whitney(runs["mapreduce"], runs["spark"])

    assert (u, round(one_sided, 3), round(two_sided, 3)) == expected


def test_three_runs_against_three_cannot_go_below_one_chance_in_twenty():
    assert mann_whitney([4, 5, 6], [1, 2, 3]) == (9, 0.05, 0.1)


def test_mann_whitney_counts_a_tie_for_half_a_pair():
    u, one_sided, two_sided = mann_whitney([1, 2], [2, 3])

    assert (u, one_sided, two_sided) == pytest.approx((0.5, 1.0, 2 / 3))


def test_the_task_cost_separates_the_fixed_part_from_the_cost_per_million_records():
    tasks = [Task(0, 1.0, 2.0), Task(2_000_000, 5.0, 8.0), Task(4_000_000, 9.0, 14.0)]

    assert astuple(task_cost(tasks)) == pytest.approx(astuple(TaskCost(1.0, 2.0, 2.0, 3.0)))


def test_tasks_of_a_single_size_give_no_task_cost():
    assert task_cost([Task(10, 1.0, 1.0), Task(10, 2.0, 2.0)]) is None


def test_a_map_task_reads_its_records_cpu_and_elapsed_time():
    task = {"id": "task_1_m_000000", "elapsedTime": 8500}
    counters = {
        "jobTaskCounters": {
            "taskCounterGroup": [
                {
                    "counterGroupName": "org.apache.hadoop.mapreduce.TaskCounter",
                    "counter": [
                        {"name": "MAP_INPUT_RECORDS", "value": 1_880_000},
                        {"name": "CPU_MILLISECONDS", "value": 2_250},
                    ],
                },
                {"counterGroupName": "Quality", "counter": [{"name": "BAD_PRICE", "value": 3}]},
            ]
        }
    }

    assert mr_task(task, counters) == Task(1_880_000, 2.25, 8.5)


def test_spark_tasks_keep_the_successful_attempts():
    def attempt(status: str, records: int) -> dict:
        metrics = {
            "inputMetrics": {"recordsRead": records},
            "executorCpuTime": 1_500_000_000,
            "executorRunTime": 1_600,
        }
        return {"status": status, "taskMetrics": metrics}

    assert spark_tasks([attempt("SUCCESS", 10), attempt("KILLED", 4)]) == [Task(10, 1.5, 1.6)]


def test_the_main_stage_is_the_completed_stage_that_read_the_most_records():
    stages = [
        {"stageId": 0, "status": "COMPLETE", "inputRecords": 5_166},
        {"stageId": 3, "status": "COMPLETE", "inputRecords": 10_054_797},
        {"stageId": 4, "status": "FAILED", "inputRecords": 99_000_000},
    ]

    assert main_stage(stages)["stageId"] == 3


def test_mapreduce_holds_one_core_per_task_attempt_plus_the_application_master():
    counters = {"VCORES_MILLIS_MAPS": 48_000, "VCORES_MILLIS_REDUCES": 2_200}

    assert mr_core_seconds(counters, 18.2) == pytest.approx(68.4)


def test_spark_holds_each_executor_s_cores_until_removal_or_the_end():
    executors = [
        {"id": "driver", "totalCores": 0, "addTime": "2026-11-26T18:43:35.390GMT"},
        {"id": "1", "totalCores": 2, "addTime": "2026-11-26T18:43:38.000GMT"},
        {
            "id": "2",
            "totalCores": 2,
            "addTime": "2026-11-26T18:43:38.000GMT",
            "removeTime": "2026-11-26T18:43:43.000GMT",
        },
    ]
    end = 1795718628000
    """2026-11-26T18:43:48.000 UTC."""

    assert spark_core_seconds(executors, 16.0, end) == pytest.approx(16.0 + 2 * 10 + 2 * 5)


class _ReplayedRM(Cluster):
    def __init__(self, readings: list[tuple[str, int, int]]) -> None:
        super().__init__(out=None, settle_s=0)
        self.readings = iter(readings)

    def app(self, app_id: str) -> dict:
        state, memory, vcores = next(self.readings)
        return {"id": app_id, "state": state, "memorySeconds": memory, "vcoreSeconds": vcores}


def test_resources_are_read_once_two_readings_of_the_finished_application_agree():
    cluster = _ReplayedRM(
        [
            ("RUNNING", 100, 1),
            ("FINISHED", 50_844, 83),
            ("FINISHED", 53_277, 86),
            ("FINISHED", 53_277, 86),
            ("FINISHED", 99_999, 99),
        ]
    )

    assert cluster.settled_app("app")["memorySeconds"] == 53_277


def test_checks_compare_every_variant_with_the_first_mapreduce_run():
    tuned = Result("FR\t10.01\t2\n", RESULT.counters)

    assert checks([measure("mapreduce"), measure("mapreduce-tuned", tuned)], None) == [
        r"mapreduce-tuned #1: output 'FR\t10.01\t2\n' != 'FR\t10.00\t2\n'",
        "mapreduce-tuned #1: revenue FR: (Decimal('10.00'), 2) != (Decimal('10.01'), 2)",
    ]


def test_the_volume_report_tests_each_pair_of_variants_and_fits_the_task_costs():
    measures = [
        measure(variant, elapsed_s=seconds)
        for variant, seconds in [("mapreduce", 20.0), ("spark", 16.0)] * 3
    ]

    report = "\n".join(volume_report("full", measures, "context", [], manifests=True))

    assert "| mapreduce | spark | 3, 3 | 9 / 9 | 0.050 | 0.100 |" in report
    assert "| 1.00 | 1.00 | 1.00 | 2.00 |" in report
    assert "Les compteurs Quality sont égaux aux anomalies des manifestes." in report


def _counter(name: str, value: int) -> dict:
    return {"name": name, "totalCounterValue": value}


def _orders(anomalies: dict) -> dict:
    return {"source": "orders", "anomalies": anomalies}
