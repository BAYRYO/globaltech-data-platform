"""The comparison tool's readings of the REST answers, its checks and its code count."""

from decimal import Decimal

from gtdp.perf.mr_compare import (
    Measure,
    Result,
    checks,
    code_lines,
    differences,
    jobs_window_s,
    manifest_quality,
    mr_counters,
    overlapping,
    parse_revenue,
    result_counters,
    stage_totals,
)

RESULT = Result({"FR": (Decimal("10.00"), 2)}, {"Lines.COMPLETED": 2, "Quality.BAD_PRICE": 1})


def measure(engine: str, result: Result = RESULT, inputs=("/orders/d=1",), lines_read=3):
    return Measure(engine, 1, "app", 1.0, 1.0, 1.0, 1, 1, lines_read, 1, 1, 1, list(inputs), result)


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


def test_a_one_cent_gap_and_a_missing_counter_are_both_reported():
    other = Result({"FR": (Decimal("10.01"), 2)}, {"Lines.COMPLETED": 2})

    assert differences(RESULT, other) == [
        "revenue FR: (Decimal('10.00'), 2) != (Decimal('10.01'), 2)",
        "counter Quality.BAD_PRICE: 1 != None",
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


def _counter(name: str, value: int) -> dict:
    return {"name": name, "totalCounterValue": value}


def _orders(anomalies: dict) -> dict:
    return {"source": "orders", "anomalies": anomalies}
