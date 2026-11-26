"""Inputs outside the contract: not strict JSON, not finite, dated after their emission."""

import json
from dataclasses import replace
from datetime import UTC, datetime

from stream_helpers import drop_files, event, run_stream

from gtdp.io import read_table
from gtdp.streaming.stream import AGGREGATES, ALERTS, REJECTS, SCHEMAS

GOOD = json.loads(event("G1", "00:00:05", 60.0))
CLOSING = [[event("N1", "00:10:00", 60.0)], [event("N2", "00:10:05", 60.0)]]


def variant(event_id: str, **changes) -> str:
    return json.dumps({**GOOD, "event_id": event_id, **changes})


NOT_ONE_STRICT_OBJECT = {
    "trailing garbage": variant("X1") + " trailing garbage",
    "single quotes": variant("X2").replace('"', "'"),
    "bare NaN": variant("X3").replace("60.0", "NaN"),
    "quoted NaN": variant("X4", value="NaN"),
    "quoted Infinity": variant("X5", value="Infinity"),
    "bare -Infinity": variant("X6").replace("60.0", "-Infinity"),
    "two objects": variant("X7") + variant("X8"),
    "array of one object": "[" + variant("X9") + "]",
    "brace then garbage": variant("X10") + ' } , {"a": 1',
    "bracket then garbage": variant("X11") + "] , [" + variant("X12"),
    "extra key after the object": variant("X13") + ' , "k": 1',
    "value as a string": variant("X14", value="60.0"),
    "time without offset": variant("X15", event_time="2026-01-01 00:00:05"),
}
OVERFLOW = variant("X20").replace("60.0", "1e400")


def table(spark, paths, name):
    return read_table(spark, paths.tables[name], SCHEMAS[name])


def windows(spark, paths) -> dict[tuple[int, str], tuple]:
    rows = table(spark, paths, AGGREGATES).collect()
    found = {
        (int(r.window_start.replace(tzinfo=UTC).timestamp()), r.sensor_id): (
            r.event_count,
            r.min_value,
            r.max_value,
            r.avg_value,
        )
        for r in rows
    }
    assert len(found) == len(rows)
    return found


def epoch(at: str) -> int:
    return int(datetime.fromisoformat(f"2026-01-01T{at}+00:00").timestamp())


def test_a_line_that_is_not_exactly_one_strict_json_object_with_a_finite_value_is_rejected(
    spark, stream_lake
):
    paths, settings = stream_lake
    drop_files(paths, [[variant("G1"), *NOT_ONE_STRICT_OBJECT.values(), OVERFLOW], *CLOSING])

    run_stream(spark, paths, settings)

    reasons = {r.raw_line: r.reject_reason for r in table(spark, paths, REJECTS).collect()}
    assert {
        name: reasons.get(line) for name, line in NOT_ONE_STRICT_OBJECT.items()
    } == dict.fromkeys(NOT_ONE_STRICT_OBJECT, "MALFORMED_JSON")
    assert reasons[OVERFLOW] == "BAD_VALUE"
    assert windows(spark, paths)[(epoch("00:00:00"), "FR-LYON-M01-TEMP")] == (1, 60.0, 60.0, 60.0)
    assert table(spark, paths, ALERTS).count() == 0


def test_an_event_dated_after_its_emission_is_rejected_and_the_stream_goes_on(spark, stream_lake):
    paths, settings = stream_lake
    drop_files(
        paths,
        [
            [event("E1", "00:00:05", 60.0)],
            # A clock glitch: the event says 23:00, its emitter sent it at 00:00:10.
            [event("E2", "00:00:10", 60.0), event("BAD", "23:00:00", 60.0, emitted="00:00:10")],
            [event("E3", "00:00:15", 60.0)],
            [event("E4", "00:01:05", 99.0)],
            [event("E5", "00:02:05", 60.0)],
            [event("E6", "00:05:05", 60.0)],
            [event("E7", "00:08:05", 99.0)],
        ],
    )

    metrics = run_stream(spark, paths, replace(settings, future_tolerance_s=60))

    rejected = table(spark, paths, REJECTS).collect()
    assert [(r.reject_reason, json.loads(r.raw_line)["event_id"]) for r in rejected] == [
        ("FUTURE_EVENT", "BAD")
    ]
    assert metrics[AGGREGATES]["rows_dropped_by_watermark"] == 0
    assert sum(count for count, *_ in windows(spark, paths).values()) == 6
    assert sorted(r.event_id for r in table(spark, paths, ALERTS).collect()) == ["E4", "E7"]


def test_an_event_within_the_tolerance_after_its_emission_is_kept(spark, stream_lake):
    paths, settings = stream_lake
    drop_files(paths, [[event("E1", "00:00:50", 60.0, emitted="00:00:05")], *CLOSING])

    run_stream(spark, paths, replace(settings, future_tolerance_s=60))

    assert table(spark, paths, REJECTS).count() == 0
    assert windows(spark, paths)[(epoch("00:00:00"), "FR-LYON-M01-TEMP")][0] == 1
