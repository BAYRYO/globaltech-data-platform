"""The sensor stream over a generated day, checked against its manifest and against a recount.

The recount reads the files with Python alone and applies the contract's rules: an unreadable
line or a null value is rejected, an ``event_id`` counts once, an event more than 2 minutes older
than its file is beyond the watermark, a window is final once the last event time minus 2 minutes
passes its end, and an alert is a (sensor, minute) with a value beyond the contract's limits.
"""

import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from stream_helpers import drop_files, lake, run_stream

from gtdp.datagen.model import PROFILES
from gtdp.datagen.sensors import write_sensors
from gtdp.io import read_table
from gtdp.streaming.sensors import AGGREGATES_SCHEMA, ALERTS_SCHEMA, REJECTS_SCHEMA
from gtdp.streaming.stream import AGGREGATES, ALERTS, REJECTS

LIMITS = {"temperature": (None, 85.0), "vibration": (None, 7.1), "pressure": (4.0, 8.0)}
WATERMARK = timedelta(minutes=2)


def recount(day: Path) -> dict:
    seen, values, alerts, beyond = set(), defaultdict(list), set(), 0
    last = datetime.min.replace(tzinfo=UTC)
    for path in sorted(day.glob("sensors_*.jsonl")):
        start = datetime.strptime(path.stem, "sensors_%Y%m%dT%H%M%S").replace(tzinfo=UTC)
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event["value"] is None:
                continue
            at = datetime.strptime(event["event_time"], "%Y-%m-%dT%H:%M:%S.%f%z")
            last = max(last, at)
            if event["event_id"] in seen:
                continue
            seen.add(event["event_id"])
            if at < start - WATERMARK:
                beyond += 1
                continue
            minute = int(at.replace(second=0, microsecond=0).timestamp())
            values[(minute, event["sensor_id"])].append(event["value"])
            low, high = LIMITS[event["metric"]]
            if (low is not None and event["value"] < low) or event["value"] > high:
                alerts.add((event["sensor_id"], minute))
    final = (last - WATERMARK).timestamp()
    windows = {
        key: (len(found), min(found), max(found), sum(found) / len(found))
        for key, found in values.items()
        if key[0] + 60 <= final
    }
    return {"windows": windows, "alerts": alerts, "beyond_watermark": beyond}


@pytest.fixture(scope="module")
def ci_day_stream(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("ci")
    written = write_sensors(root, 42, PROFILES["ci"])
    day = root / "sensors" / "2026-01-01"
    paths, settings = lake(root / "lake")
    files = sorted(day.glob("sensors_*.jsonl"))
    drop_files(paths, [path.read_text(encoding="utf-8").splitlines() for path in files])
    metrics = run_stream(spark, paths, replace(settings, max_files_per_trigger=4))
    manifest = sum((Counter(file.anomalies) for file in written), Counter())
    return paths, metrics, manifest, sum(file.rows for file in written), recount(day)


def test_every_line_is_read_and_each_rejected_one_has_its_contract_code(spark, ci_day_stream):
    paths, metrics, manifest, lines, _ = ci_day_stream

    rejected = read_table(spark, paths.tables[REJECTS], REJECTS_SCHEMA)

    assert metrics[AGGREGATES]["input_rows"] == lines
    assert Counter(row.reject_reason for row in rejected.collect()) == {
        "MALFORMED_JSON": manifest["MALFORMED_JSON"],
        "NULL_VALUE": manifest["NULL_VALUE"],
    }


def test_duplicates_and_very_late_events_are_dropped_and_counted(ci_day_stream):
    _, metrics, manifest, _, expected = ci_day_stream

    assert metrics[AGGREGATES]["rows_deduplicated"] == manifest["DUPLICATE_EVENT"]
    assert metrics[AGGREGATES]["rows_dropped_by_watermark"] == manifest["VERY_LATE_EVENT"]
    assert expected["beyond_watermark"] == manifest["VERY_LATE_EVENT"]


def test_final_windows_match_the_recount(spark, ci_day_stream):
    paths, _, _, _, expected = ci_day_stream

    rows = read_table(spark, paths.tables[AGGREGATES], AGGREGATES_SCHEMA).select(
        F.col("window_start").cast("long").alias("minute"),
        "sensor_id",
        "event_count",
        "min_value",
        "max_value",
        "avg_value",
    )
    found = {(r.minute, r.sensor_id): r for r in rows.collect()}

    assert rows.count() == len(found) == len(expected["windows"])
    assert found.keys() == expected["windows"].keys()
    for key, (count, low, high, mean) in expected["windows"].items():
        assert (found[key].event_count, found[key].min_value, found[key].max_value) == (
            count,
            low,
            high,
        )
        assert found[key].avg_value == pytest.approx(mean, abs=1e-9)


def test_spikes_raise_one_alert_per_sensor_and_minute(spark, ci_day_stream):
    paths, _, _, _, expected = ci_day_stream

    alerts = read_table(spark, paths.tables[ALERTS], ALERTS_SCHEMA).collect()

    assert len(alerts) == len(expected["alerts"]) == 10  # 5 spiking sensors x 2 minutes
    assert {row.alert_id for row in alerts} == {
        hashlib.sha256(f"{sensor}|{minute}".encode()).hexdigest()
        for sensor, minute in expected["alerts"]
    }
