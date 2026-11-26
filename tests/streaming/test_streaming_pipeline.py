import hashlib
from collections import Counter
from datetime import UTC, datetime

import pytest
from pyspark.errors import StreamingQueryException
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from stream_helpers import drop_files, event, lake, run_stream

from gtdp.io import read_table
from gtdp.streaming import stream
from gtdp.streaming.sensors import AGGREGATES_SCHEMA, ALERTS_SCHEMA, BATCH_ID, REJECTS_SCHEMA
from gtdp.streaming.stream import AGGREGATES, ALERTS, REJECTS, SCHEMAS, checkpoint_state

T1, T2 = "FR-LYON-M01-TEMP", "FR-LYON-M02-TEMP"


def table(spark, paths, name):
    return read_table(spark, paths.tables[name], SCHEMAS[name])


def readable(df):
    """``df`` with its times as ``HH:mm:ss`` (UTC) and its source files as bare file names."""
    columns = []
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampType):
            columns.append(F.date_format(field.name, "HH:mm:ss").alias(field.name))
        elif field.name == "source_file":
            columns.append(F.element_at(F.split(field.name, "/"), -1).alias(field.name))
        else:
            columns.append(field.name)
    return df.select(*columns)


def epoch(at: str) -> int:
    return int(datetime.fromisoformat(f"2026-01-01T{at}").replace(tzinfo=UTC).timestamp())


def windows(spark, paths):
    """{(window start "HH:MM:SS", sensor): (count, avg, min, max, batch_id)} of the aggregates."""
    rows = table(spark, paths, AGGREGATES).select(
        F.date_format("window_start", "HH:mm:ss").alias("start"),
        (F.col("window_end").cast("long") - F.col("window_start").cast("long")).alias("width"),
        "sensor_id",
        "event_count",
        "avg_value",
        "min_value",
        "max_value",
        BATCH_ID,
    )
    found = {}
    for row in rows.collect():
        assert row.width == 60
        key = (row.start, row.sensor_id)
        assert key not in found, f"window {key} written twice"
        found[key] = (row.event_count, row.avg_value, row.min_value, row.max_value, row.batch_id)
    return found


@pytest.fixture(scope="module")
def late_and_duplicate_stream(spark, tmp_path_factory):
    """Five files, one micro-batch each, then the final batch without data.

    The watermark (max event time - 2 min) becomes 00:08:00 after batch 1; batch 3 filters late
    events with it, so its 00:00:20 event is dropped while its 00:09:30 one is kept.
    """
    paths, settings = lake(tmp_path_factory.mktemp("stream") / "lake")
    drop_files(
        paths,
        [
            [
                event("E1", "00:00:05", 10.0),
                event("E2", "00:00:35", 30.0),
                event("E3", "00:00:10", 50.0, machine="FR-LYON-M02"),
                event("E4", "00:01:10", 20.0),
                "not json",
                '{"event_id":"E90","value":,"metric":"temperature"}',
                "",
                event("E91", "00:00:15", None),
                event("E92", "00:00:15", 40.0).replace('"event_time"', '"event_tme"'),
            ],
            [event("E1", "00:00:05", 10.0), event("E5", "00:10:00", 20.0)],
            [event("E6", "00:10:05", 21.0)],
            [event("E7", "00:00:20", 70.0), event("E8", "00:09:30", 30.0)],
            [event("E9", "00:20:00", 1.0)],
        ],
    )
    metrics = run_stream(spark, paths, settings)
    return paths, metrics


def test_aggregates_count_average_min_max_per_window_and_sensor(spark, late_and_duplicate_stream):
    paths, _ = late_and_duplicate_stream

    found = {key: value[:4] for key, value in windows(spark, paths).items()}

    assert found == {
        ("00:00:00", T1): (2, 20.0, 10.0, 30.0),
        ("00:00:00", T2): (1, 50.0, 50.0, 50.0),
        ("00:01:00", T1): (1, 20.0, 20.0, 20.0),
        ("00:09:00", T1): (1, 30.0, 30.0, 30.0),
        ("00:10:00", T1): (2, 20.5, 20.0, 21.0),
    }


def test_a_window_is_written_once_when_the_watermark_passes_its_end(
    spark, late_and_duplicate_stream
):
    paths, _ = late_and_duplicate_stream

    batches = {key: value[4] for key, value in windows(spark, paths).items()}

    # Watermark 00:08:00 from batch 2, 00:18:00 from batch 5: the 00:20 window stays open.
    assert batches == {
        ("00:00:00", T1): 2,
        ("00:00:00", T2): 2,
        ("00:01:00", T1): 2,
        ("00:09:00", T1): 5,
        ("00:10:00", T1): 5,
    }


def test_an_event_later_than_the_watermark_is_dropped_and_counted(spark, late_and_duplicate_stream):
    paths, metrics = late_and_duplicate_stream

    assert metrics[AGGREGATES]["rows_dropped_by_watermark"] == 1
    assert windows(spark, paths)[("00:00:00", T1)][0] == 2  # E7 (00:00:20, value 70) is not there
    assert windows(spark, paths)[("00:09:00", T1)][0] == 1  # 30 s late, inside the watermark


def test_a_duplicate_event_id_is_counted_once(spark, late_and_duplicate_stream):
    paths, metrics = late_and_duplicate_stream

    assert metrics[AGGREGATES]["rows_deduplicated"] == 1
    assert windows(spark, paths)[("00:00:00", T1)][0] == 2


def test_unreadable_lines_and_null_values_go_to_rejects_with_their_reason(
    spark, late_and_duplicate_stream
):
    paths, metrics = late_and_duplicate_stream

    rejected = table(spark, paths, REJECTS).collect()

    assert Counter(row.reject_reason for row in rejected) == {
        "MALFORMED_JSON": 3,
        "NULL_VALUE": 1,
        "MISSING_FIELD": 1,
    }
    assert {row.raw_line for row in rejected} >= {"not json", ""}
    assert {row.source_file.rsplit("/", 1)[-1] for row in rejected} == {"sensors_0000.jsonl"}
    assert {row.batch_id for row in rejected} == {0}
    assert metrics[AGGREGATES]["input_rows"] == 15
    assert metrics[REJECTS]["rows_written"] == 5


def test_every_output_table_has_its_documented_schema(spark, late_and_duplicate_stream):
    paths, _ = late_and_duplicate_stream

    assert table(spark, paths, AGGREGATES).schema == AGGREGATES_SCHEMA
    assert table(spark, paths, REJECTS).schema == REJECTS_SCHEMA


def test_one_value_beyond_its_threshold_raises_exactly_one_alert(spark, stream_lake):
    paths, settings = stream_lake
    drop_files(
        paths,
        [
            [event("E1", "00:00:10", 60.0), event("E2", "00:00:20", 90.0)],
            [event("E3", "00:00:30", 61.0), event("E4", "00:01:05", 62.0)],
        ],
    )

    run_stream(spark, paths, settings)

    [alert] = readable(table(spark, paths, ALERTS)).collect()
    assert alert.asDict() == {
        "alert_id": hashlib.sha256(f"{T1}|{epoch('00:00:00')}".encode()).hexdigest(),
        "sensor_id": T1,
        "machine_id": "FR-LYON-M01",
        "site": "FR-LYON",
        "metric": "temperature",
        "window_start": "00:00:00",
        "window_end": "00:01:00",
        "value": 90.0,
        "threshold": 85.0,
        "direction": "above",
        "event_time": "00:00:20",
        "event_id": "E2",
        BATCH_ID: 0,
    }
    assert table(spark, paths, ALERTS).schema == ALERTS_SCHEMA


def test_the_alert_of_a_window_is_its_earliest_breach_and_comes_once(spark, stream_lake):
    paths, settings = stream_lake
    drop_files(
        paths,
        [
            [event("E1", "00:00:05", 60.0)],
            [event("E3", "00:00:40", 99.0), event("E2", "00:00:20", 90.0)],
            [event("E4", "00:00:10", 95.0), event("E5", "00:00:50", 97.0)],
        ],
    )

    run_stream(spark, paths, settings)

    alerts = readable(table(spark, paths, ALERTS)).collect()
    assert [(a.event_id, a.event_time, a.value, a.batch_id) for a in alerts] == [
        ("E2", "00:00:20", 90.0, 1)
    ]


def test_alerts_once_per_sensor_and_window_in_either_direction(spark, stream_lake):
    paths, settings = stream_lake
    pressure = {"metric": "pressure"}
    drop_files(
        paths,
        [
            [event("P1", "00:00:10", 3.5, **pressure)],
            [event("P2", "00:00:20", 3.0, **pressure), event("P3", "00:00:50", 9.0, **pressure)],
            [event("P4", "00:01:10", 8.5, **pressure), event("P5", "00:01:20", 8.0, **pressure)],
            [event("T1", "00:01:15", 85.0), event("V1", "00:01:15", 7.2, metric="vibration")],
        ],
    )

    run_stream(spark, paths, settings)

    alerts = readable(table(spark, paths, ALERTS)).orderBy("event_time").collect()
    assert [
        (a.event_id, a.window_start, a.direction, a.threshold, a.value, a.batch_id) for a in alerts
    ] == [
        ("P1", "00:00:00", "below", 4.0, 3.5, 0),
        ("P4", "00:01:00", "above", 8.0, 8.5, 2),
        ("V1", "00:01:00", "above", 7.1, 7.2, 3),
    ]


def test_a_batch_replayed_after_a_crash_rewrites_its_output_instead_of_adding_to_it(
    spark, tmp_path, monkeypatch
):
    files = [
        [event("E1", "00:00:10", 60.0), "bad line 0"],
        [event("E2", "00:03:00", 61.0), event("E1", "00:00:10", 60.0)],
        [event("E3", "00:03:10", 90.0), "bad line 2"],
        [event("E4", "00:06:00", 62.0)],
    ]
    clean, settings = lake(tmp_path / "clean")
    crashed, _ = lake(tmp_path / "crashed")
    drop_files(clean, files)
    drop_files(crashed, files)
    run_stream(spark, clean, settings)

    write = stream.write_partitions

    def write_then_crash(df, path, partitions):
        write(df, path, partitions)
        if partitions[BATCH_ID] == ["2"]:
            raise RuntimeError("driver lost after writing batch 2, before Spark committed it")

    monkeypatch.setattr(stream, "write_partitions", write_then_crash)
    with pytest.raises(StreamingQueryException, match="driver lost"):
        run_stream(spark, crashed, settings)
    monkeypatch.undo()
    for name in SCHEMAS:
        before = checkpoint_state(spark, crashed.checkpoint(name))
        assert before == {"last_planned_batch": 2, "last_committed_batch": 1}, name
        assert table(spark, crashed, name).where(F.col(BATCH_ID) == 2).count() > 0, name

    run_stream(spark, crashed, settings)

    for name in SCHEMAS:
        expected = sorted(readable(table(spark, clean, name)).collect())
        assert sorted(readable(table(spark, crashed, name)).collect()) == expected, name
    assert len(windows(spark, crashed)) == 2
