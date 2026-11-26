"""The Hadoop Streaming mapper and reducer, chained by a cat | mapper | sort | reducer pipe."""

import subprocess
import sys
from pathlib import Path

REDUCER = Path(__file__).resolve().parents[2] / "mapreduce" / "reducer.py"


def test_revenue_per_country_is_rounded_once_per_country(edge_drop, mapreduce_pipe):
    assert mapreduce_pipe(edge_drop).revenue == edge_drop.revenue


def test_each_line_is_counted_once_under_its_first_failing_rule(edge_drop, mapreduce_pipe):
    assert mapreduce_pipe(edge_drop).counters == edge_drop.counters


def test_the_combiner_on_each_map_output_leaves_the_result_unchanged(edge_drop, mapreduce_pipe):
    assert mapreduce_pipe(edge_drop, combine_halves=True) == mapreduce_pipe(edge_drop)


def test_quality_counters_equal_the_anomalies_of_the_manifests(ci_drop, mapreduce_pipe):
    counters = mapreduce_pipe(ci_drop).counters

    assert {k: v for k, v in counters.items() if k.startswith("Quality.")} == ci_drop.counters


def test_the_counters_cover_every_line_of_the_files(ci_drop, mapreduce_pipe):
    lines = sum(path.read_bytes().count(b"\n") for path in ci_drop.orders)

    assert sum(mapreduce_pipe(ci_drop).counters.values()) == lines


def test_the_reducer_refuses_an_unknown_option():
    done = subprocess.run(
        [sys.executable, str(REDUCER), "--combiner"],
        input="",
        capture_output=True,
        text=True,
        check=False,
    )

    assert done.returncode != 0
    assert "Usage" in done.stderr
