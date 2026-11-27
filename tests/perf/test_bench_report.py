"""The benchmark report: warm-up left out, medians, gains, and the result checks."""

from gtdp.perf.bench import (
    Run,
    gain,
    markdown,
    rows_check,
    runs_csv,
    stored_markdown,
    summarize,
)


def run(variant: str, number: int, wall_s: float, check=(1, "ab"), **stages) -> Run:
    return Run(
        variant=variant,
        run=number,
        group=f"x/{variant}/{number}",
        started_ms=0,
        wall_s=wall_s,
        yarn_check="0 other application",
        check=list(check),
        stages={"inputBytes": 100, **stages},
        key_stage={"duration_ratio": 2.0, "read_ratio": 3.0},
    )


RUNS = [
    run("csv", 0, 99.0),
    run("parquet", 0, 99.0),
    run("csv", 1, 10.0),
    run("parquet", 1, 2.0),
    run("csv", 2, 12.0),
    run("parquet", 2, 3.0),
    run("csv", 3, 11.0, inputBytes=700),
    run("parquet", 3, 4.0, inputBytes=50),
]


def test_the_warm_up_round_is_left_out_of_the_medians():
    rows = summarize(RUNS)
    assert [(r["variant"], r["n"], r["median_s"]) for r in rows] == [
        ("csv", 3, 11.0),
        ("parquet", 3, 3.0),
    ]
    assert (rows[0]["min_s"], rows[0]["max_s"]) == (10.0, 12.0)
    assert rows[1]["inputBytes"] == 100


def test_the_gain_is_against_the_first_variant():
    rows = summarize(RUNS)
    assert rows[0]["gain_pct"] == 0.0
    assert round(rows[1]["gain_pct"], 2) == 72.73
    assert rows[1]["step_gain_pct"] == rows[1]["gain_pct"]
    assert rows[1]["read_gain_pct"] == 0.0
    assert gain(0, 5) == 0.0


def test_a_run_with_another_result_shows_two_checks():
    rows = summarize([*RUNS, run("parquet", 4, 3.0, check=(2, "cd"))])
    assert len(rows[0]["checks"]) == 1
    assert len(rows[1]["checks"]) == 2


def test_the_table_has_one_line_per_variant_with_spaced_thousands():
    table = markdown(summarize(RUNS)).splitlines()
    assert len(table) == 4
    assert table[2].startswith("| `csv` | 3 | 11.00 | 10.00 à 12.00 | 0.0 % | 0.0 % | 100 |")


def test_the_stored_bytes_gain_is_against_the_first_and_the_previous_layout():
    table = stored_markdown(
        {
            "csv": {"logical": 1000, "physical": 3000},
            "parquet": {"logical": 400, "physical": 1200},
            "parquet_zstd": {"logical": 300, "physical": 900},
        }
    ).splitlines()
    assert table[2] == "| `csv` | 1 000 | 3 000 | 0.0 % | 0.0 % |"
    assert table[4] == "| `parquet_zstd` | 300 | 900 | 70.0 % | 25.0 % |"


def test_the_csv_has_one_line_per_run_with_its_yarn_check():
    lines = runs_csv(RUNS).splitlines()
    assert lines[0].startswith("variant,run,wall_s,inputBytes,")
    assert len(lines) == len(RUNS) + 1
    assert lines[1].startswith("csv,0,99.000,100,0,0,0,2.00,3.00,,0 other application,")


def test_the_result_check_ignores_row_order():
    assert rows_check([(1, "a"), (2, "b")]) == rows_check([(2, "b"), (1, "a")])
    assert rows_check([(1, "a")]) != rows_check([(1, "b")])
