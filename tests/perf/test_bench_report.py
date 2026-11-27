"""The benchmark report: warm-up and overlapped runs left out, medians, the comparisons with their
exact Mann-Whitney p, the sensitivity to the first measured run, and runs.json at full precision."""

import pytest

from gtdp.perf import bench
from gtdp.perf.bench import (
    Run,
    change,
    compare,
    comparisons_markdown,
    load_runs,
    markdown,
    rows_check,
    save_runs,
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
        key_stage={"stage": 7, "attempt": 0, "duration_ratio": 2.0, "read_ratio": 3.0},
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


def test_every_warm_up_round_is_left_out_of_the_medians():
    rows = summarize([run("csv", -1, 50.0), run("csv", 0, 40.0), *RUNS])
    assert (rows[0]["n"], rows[0]["max_s"]) == (3, 12.0)


def test_a_run_that_another_application_overlapped_is_left_out_and_listed():
    late = Run("csv", 4, "x/csv/4", 0, 50.0, "0 other", [1, "ab"], overlapping=["application_7"])
    rows = summarize([*RUNS, late])
    assert (rows[0]["n"], rows[0]["max_s"]) == (3, 12.0)
    assert "exclues des statistiques : `csv` 4 (application_7)." in markdown(rows)
    assert compare([*RUNS, late], [("parquet", "csv")])[0]["n"] == (3, 3)


def test_a_run_with_another_result_shows_two_checks():
    rows = summarize([*RUNS, run("parquet", 4, 3.0, check=(2, "cd"))])
    assert len(rows[0]["checks"]) == 1
    assert len(rows[1]["checks"]) == 2


def five(variant: str, walls, **stages) -> list[Run]:
    return [run(variant, n, wall, **stages) for n, wall in enumerate(walls, start=1)]


def test_a_comparison_gives_the_change_and_the_exact_two_sided_p():
    runs = five("csv", [10, 11, 12, 13, 14]) + five("parquet", [1, 2, 3, 4, 5], inputBytes=10)
    [row] = compare(runs, [("csv", "parquet")])
    assert row["n"] == (5, 5)
    assert row["medians_s"] == (12, 3)
    assert row["change_pct"] == -75.0
    assert row["read_change_pct"] == -90.0
    # Every parquet run is faster: U = 0, and 2 of the 252 splits are as extreme.
    assert row["u"] == 0
    assert row["p"] == pytest.approx(2 / 252)
    assert change(0, 5) == 0.0


def test_the_stage_times_are_compared_when_every_run_has_them():
    runs = five("a", [1, 2, 3, 4, 5]) + five("b", [1, 2, 3, 4, 5])
    for r in runs:
        r.stage_walls = {"scan": 1.0 + r.run / 100, "key": (10 if r.variant == "b" else 0) + r.run}
    [row] = compare(runs, [("a", "b")])
    assert row["p"] == 1.0
    assert row["scan_p"] == 1.0
    assert row["key_p"] == pytest.approx(2 / 252)
    assert compare(RUNS, [("csv", "parquet")])[0]["key_p"] is None


def test_the_sensitivity_drops_the_first_measured_run():
    """Run 1 of the variant is its only run slower than a reference run: with it U = 1 of 25
    (p = 0.016); without it the 4 runs are all faster (U = 0 of 16, p = 2/70)."""
    runs = five("ref", [10, 11, 12, 13, 14]) + five("var", [10.5, 2, 3, 4, 5])
    [with_first] = compare(runs, [("ref", "var")])
    [without] = compare(runs, [("ref", "var")], first_run=2)
    assert (with_first["u"], round(with_first["p"], 3)) == (1, 0.016)
    assert (without["n"], without["u"], without["p"]) == ((4, 4), 0, pytest.approx(2 / 70))


def test_the_sensitivity_table_says_when_a_conclusion_changes():
    runs = five("ref", [10, 11, 12, 13, 14]) + five("var", [1, 2, 3, 15, 16])
    pairs = [("ref", "var")]
    table = comparisons_markdown(compare(runs, pairs, first_run=2), compare(runs, pairs))
    assert table.splitlines()[0].endswith("| Retenu avec l'exécution 1 | Conclusion |")
    assert table.splitlines()[2].endswith("| non | inchangée |")
    # With run 1, U = 2 of 25 (p = 0.032, retained); without it, U = 1 of 16 (p = 4/70).
    shifted = five("ref", [10, 11, 12, 13, 14]) + five("var", [1, 2, 3, 4, 11.5])
    table = comparisons_markdown(compare(shifted, pairs, first_run=2), compare(shifted, pairs))
    cells = [cell.strip() for cell in table.splitlines()[2].strip("|").split("|")]
    assert (cells[7], cells[8], cells[-2], cells[-1]) == ("0.057", "non", "oui", "change")


def test_runs_json_keeps_every_duration_at_full_precision(tmp_path):
    header = {"experiment": "x", "app_id": "application_1", "settings": {}, "warmup": 1}
    header["variants"] = [{"name": "csv", "metric": "inputBytes", "family": "", "references": []}]
    runs = [run("csv", 0, 1.1024999), run("csv", 1, 1.1015001, check=([3, "ef"], [4, "gh"]))]
    runs[1].details = {"after_unpersist": []}
    runs[1].stage_walls = {"scan": 0.5, "key": 0.25}
    runs[1].overlapping = ["application_9"]
    save_runs(tmp_path / "runs.json", header, runs)
    loaded_header, loaded = load_runs(tmp_path / "runs.json")
    assert loaded_header == header
    assert [r.wall_s for r in loaded] == [1.1024999, 1.1015001]
    assert loaded[1] == runs[1]
    assert loaded[1].group == "x/csv/1"


def test_the_report_is_rebuilt_from_runs_json(tmp_path):
    header = {"experiment": "x", "app_id": "application_1", "settings": {}, "warmup": 1}
    header["variants"] = [
        {"name": "csv", "metric": "inputBytes", "family": "", "references": []},
        {"name": "parquet", "metric": "inputBytes", "family": "", "references": ["csv"]},
    ]
    runs = five("csv", [10, 11, 12, 13, 14]) + five("parquet", [1, 2, 3, 4, 5])
    save_runs(tmp_path / "runs.json", header, runs)
    assert bench.main(["report", "--out", str(tmp_path)]) == 0
    text = (tmp_path / "report.md").read_text()
    assert "| `csv` | `parquet` | 5 contre 5 | 12.00 → 3.00 | -75.0 % |" in text
    assert "| 0 / 25 | 0.008 | oui |" in text
    assert "[7](http://localhost:18080/history/application_1/stages/stage/?id=7&attempt=0)" in text


def test_the_table_has_one_line_per_variant_with_spaced_thousands():
    table = markdown(summarize(RUNS)).splitlines()
    assert len(table) == 6
    assert table[2].startswith("| `csv` | 3 | 11.00 | 10.00 à 12.00 | 100 | 0 | 0 | 0 | — |")
    assert table[2].endswith("| 2.00 | 3.00 | 7 |")


def test_the_stored_bytes_change_is_against_the_first_and_the_previous_layout():
    table = stored_markdown(
        {
            "csv": {"logical": 1000, "physical": 3000},
            "parquet": {"logical": 400, "physical": 1200},
            "parquet_zstd": {"logical": 300, "physical": 900},
        }
    ).splitlines()
    assert table[2] == "| `csv` | 1 000 | 3 000 | +0.0 % | +0.0 % |"
    assert table[4] == "| `parquet_zstd` | 300 | 900 | -70.0 % | -25.0 % |"


def test_the_result_check_ignores_row_order():
    assert rows_check([(1, "a"), (2, "b")]) == rows_check([(2, "b"), (1, "a")])
    assert rows_check([(1, "a")]) != rows_check([(1, "b")])
