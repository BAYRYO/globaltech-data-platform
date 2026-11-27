"""Review A of PR #20 (measurement validity): recomputations from the committed evidence.

Every test named ``test_finding_*`` fails at head 9d1da61 and proves one finding of the review.
The ``test_control_*`` tests pass: they show the recomputation method agrees with the document
where the document is right.

Adapted in the fix round where a decision of the lead changed what the test can check:

- Decision 3: the evidence is ``runs.json`` at full precision, and ``runs.csv``, rounded to the
  millisecond, is gone, so ``walls`` reads ``runs.json``. The two disputed p-values follow from it
  as documented (0.841 and 0.421); the CSV gave 0.802 and 0.341 because its rounding tied two
  durations. The tool's p-values are in ``compare``, one row per declared pair, so the report test
  asks ``compare`` for the pair.
- Decisions 4, 6 and 7 rewrite statements of the documents, and no run is repeated: the data
  cannot change. The three tests that asserted the old statements now recompute the same figures
  and assert the corrected statements: the warm-up observation and its sensitivity check, the AQE
  gains against the decision rule, and the MapReduce summary from the campaign.
"""

import re
import statistics
from pathlib import Path

import pytest

from gtdp.config import load_config
from gtdp.perf.bench import (
    Run,
    compare,
    comparisons,
    comparisons_markdown,
    load_runs,
    markdown,
    summarize,
)
from gtdp.perf.mr_compare import engine_comparison, mann_whitney, synthesis

DOCS = Path(__file__).resolve().parents[2] / "docs"
EVIDENCE = DOCS / "perf-evidence"


def walls(experiment: str) -> dict[str, list[float]]:
    """Measured wall times (run > 0) per variant of a committed ``runs.json``, in run order."""
    found: dict[str, list[float]] = {}
    for run in load_runs(EVIDENCE / experiment / "runs.json")[1]:
        if run.run > 0:
            found.setdefault(run.variant, []).append(run.wall_s)
    return found


def two_sided(experiment: str, a: str, b: str) -> float:
    times = walls(experiment)
    return round(mann_whitney(times[a], times[b])[2], 3)


@pytest.mark.parametrize(
    ("experiment", "a", "b", "documented"),
    [
        ("formats", "csv", "parquet", 0.008),
        ("formats", "parquet", "parquet_part", 0.008),
        ("skew-4slots", "baseline", "salted-16", 0.016),
        ("skew-4slots", "baseline", "aqe-skew-join", 0.841),
        ("skew-8slots", "baseline", "aqe-skew-join", 0.056),
        ("cache", "no-cache-3", "cache-3", 0.008),
        ("partitions", "sp-16", "sp-4", 0.690),
        ("partitions", "sp-16", "sp-64", 0.548),
        ("partitions", "sp-16", "sp-200", 0.016),
        ("partitions", "sp-16", "sp-200-aqe-coalesce", 0.016),
    ],
)
def test_control_the_committed_runs_give_the_documented_p_value(experiment, a, b, documented):
    assert two_sided(experiment, a, b) == documented


@pytest.mark.parametrize(
    ("experiment", "a", "b", "documented"),
    [
        # docs/performance.md, KAN-44 table: "Compression : snappy vers zstd ... 0,841"
        ("formats", "parquet_part", "parquet_part_zstd", 0.841),
        # docs/performance.md, KAN-46 cache table: "Jointure diffusée | 6 | ... | 0,421"
        ("cache", "no-cache-6", "cache-6", 0.421),
    ],
)
def test_finding_two_documented_p_values_do_not_follow_from_the_committed_runs(
    experiment, a, b, documented
):
    """``runs.csv`` rounded to the millisecond, which tied two durations; ``runs.json``, the only
    file with the full durations, is now the evidence."""
    assert two_sided(experiment, a, b) == documented


def test_finding_the_benchmark_report_gives_no_p_value():
    """docs/performance.md, "Protocole commun": "Deux outils produisent toutes les mesures" and
    "Différence retenue. Test exact de Mann-Whitney sur les durées, 5 contre 5". The report of
    ``bench.py`` now holds the p of every declared comparison."""
    runs = [
        Run(variant, number, f"x/{variant}/{number}", 0, wall_s, "0 other application", [1, "ab"])
        for number in range(6)
        for variant, wall_s in (("baseline", 2.0 + number / 10), ("fix", 1.0 + number / 10))
    ]
    [row] = compare(runs, [("baseline", "fix")])
    assert any(key.startswith("p") for key in row), sorted(row)
    assert re.search(r"\bp\b", comparisons_markdown([row]).splitlines()[0])
    assert markdown(summarize(runs))


def test_finding_the_evidence_lacks_the_runs_json_fields_the_document_cites():
    """docs/performance.md cites ``runs.json`` three times as the proof (``yarn_check``,
    ``overlapping``, ``after_unpersist``); the brief wanted the raw evidence in ``docs/
    perf-evidence`` or in the PR comment. ``after_unpersist`` and the cached storage of each run
    are in neither; the result fingerprints are one sentence of the PR comment, not data."""
    assert "after_unpersist" in (DOCS / "performance.md").read_text(encoding="utf-8")
    evidence = "".join(
        path.read_text(encoding="utf-8") for path in EVIDENCE.rglob("*") if path.is_file()
    )
    assert "after_unpersist" in evidence
    assert "bd45a992d46c55b8" in evidence


def test_finding_one_warm_up_round_leaves_the_first_measured_run_the_slowest():
    """docs/performance.md: "Un tour d'échauffement (exécution 0), exclu des résultats, puis
    5 tours". With exchangeable runs, run 1 is the slowest of 5 in a fifth of the variants.

    Decision 7 keeps the runs: the document now states the observation, the tool defaults to 2
    warm-up rounds, and every report recomputes each comparison without run 1. One conclusion
    changes, the 8-slot salting, and the document says so."""
    slowest_first = {
        experiment: sum(times[0] == max(times) for times in walls(experiment).values())
        for experiment in ("formats", "skew-4slots", "skew-first-txid-salt")
    }
    variants = sum(len(walls(experiment)) for experiment in slowest_first)
    assert (sum(slowest_first.values()), variants) == (12, 14)
    doc = " ".join((DOCS / "performance.md").read_text(encoding="utf-8").split())
    assert "12 des 14 variantes" in doc
    assert load_config("cluster").perf.warmup_rounds == 2
    changed = []
    for experiment in (*slowest_first, "skew-8slots", "cache", "partitions"):
        header, runs = load_runs(EVIDENCE / experiment / "runs.json")
        pairs = comparisons(header)
        for full, without in zip(compare(runs, pairs), compare(runs, pairs, 2), strict=True):
            if (full["p"] < 0.05) != (without["p"] < 0.05):
                changed.append((experiment, full["variant"]))
    assert changed == [("skew-8slots", "salted-16")]


def test_finding_the_aqe_gain_is_told_against_the_documents_own_decision_rule():
    """docs/performance.md: "Une différence est retenue quand le p bilatéral est inférieur à
    0,05". The 8-slot gain (p = 0.056) is not retained by the rule, and neither is the gain of
    the first 4-slot series, same baseline and AQE code and settings (p = 0.056 at full
    precision; the rounded CSV gave 0.048). The document now says both."""
    eight_slots = two_sided("skew-8slots", "baseline", "aqe-skew-join")
    four_slots_first_series = two_sided("skew-first-txid-salt", "baseline", "aqe-skew-join")
    assert (eight_slots, four_slots_first_series) == (0.056, 0.056)
    doc = " ".join((DOCS / "performance.md").read_text(encoding="utf-8").split())
    assert "AQE gagne 0,23 s (\u22129,9 %, p = 0,056), une différence que la règle ne retient pas" in doc
    assert "AQE gagne 0,16 s (\u22126,0 %, p = 0,056), non retenu non plus" in doc


def campaign(volume: str) -> dict[str, dict[str, float]]:
    """Median memory-seconds, core-seconds and CPU per million lines of each variant of
    ``volume`` in the committed campaign report."""
    text = (EVIDENCE / "mr-vs-spark-campaign.md").read_text(encoding="utf-8")
    section = text.split(f"## Volume `{volume}`")[1].split("### Synthèse")[1].split("###")[0]
    found = {}
    for line in section.splitlines():
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if cells[0] in ("mapreduce", "mapreduce-tuned", "spark"):
            found[cells[0]] = {
                "memory_s": float(cells[6]),
                "core_s": float(cells[7]),
                "cpu_per_m": float(cells[9]),
            }
    return found


def test_finding_the_mapreduce_summary_keeps_figures_the_campaign_replaced():
    """docs/mapreduce-vs-spark.md, "En bref", next to the campaign's durations, gave the 3-run
    series' "9 % de mémoire·s en moins, Spark 9 % de cœurs·s en moins" and "2,1 à 2,5 fois plus
    de CPU par ligne". The summary and conclusion 3 now give the campaign's figures, which
    ``mr_compare.engine_comparison`` computes with the same formula."""
    full = campaign("full")
    memory_saved = 100 * (1 - full["mapreduce"]["memory_s"] / full["spark"]["memory_s"])
    cores_saved = 100 * (1 - full["spark"]["core_s"] / full["mapreduce"]["core_s"])
    python_low, python_high, combiner = 2.1, 2.7, 0.24
    jvm, spark = full["mapreduce"]["cpu_per_m"], full["spark"]["cpu_per_m"]
    ratio = ((jvm + python_low + combiner) / spark, (jvm + python_high + combiner) / spark)
    found = (round(memory_saved), round(cores_saved), round(ratio[0], 1), round(ratio[1], 1))
    assert found == (16, 5, 1.7, 2.0)
    text = (EVIDENCE / "mr-vs-spark-campaign.md").read_text(encoding="utf-8")
    medians = synthesis(text)["full"]
    tool = engine_comparison(medians["mapreduce"], medians["spark"])
    assert (round(-tool.memory_change_pct), round(-tool.core_change_pct)) == found[:2]
    assert tuple(round(r, 1) for r in tool.cpu_ratio) == found[2:]
    doc = " ".join((DOCS / "mapreduce-vs-spark.md").read_text(encoding="utf-8").split())
    summary, conclusion = doc.split("## Le calcul")[0], doc.split("## Conclusion")[1]
    assert "MapReduce réserve 16 % de mémoire·s en moins, Spark 5 % de cœurs·s en moins" in summary
    assert "environ 1,7 à 2,0 fois plus de CPU par ligne" in summary
    assert "environ 1,7 à 2,0 fois sur 10 M de lignes" in conclusion
    assert "9 %" not in summary


def test_control_the_campaign_medians_are_those_of_the_document():
    full = campaign("full")
    assert statistics.median([22.9, 21.6, 21.2, 21.4, 23.1]) == 21.6
    assert (full["mapreduce"]["memory_s"], full["spark"]["memory_s"]) == (58800, 69741)
