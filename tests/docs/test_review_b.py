"""Review B of PR #21 (KAN-54): cases the dictionary test and the overview diagram got wrong.

The reviewer's parser cases called the column and type checks; a repeated column and a table
under a heading without a path now have checks of their own, which these cases call.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import test_data_dictionary as dd

ROOT = Path(__file__).resolve().parents[2]
TABLE = "| Colonne | Type | Règle | Exemple |\n| --- | --- | --- | --- |\n"
QUANTITY = "| `quantity` | int | Quantité commandée. | `12` |\n"
FACT_SALES = {"gold/fact_sales": dd.Shape({"quantity": "int", "revenue_eur": "decimal(20,2)"})}


def _parse(tmp_path: Path, page: str) -> dd.Pages:
    (tmp_path / "page.md").write_text(page, encoding="utf-8")
    return dd._sections(tmp_path)


def test_a_column_documented_with_a_wrong_type_fails_even_when_listed_twice(tmp_path):
    pages = _parse(
        tmp_path,
        f"## `gold/fact_sales`\n\n{TABLE}| `quantity` | string | Texte libre. | `douze` |\n"
        + QUANTITY,
    )
    with pytest.raises(AssertionError, match="quantity"):
        dd.test_the_dictionary_documents_each_column_once(FACT_SALES, pages.shapes)


def test_a_column_documented_in_an_earlier_section_naming_the_same_path_fails(tmp_path):
    pages = _parse(
        tmp_path,
        f"## Aperçu de `gold/fact_sales`\n\n{TABLE}"
        "| `revenue_eur` | double | Montant en flottant. | `1.0` |\n\n"
        f"## `gold/fact_sales`\n\n{TABLE}{QUANTITY}"
        "| `revenue_eur` | decimal(20,2) | Chiffre d'affaires. | `1.00` |\n",
    )
    with pytest.raises(AssertionError, match="revenue_eur"):
        dd.test_the_dictionary_documents_each_column_once(FACT_SALES, pages.shapes)


def test_a_column_documented_in_a_table_under_a_heading_without_a_path_is_checked(tmp_path):
    pages = _parse(
        tmp_path,
        f"## `gold/fact_sales`\n\n{TABLE}{QUANTITY}\n### Colonnes dérivées\n\n{TABLE}"
        "| `margin_eur` | decimal(20,2) | Marge de la ligne. | `512.30` |\n",
    )
    with pytest.raises(AssertionError, match="page.md: Colonnes dérivées"):
        dd.test_every_column_table_is_under_a_heading_naming_its_path(pages)


EDGE = re.compile(r"^\s*(\w+)\s*(?:==>|-->|-\.->|---)\s*(?:\|\"([^\"]*)\"\|)?\s*(\w+)\s*$")
GITHUB_CANVAS = {"light": "#ffffff", "dark": "#0d1117"}


def _overview() -> list[str]:
    """The lines of the first mermaid block of ``docs/architecture.md``."""
    text = (ROOT / "docs" / "architecture.md").read_text(encoding="utf-8")
    return text.split("```mermaid", 1)[1].split("```", 1)[0].splitlines()


def _edges() -> list[tuple[str, str, str]]:
    """(source, label, target) of each edge, in the order ``linkStyle`` numbers them."""
    return [(m[1], m[2] or "", m[3]) for line in _overview() if (m := EDGE.match(line))]


def _overview_edges() -> set[tuple[str, str]]:
    return {(source, target) for source, _, target in _edges()}


def _frames() -> dict[str, list[str]]:
    """Each node and frame, with the frames around it, innermost first."""
    stack: list[str] = []
    around: dict[str, list[str]] = {}
    for line in _overview():
        if frame := re.match(r"\s*subgraph (\w+)", line):
            around[frame[1]] = stack[::-1]
            stack.append(frame[1])
        elif line.strip() == "end":
            stack.pop()
        elif (node := re.match(r"\s*(\w+)[\[\(]", line)) and not EDGE.match(line):
            around[node[1]] = stack[::-1]
    return around


def _node(label: str) -> str:
    """The id of the one node whose label holds ``label``."""
    (node,) = [
        m[1]
        for line in _overview()
        if (m := re.match(r'\s*(\w+)[\[\(/]+"([^"]*)"', line)) and label in m[2]
    ]
    return node


def _contrast(colour: str, background: str) -> float:
    """WCAG 2 contrast ratio of two ``#rrggbb`` colours."""

    def luminance(rgb: str) -> float:
        channels = [int(rgb[i : i + 2], 16) / 255 for i in (1, 3, 5)]
        linear = [c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4 for c in channels]
        return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]

    light, dark = sorted((luminance(colour), luminance(background)), reverse=True)
    return (light + 0.05) / (dark + 0.05)


def test_the_overview_shows_gold_reading_the_silver_rejects():
    """``jobs/gold.py`` reads ``rejects_path(silver, source)`` for ``kpi_data_quality``."""
    source = (ROOT / "src" / "gtdp" / "jobs" / "gold.py").read_text(encoding="utf-8")
    assert "rejects_path(silver, source)" in source
    assert ("REJ", "JG") in _overview_edges()


def test_the_overview_shows_silver_writing_the_salt_fingerprint_to_ops():
    """``jobs/silver.py`` calls ``ensure_fingerprint`` on ``paths.ops`` before writing."""
    source = (ROOT / "src" / "gtdp" / "jobs" / "silver.py").read_text(encoding="utf-8")
    assert "ensure_fingerprint(" in source
    assert ("JS", "OPS") in _overview_edges()


def test_the_overview_draws_the_simulator_and_the_generator_in_the_client():
    """``scripts/streaming/demo.sh`` runs ``replay-sensors`` in the client container: the
    simulator calls ``hdfs dfs -put`` and ``-mv``, which the client image holds."""
    demo = (ROOT / "scripts" / "streaming" / "demo.sh").read_text(encoding="utf-8")
    assert "client python3 -m gtdp.datagen replay-sensors" in demo
    around = _frames()
    client = around[_node("land.sh")][:1]

    assert client
    assert around[_node("Simulateur")][:1] == around[_node("Générateur")][:1] == client


def test_no_audit_or_event_log_edge_starts_from_the_hadoop_streaming_job():
    """``scripts/mapreduce/run_mr.sh`` runs ``hadoop jar`` without ``run.py``: no audit row, no
    Spark event log."""
    streaming = _node("Hadoop Streaming")
    starts = [source for source, label, _ in _edges() if label in {"audit", "event logs"}]

    assert len(starts) == 2
    assert [s for s in starts if s == streaming or s in _frames()[streaming]] == []


def test_the_overview_draws_the_aggregated_container_logs():
    """``yarn-site.xml`` aggregates the logs of the containers in ``/app-logs``."""
    yarn = (ROOT / "docker" / "conf" / "hadoop" / "yarn-site.xml").read_text(encoding="utf-8")
    assert "<value>/app-logs</value>" in yarn
    logs = _node("/app-logs")

    assert {source for source, _, target in _edges() if target == logs} == {
        _node("NodeManager 1"),
        _node("NodeManager 2"),
    }


def test_every_edge_has_one_line_style():
    styled = [
        int(index)
        for line in _overview()
        if line.strip().startswith("linkStyle ")
        for index in line.split()[1].split(",")
    ]

    assert sorted(styled) == list(range(len(_edges())))


def test_every_line_colour_has_a_3_to_1_contrast_on_the_light_and_dark_github_canvas():
    colours = {
        colour
        for line in _overview()
        for colour in re.findall(r"linkStyle .*stroke:(#\w{6})", line)
    }
    low = {
        colour: {
            mode: round(_contrast(colour, canvas), 2) for mode, canvas in GITHUB_CANVAS.items()
        }
        for colour in colours
        if min(_contrast(colour, canvas) for canvas in GITHUB_CANVAS.values()) < 3
    }

    assert colours
    assert not low, f"line colours under 3:1: {low}"
