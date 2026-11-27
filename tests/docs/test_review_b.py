"""Reviewer B of PR #21: cases the dictionary test and the overview diagram get wrong.

Each test states the behaviour the PR claims and fails at 4a9554a.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import test_data_dictionary as dd

ROOT = Path(__file__).resolve().parents[2]


def _parse(tmp_path: Path, monkeypatch, page: str) -> dict[str, dd.Shape]:
    (tmp_path / "page.md").write_text(page, encoding="utf-8")
    monkeypatch.setattr(dd, "DICTIONARY", tmp_path)
    return dd._sections()


FACT_SALES = {"gold/fact_sales": dd.Shape({"quantity": "int"})}


def test_a_column_documented_with_a_wrong_type_fails_even_when_listed_twice(tmp_path, monkeypatch):
    doc = _parse(
        tmp_path,
        monkeypatch,
        "## `gold/fact_sales`\n\n"
        "| Colonne | Type | Règle | Exemple |\n| --- | --- | --- | --- |\n"
        "| `quantity` | string | Texte libre. | `douze` |\n"
        "| `quantity` | int | Quantité commandée. | `12` |\n",
    )
    with pytest.raises(AssertionError):
        dd.test_the_dictionary_gives_each_column_the_type_of_the_code(FACT_SALES, doc)


def test_a_column_documented_in_a_table_under_a_heading_without_a_path_is_checked(
    tmp_path, monkeypatch
):
    doc = _parse(
        tmp_path,
        monkeypatch,
        "## `gold/fact_sales`\n\n"
        "| Colonne | Type | Règle | Exemple |\n| --- | --- | --- | --- |\n"
        "| `quantity` | int | Quantité commandée. | `12` |\n\n"
        "### Colonnes dérivées\n\n"
        "| Colonne | Type | Règle | Exemple |\n| --- | --- | --- | --- |\n"
        "| `margin_eur` | decimal(20,2) | Marge de la ligne. | `512.30` |\n",
    )
    failures = 0
    for check in (
        dd.test_every_dictionary_section_names_a_table_or_file_of_the_code,
        dd.test_the_dictionary_lists_exactly_the_columns_of_the_code,
    ):
        try:
            check(FACT_SALES, doc)
        except AssertionError:
            failures += 1
    assert failures, "margin_eur is documented for gold/fact_sales and no check sees it"


def _overview_edges() -> set[tuple[str, str]]:
    text = (ROOT / "docs" / "architecture.md").read_text(encoding="utf-8")
    block = text.split("```mermaid", 1)[1].split("```", 1)[0]
    edge = re.compile(r"^\s*(\w+)\s*(?:==>|-->|-\.->|---)\s*(?:\|\"[^\"]*\"\|)?\s*(\w+)\s*$")
    return {m.groups() for line in block.splitlines() if (m := edge.match(line))}


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
