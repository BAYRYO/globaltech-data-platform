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
