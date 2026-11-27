"""The rule codes, reject reasons and alert thresholds of the dictionary are those of the code.

Silver: the code table of each source lists the codes of its rule registry (``ENTITY.rules``) with
their action, plus ``MALFORMED_ROW`` for a CSV source, whose lines can be corrupt. A table without
an action column lists reject rules. Stream: the reject reasons in ``REJECT_RULES`` order, and the
thresholds of ``[streaming.thresholds]``. The status filters and the formulas of the indicators
are reviewed by hand.
"""

import re
from pathlib import Path

import pytest

from gtdp.config import load_config
from gtdp.schemas import Format
from gtdp.silver.pipelines import PIPELINES
from gtdp.silver.quality import MALFORMED_ROW, Action
from gtdp.streaming.sensors import REJECT_RULES

DICTIONARY = Path(__file__).resolve().parents[2] / "docs" / "data-dictionary"
ACTIONS = {"rejet": Action.REJECT, "drapeau": Action.FLAG}


def section(page: str, heading: str) -> str:
    """The text of ``page`` from the line ``heading`` to the next heading of the same level."""
    text = (DICTIONARY / page).read_text(encoding="utf-8")
    level = heading.split(" ")[0]
    start = text.index(f"\n{heading}\n")
    end = re.search(rf"\n{level} ", text[start + 1 :])
    return text[start : start + 1 + end.start()] if end else text[start:]


def codes(text: str) -> list[tuple[str, str]]:
    """(code, second cell) of each table row of ``text`` that starts with an upper-case code."""
    return re.findall(r"^\| `([A-Z][A-Z_0-9]+)` \| ([^|]*?) \|", text, flags=re.MULTILINE)


@pytest.mark.parametrize("source", list(PIPELINES))
def test_silver_lists_the_rule_codes_of_each_source_with_their_action(source):
    pipeline = PIPELINES[source]
    documented = [
        (code, ACTIONS.get(cell, Action.REJECT))
        for code, cell in codes(section("silver.md", f"## `silver/{pipeline.table}`"))
    ]
    in_code = [(rule.code, rule.action) for rule in pipeline.entity.rules]
    if pipeline.entity.source.format is Format.CSV:
        in_code.append((MALFORMED_ROW, Action.REJECT))

    assert sorted(documented) == sorted(in_code)


def test_the_stream_lists_its_reject_reasons_in_priority_order():
    documented = [code for code, _ in codes(section("gold-sensors.md", "## `streaming/rejects`"))]

    assert documented == [code for code, _ in REJECT_RULES]


def test_the_stream_lists_the_thresholds_of_the_configuration():
    rows = re.findall(
        r"^\| `(\w+)` \| ([\d.]*) ?\| ([\d.]*) ?\|$",
        section("gold-sensors.md", "### Seuils"),
        flags=re.MULTILINE,
    )
    documented = {metric: {"below": below, "above": above} for metric, below, above in rows}
    configured = {
        metric: {side: str(limits.get(side, "")) for side in ("below", "above")}
        for metric, limits in load_config("local").streaming.thresholds.items()
    }

    assert documented == configured
