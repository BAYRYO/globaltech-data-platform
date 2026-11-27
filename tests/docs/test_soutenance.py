"""The defence kit of docs/soutenance holds together: the deck's speaker notes are the script's
text, every source a slide cites exists, the timed rehearsal fits in 15 minutes, the deck loads
nothing from the network, and the recorded demonstration shows no salt."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SOUTENANCE = ROOT / "docs" / "soutenance"
CAST = SOUTENANCE / "demo" / "soutenance-demo.cast"
PATH = re.compile(r"(?:docs|scripts|src)/[\w./-]*\w")


def load(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / "demo" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def deck():
    return load("deck")


@pytest.fixture(scope="module")
def script(deck):
    return deck.slides((SOUTENANCE / "script.md").read_text(encoding="utf-8"))


def test_every_slide_has_a_role_a_target_sources_and_a_text(script):
    assert [slide.number for slide in script] == list(range(1, len(script) + 1))
    for slide in script:
        assert slide.role and slide.target and slide.sources, slide.number
        assert slide.spoken().strip(), slide.number


def test_the_deck_has_one_section_per_slide_of_the_script(script):
    html = (SOUTENANCE / "index.html").read_text(encoding="utf-8")
    assert html.count('<section class="slide"') == len(script)


def test_the_speaker_notes_are_the_script(deck, script):
    html = (SOUTENANCE / "index.html").read_text(encoding="utf-8")
    _, differing = deck.sync_notes(html, script)
    assert differing == [], "run python scripts/demo/deck.py notes"


def test_every_cited_source_exists(script):
    for slide in script:
        for path in PATH.findall(slide.sources):
            assert (ROOT / path).exists(), f"slide {slide.number}: {path}"


def test_the_deck_loads_nothing_from_the_network():
    html = (SOUTENANCE / "index.html").read_text(encoding="utf-8")
    assert not re.search(r"<(?:script|link|img)\b[^>]*\b(?:src|href)=\"https?:", html)
    assert "@import" not in html


def test_the_timed_rehearsal_fits_in_15_minutes():
    report = (SOUTENANCE / "repetition.md").read_text(encoding="utf-8")
    total = re.search(r"\*\*Total\*\*.*\((\d+) s\)", report)
    assert total, "no total in repetition.md: run python scripts/demo/deck.py rehearse"
    assert int(total.group(1)) <= 15 * 60


def test_the_recorded_demonstration_shows_no_salt():
    text = CAST.read_text(encoding="utf-8")
    assert "GTDP_PSEUDO_SALT" not in text
    assert "gtdp-local-dev-salt" not in text


def test_the_video_shortens_long_waits_and_keeps_the_real_time():
    cast = load("cast")
    frames = cast.frames([(0.0, "a\n"), (10.0, "b\n"), (11.0, "c\n")], max_idle=2.0)
    assert [(f.start, f.real) for f in frames] == [(0.0, 0.0), (2.0, 10.0), (3.0, 11.0)]
    assert frames[0].skipped == 8.0
    assert frames[1].skipped == 0.0
    assert frames[-1].lines[-3:] == ["b", "c", ""]


@pytest.mark.parametrize(
    "block",
    [
        "<!-- rehearsal -->\n<!-- /rehearsal -->",
        "<!-- rehearsal -->\n| old |\n| rows |\n<!-- /rehearsal -->",
    ],
)
def test_the_rehearsal_table_replaces_an_empty_or_filled_block(deck, block):
    report = f"# R\n\n{block}\n"
    assert deck.fill_rehearsal(report, ["| a |", "| b |"]) == (
        "# R\n\n<!-- rehearsal -->\n| a |\n| b |\n<!-- /rehearsal -->\n"
    )


def test_the_rehearsal_refuses_a_report_without_its_markers(deck):
    assert deck.fill_rehearsal("# R\n", ["| a |"]) is None
