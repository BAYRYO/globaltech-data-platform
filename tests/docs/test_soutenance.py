"""The defence kit of docs/soutenance holds together: the deck's speaker notes are the script's
text, every source a slide cites exists, the timed rehearsal fits in 15 minutes, the deck loads
nothing from the network, the recorded demonstration shows no salt, the video shows what was
recorded, and the demonstration refuses a root that clean could not safely delete."""

from __future__ import annotations

import importlib.util
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SOUTENANCE = ROOT / "docs" / "soutenance"
CAST = SOUTENANCE / "demo" / "soutenance-demo.cast"
PATH = re.compile(r"(?:docs|scripts|src)/[\w./-]*\w")
# A network load: an element that fetches its src or href, a CSS url() or an @import, towards
# http(s):// or a protocol-relative //, whatever the quoting.
NETWORK = re.compile(
    r"""<(?:script|link|img|iframe|source|video|audio|embed|object)\b[^>]*\b(?:src|href|data)\s*=\s*["']?(?:https?:)?//"""
    r"""|url\(\s*["']?(?:https?:)?//|@import""",
    re.I,
)


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


def test_the_notes_of_each_slide_sit_in_its_own_section():
    html = (SOUTENANCE / "index.html").read_text(encoding="utf-8")
    sections = html.split('<section class="slide"')[1:]
    for number, section in enumerate(sections, start=1):
        assert f"<!-- notes {number} -->" in section, f"slide {number}"
        assert f"<!-- /notes {number} -->" in section, f"slide {number}"


def test_every_cited_source_exists(script):
    for slide in script:
        for path in PATH.findall(slide.sources):
            assert (ROOT / path).exists(), f"slide {slide.number}: {path}"


def test_the_deck_loads_nothing_from_the_network():
    html = (SOUTENANCE / "index.html").read_text(encoding="utf-8")
    assert not NETWORK.search(html)


@pytest.mark.parametrize(
    "load",
    [
        '<script src="https://cdn.example/x.js">',
        "<script src='https://cdn.example/x.js'>",
        "<img src=//cdn.example/x.png>",
        '<link rel="stylesheet" href="//cdn.example/x.css">',
        "<style>body { background: url('https://cdn.example/x.png') }</style>",
        "<style>@import 'x.css';</style>",
    ],
)
def test_the_network_check_sees_every_form_of_load(load):
    assert NETWORK.search(load)


def test_the_timed_rehearsal_fits_in_15_minutes(deck, script):
    report = (SOUTENANCE / "repetition.md").read_text(encoding="utf-8")
    total = re.search(r"\*\*Total\*\*.*\*\*(.+?)\*\* \((\d+) s\)", report)
    assert total, "no total in repetition.md: run python scripts/demo/deck.py rehearse"
    assert int(total.group(2)) <= 15 * 60
    rows = re.findall(r"^\| (\d+) \| (.+?) \| (.+?) \| (.+?) \| .+? \| (.+?) \|$", report, re.M)
    expected = [(str(slide.number), slide.title, slide.role, slide.target) for slide in script]
    assert [row[:4] for row in rows] == expected, "run python scripts/demo/deck.py rehearse"
    assert rows[-1][4] == total.group(1) == deck.mmss(int(total.group(2)))


@pytest.mark.parametrize(
    ("seconds", "text"),
    [(59.4, "59 s"), (59.6, "1 min 00 s"), (119.5, "2 min 00 s"), (863.0, "14 min 23 s")],
)
def test_durations_round_once(deck, seconds, text):
    assert deck.mmss(seconds) == text


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


def test_the_video_shortens_only_waits_that_save_a_second(tmp_path):
    cast = load("cast")
    frames = cast.frames([(0.0, "a\n"), (20.5, "b\n"), (45.5, "c\n")], max_idle=20.0)
    assert [round(f.skipped, 2) for f in frames] == [0.0, 5.0, 0.0]
    ass = tmp_path / "screen.ass"
    cast.write_ass(ass, frames, "T {x}", max_idle=20.0)
    text = ass.read_text(encoding="utf-8")
    notices = re.findall(r"attente de \d+ s raccourcie à 20 s", text)
    assert notices == ["attente de 25 s raccourcie à 20 s"]
    assert "Les attentes d'au moins 21 s sont raccourcies à 20 s" in text
    assert r"T \{x\}" in text


def test_a_wait_before_the_first_output_is_announced():
    cast = load("cast")
    frames = cast.frames([(30.0, "a\n")], max_idle=2.0)
    assert frames[0].lines == [""]
    assert frames[0].skipped == 28.0


def test_the_video_clocks_tick_every_second(tmp_path):
    cast = load("cast")
    frames = cast.frames([(0.0, "a\n"), (4.0, "b\n")], max_idle=20.0)
    ass = tmp_path / "screen.ass"
    cast.write_ass(ass, frames, "T", max_idle=20.0)
    text = ass.read_text(encoding="utf-8")
    clocks = re.findall(r"durée réelle (\d\d:\d\d)   vidéo (\d\d:\d\d)", text)
    assert clocks[:5] == [(f"00:0{s}", f"00:0{s}") for s in range(5)]
    assert clocks[-1] == ("00:04", "00:06")


def test_the_video_keeps_indentation_and_escapes_overrides():
    cast = load("cast")
    assert cast.ass_text("  rows {1}") == r"\h\hrows \{1\}"


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


BAD_ROOTS = [
    "/",
    "//",
    "/datalake",
    "/datalake/x",
    "/./datalake",
    "/user/../datalake",
    "/user",
    "/user/etl",
    "/user/etl/.",
    "/user/etl/..",
    "/tmp/x",
    "relative/x",
    "/user/etl/a b",
]


@pytest.fixture
def sandbox(tmp_path):
    """A copy of the demonstration script whose cluster client only records its arguments."""
    shutil.copytree(ROOT / "scripts" / "demo", tmp_path / "scripts" / "demo")
    client = tmp_path / "scripts" / "cluster" / "client.sh"
    client.parent.mkdir(parents=True)
    stub = '#!/usr/bin/env bash\necho "$*" >> "$(dirname "$0")/calls.log"\n'
    client.write_text(stub, encoding="utf-8")
    client.chmod(0o755)
    return tmp_path


def run_clean(sandbox, root):
    script = sandbox / "scripts" / "demo" / "soutenance.sh"
    env = {**os.environ, "GTDP_DEMO_ROOT": root}
    return subprocess.run(
        ["bash", str(script), "clean"], env=env, capture_output=True, text=True, check=False
    )


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
@pytest.mark.parametrize("root", BAD_ROOTS)
def test_the_demonstration_refuses_a_root_clean_could_not_safely_delete(sandbox, root):
    result = run_clean(sandbox, root)
    assert result.returncode == 2, result.stderr
    assert not (sandbox / "scripts" / "cluster" / "calls.log").exists()


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
def test_the_demonstration_deletes_only_its_own_root(sandbox):
    result = run_clean(sandbox, "/user/etl/soutenance/")
    assert result.returncode == 0, result.stderr
    calls = (sandbox / "scripts" / "cluster" / "calls.log").read_text(encoding="utf-8")
    assert calls == "hdfs dfs -rm -r -f -skipTrash /user/etl/soutenance\n"
