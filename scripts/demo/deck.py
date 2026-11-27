"""Keep the defence deck's speaker notes in step with its script, and time the script.

    python scripts/demo/deck.py notes [--check]
    python scripts/demo/deck.py rehearse --demo-cast FILE [--voice V] [--rate R] [--out DIR]

`notes` copies the role, target duration, text and sources of each slide of
docs/soutenance/script.md into the speaker notes of docs/soutenance/index.html, between the
`<!-- notes N -->` and `<!-- /notes N -->` markers; with --check it only says which slides differ
and exits with 1.

`rehearse` (Windows) synthesises the text of each slide with System.Speech
(scripts/demo/tts.ps1) to OUT/<N>.wav, measures each file, counts the demonstration slide at the
duration of the recorded live run (the last event of --demo-cast), and writes the table between
the `<!-- rehearsal -->` markers of docs/soutenance/repetition.md. It exits with 1 when the total
exceeds 15 minutes or the demonstration's narration is longer than the demonstration.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import subprocess
import sys
import tempfile
import wave
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "docs/soutenance/script.md"
DECK = ROOT / "docs/soutenance/index.html"
REHEARSAL = ROOT / "docs/soutenance/repetition.md"
TTS = ROOT / "scripts/demo/tts.ps1"
DEMO_TITLE = "Démonstration"
LIMIT_S = 15 * 60
REHEARSAL_BLOCK = re.compile(r"(<!-- rehearsal -->)\n(?:.*?\n)?(<!-- /rehearsal -->)", re.S)
HEADING = re.compile(r"^### (\d+)\. (.+)$")
FIELD = re.compile(r"^- (Rôle|Durée cible|Sources) : (.+)$")
BOLD = re.compile(r"\*\*(.+?)\*\*")


@dataclass
class Slide:
    number: int
    title: str
    role: str = ""
    target: str = ""
    sources: str = ""
    paragraphs: tuple[str, ...] = ()

    def spoken(self) -> str:
        """The text as said aloud: role labels dropped, digit groups joined for the voice."""
        text = " ".join(BOLD.sub("", p).strip() for p in self.paragraphs)
        return re.sub(r"(?<=\d) (?=\d{3}\b)", "", text)


def slides(markdown: str) -> list[Slide]:
    section = markdown.split("\n## Diapositives\n", 1)[1].split("\n## ", 1)[0]
    found: list[Slide] = []
    quote: list[str] = []
    for line in section.splitlines() + ["### 0. end"]:
        if match := HEADING.match(line):
            if found:
                found[-1].paragraphs = tuple(" ".join(quote).split(" \n "))
            quote = []
            found.append(Slide(int(match.group(1)), match.group(2)))
        elif match := FIELD.match(line):
            key = {"Rôle": "role", "Durée cible": "target", "Sources": "sources"}[match.group(1)]
            setattr(found[-1], key, match.group(2))
        elif line.startswith(">"):
            quote.append(line[1:].strip() or "\n")
    return found[:-1]


def notes_html(slide: Slide) -> str:
    def inline(text: str) -> str:
        return BOLD.sub(r"<strong>\1</strong>", html.escape(text, quote=False))

    body = "".join(f"\n<p>{inline(p)}</p>" for p in slide.paragraphs)
    return (
        f'<aside class="notes">\n<p class="who">{html.escape(slide.role)} · {html.escape(slide.target)}</p>'
        f'{body}\n<p class="src">Sources : {html.escape(slide.sources, quote=False)}</p>\n</aside>'
    )


def sync_notes(deck: str, script: list[Slide]) -> tuple[str, list[int]]:
    differing = []
    for slide in script:
        block = re.compile(rf"(<!-- notes {slide.number} -->\n).*?(\n<!-- /notes {slide.number} -->)", re.S)
        if not block.search(deck):
            raise SystemExit(f"error: {DECK.name} has no notes markers for slide {slide.number}")
        updated = block.sub(lambda m: m.group(1) + notes_html(slide) + m.group(2), deck)
        if updated != deck:
            differing.append(slide.number)
        deck = updated
    return deck, differing


def notes(check: bool) -> int:
    deck = DECK.read_text(encoding="utf-8")
    updated, differing = sync_notes(deck, slides(SCRIPT.read_text(encoding="utf-8")))
    if check:
        if differing:
            print(f"speaker notes differ from {SCRIPT.name} on slides {differing}: run deck.py notes")
        return 1 if differing else 0
    DECK.write_text(updated, encoding="utf-8", newline="\n")
    print(f"{DECK.name}: notes of slides {differing or 'none'} rewritten")
    return 0


def wav_seconds(path: Path) -> float:
    with wave.open(str(path)) as audio:
        return audio.getnframes() / audio.getframerate()


def mmss(seconds: float) -> str:
    whole = round(seconds)
    return f"{whole // 60} min {whole % 60:02d} s" if whole >= 60 else f"{whole} s"


def fill_rehearsal(report: str, table: list[str]) -> str | None:
    """Put the table between the rehearsal markers, empty or not; None without the markers."""
    updated, found = REHEARSAL_BLOCK.subn(
        lambda m: "\n".join([m.group(1), *table, m.group(2)]), report, count=1
    )
    return updated if found else None


def rehearse(demo_cast: Path, voice: str, rate: int, out: Path | None) -> int:
    script = slides(SCRIPT.read_text(encoding="utf-8"))
    with demo_cast.open(encoding="utf-8") as cast:
        demo_s = [json.loads(line) for line in cast][-1][0]
    workdir = Path(out or tempfile.mkdtemp(prefix="gtdp-rehearsal-"))
    workdir.mkdir(parents=True, exist_ok=True)
    for slide in script:
        (workdir / f"{slide.number:02d}.txt").write_text(slide.spoken(), encoding="utf-8")
    subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(TTS),
         "-Dir", str(workdir), "-Voice", voice, "-Rate", str(rate)],
        check=True,
    )
    rows, roles, total, failures = [], {}, 0.0, []
    for slide in script:
        speech = wav_seconds(workdir / f"{slide.number:02d}.wav")
        counted = speech
        if slide.title == DEMO_TITLE:
            counted = demo_s
            if speech > demo_s:
                failures.append(f"the narration of the demonstration lasts {speech:.0f} s, the run {demo_s:.0f} s")
            measured = f"démonstration {demo_s:.0f} s (texte dit en {speech:.0f} s pendant la démonstration)"
        else:
            measured = f"{speech:.0f} s"
            roles[slide.role] = roles.get(slide.role, 0.0) + speech
        total += counted
        rows.append(f"| {slide.number} | {slide.title} | {slide.role} | {slide.target} | {measured} | {mmss(total)} |")
    if total > LIMIT_S:
        failures.append(f"total {total:.0f} s > {LIMIT_S} s")
    table = [
        "| # | Diapositive | Rôle | Durée cible | Durée mesurée | Cumul |",
        "| ---: | --- | --- | ---: | --- | ---: |",
        *rows,
        f"| | **Total** | | | | **{mmss(total)}** ({total:.0f} s) |",
        "",
        "| Rôle | Temps de parole hors démonstration |",
        "| --- | ---: |",
        *(f"| {role} | {mmss(seconds)} |" for role, seconds in roles.items()),
    ]
    updated = fill_rehearsal(REHEARSAL.read_text(encoding="utf-8"), table)
    if updated is None:
        print(f"error: no <!-- rehearsal --> block in {REHEARSAL}", file=sys.stderr)
        return 1
    REHEARSAL.write_text(updated, encoding="utf-8", newline="\n")
    print("\n".join(table))
    print(f"voice {voice}, rate {rate}, files in {workdir}")
    for failure in failures:
        print(f"error: {failure}", file=sys.stderr)
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("notes").add_argument("--check", action="store_true")
    reh = commands.add_parser("rehearse")
    reh.add_argument("--demo-cast", type=Path, required=True)
    reh.add_argument("--voice", default="Microsoft Hortense Desktop")
    reh.add_argument("--rate", type=int, default=0)
    reh.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    if args.command == "notes":
        return notes(args.check)
    return rehearse(args.demo_cast, args.voice, args.rate, args.out)


if __name__ == "__main__":
    sys.exit(main())
