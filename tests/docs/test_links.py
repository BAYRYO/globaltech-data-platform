"""Every relative link of the tracked Markdown files points at a file that exists, and every
``#anchor`` at a heading of its target, with the slugs GitHub gives headings.

External links (``https:``, ``mailto:``...) are not followed. Links inside fenced code blocks and
inline code are not links. The files are those ``git ls-files`` lists; without Git (the test
container mounts the checkout without it) the walk of the checkout stands in, ignored folders left
out.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from urllib.parse import unquote

import pytest

ROOT = Path(__file__).resolve().parents[2]
UNTRACKED = {".git", ".venv", "venv", "build", "dist", ".pytest_cache", ".ruff_cache"}
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
FENCE = re.compile(r"^\s*(```|~~~)")
SCHEME = re.compile(r"[a-z][a-z0-9+.-]*:", re.IGNORECASE)


def markdown_files() -> list[Path]:
    try:
        listed = subprocess.run(
            ["git", "ls-files", "-z", "--", "*.md"], cwd=ROOT, capture_output=True, check=True
        ).stdout.decode()
    except (OSError, subprocess.CalledProcessError):
        return sorted(
            path
            for path in ROOT.rglob("*.md")
            if not UNTRACKED & set(path.relative_to(ROOT).parts)
            and not path.relative_to(ROOT).as_posix().startswith("data/generated/")
        )
    return sorted(ROOT / name for name in listed.split("\0") if name)


def slug(heading: str) -> str:
    text = re.sub(r"`([^`]*)`", r"\1", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = "".join(c for c in text.strip().lower() if c.isalnum() or c in " -_")
    return text.replace(" ", "-")


def prose(path: Path):
    """(line number, line) of each line outside fenced code blocks."""
    fenced = False
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if FENCE.match(line):
            fenced = not fenced
        elif not fenced:
            yield number, line


def anchors(path: Path) -> set[str]:
    seen: dict[str, int] = {}
    found = set()
    for _, line in prose(path):
        if match := HEADING.match(line):
            base = slug(match.group(2))
            count = seen.get(base, 0)
            seen[base] = count + 1
            found.add(base if count == 0 else f"{base}-{count}")
    return found


def broken_links(path: Path, root: Path) -> list[str]:
    name = path.relative_to(root).as_posix()
    found = []
    for number, line in prose(path):
        for match in LINK.finditer(re.sub(r"`[^`]*`", "", line)):
            target = match.group(1)
            if SCHEME.match(target):
                continue
            file_part, _, anchor = (unquote(part) for part in target.partition("#"))
            dest = (path.parent / file_part).resolve() if file_part else path
            if not dest.exists():
                found.append(f"{name}:{number}: no file {target}")
            elif anchor and dest.suffix == ".md" and anchor not in anchors(dest):
                found.append(f"{name}:{number}: no heading {target}")
    return found


@pytest.mark.parametrize(
    ("heading", "expected"),
    [
        ("12. Limites et pistes d'amélioration", "12-limites-et-pistes-damélioration"),
        (
            "Déséquilibre de clé : deux corrections et la diffusion",
            "déséquilibre-de-clé--deux-corrections-et-la-diffusion",
        ),
        ("Jointure de `fact_sales`", "jointure-de-fact_sales"),
        ("Constat : `_record_hash` en bronze", "constat--_record_hash-en-bronze"),
        ("Voir [le guide](platform.md)", "voir-le-guide"),
    ],
)
def test_a_heading_gets_the_slug_github_gives_it(heading: str, expected: str) -> None:
    assert slug(heading) == expected


def test_a_repeated_heading_gets_a_numbered_anchor(tmp_path: Path) -> None:
    page = tmp_path / "page.md"
    page.write_text("# Titre\n\n## Résultats\n\n## Résultats\n\n## Résultats\n", encoding="utf-8")

    assert anchors(page) == {"titre", "résultats", "résultats-1", "résultats-2"}


def test_a_missing_file_or_heading_is_reported_and_nothing_else(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "target.md").write_text("# Cible\n\n## Résultats\n", encoding="utf-8")
    page = tmp_path / "page.md"
    page.write_text(
        "\n".join(
            [
                "# Page",
                "[ok](docs/target.md#résultats) [ici](#page) [web](https://example.org/x.md)",
                "[fichier absent](docs/absent.md) [section absente](docs/target.md#autre)",
                "`[code](absent.md)`",
                "```",
                "[bloc](absent.md)",
                "```",
                "[encodé](docs/target.md#r%C3%A9sultats) [dossier](docs/)",
            ]
        ),
        encoding="utf-8",
    )

    assert broken_links(page, tmp_path) == [
        "page.md:3: no file docs/absent.md",
        "page.md:3: no heading docs/target.md#autre",
    ]


def test_every_relative_link_of_the_tracked_markdown_files_resolves() -> None:
    files = markdown_files()
    assert ROOT / "docs" / "rapport-technique.md" in files

    assert [problem for path in files for problem in broken_links(path, ROOT)] == []
