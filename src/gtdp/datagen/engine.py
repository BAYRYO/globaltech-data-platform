"""Row-plan engine shared by every source.

A file is a plan of slots: slot 0 is always clean, then each anomaly type owns an exact number of
randomly placed slots. A clean slot takes the next row of the source's clean stream, an anomaly
slot calls that type's injector, so a row carries at most one anomaly and the manifest counts are
exact by construction.
"""

import hashlib
import json
import os
import random
from collections.abc import Callable, Iterable, Iterator
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

RECENT_ROWS = 1024
WRITE_BATCH = 8192


@dataclass(frozen=True)
class Anomaly:
    code: str
    rate: float
    inject: Callable[[Any], str]
    needs_history: bool = False
    """Applies only once an earlier drop exists (cross-day copies)."""


@dataclass(frozen=True)
class WrittenFile:
    source: str
    path: str
    rows: int
    sha256: str
    anomalies: dict[str, int]


def file_rng(seed: int, name: str) -> random.Random:
    return random.Random(f"{seed}/{name}")


def anomaly_counts(anomalies: Iterable[Anomaly], base: int, has_history: bool) -> dict[str, int]:
    """Exact row count per applicable type: the rate applied to `base`, at least one row."""
    return {
        anomaly.code: max(1, round(anomaly.rate * base))
        for anomaly in anomalies
        if has_history or not anomaly.needs_history
    }


class RowSource:
    """Clean-row stream of one file, plus the state its injectors draw from."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self._recent: list[str] = []
        self._clean_rows = 0

    def next_clean(self) -> str:
        raise NotImplementedError

    def remember(self, line: str) -> None:
        if len(self._recent) < RECENT_ROWS:
            self._recent.append(line)
        else:
            self._recent[self._clean_rows % RECENT_ROWS] = line
        self._clean_rows += 1

    def recent(self) -> str:
        """A clean row already written to this file, for in-file duplicates."""
        return self.rng.choice(self._recent)


def emit_rows(
    source: RowSource, anomalies: Iterable[Anomaly], counts: dict[str, int], n_rows: int
) -> Iterator[str]:
    injectors: list[Callable[[Any], str]] = []
    labels: list[int] = []
    for anomaly in anomalies:
        if anomaly.code in counts:
            injectors.append(anomaly.inject)
            labels += [len(injectors)] * counts[anomaly.code]
    plan = bytearray(n_rows)
    positions = source.rng.sample(range(1, n_rows), len(labels))
    for position, label in zip(positions, labels, strict=True):
        plan[position] = label
    for label in plan:
        if label:
            yield injectors[label - 1](source)
        else:
            line = source.next_clean()
            source.remember(line)
            yield line


def write_lines(path: Path, lines: Iterable[str], header: str | None = None) -> tuple[int, str]:
    """Write LF-terminated UTF-8 lines through a temporary file; return (data rows, sha256)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    digest = hashlib.sha256()
    rows = 0
    with tmp.open("wb") as out:
        batch = [header] if header is not None else []
        for line in lines:
            batch.append(line)
            rows += 1
            if len(batch) >= WRITE_BATCH:
                data = ("\n".join(batch) + "\n").encode()
                out.write(data)
                digest.update(data)
                batch.clear()
        if batch:
            data = ("\n".join(batch) + "\n").encode()
            out.write(data)
            digest.update(data)
    os.replace(tmp, path)
    return rows, digest.hexdigest()


def write_source_file(
    directory: Path,
    relative_path: str,
    source_name: str,
    source: RowSource,
    anomalies: Iterable[Anomaly],
    counts: dict[str, int],
    n_rows: int,
    header: str | None = None,
) -> WrittenFile:
    rows, sha256 = write_lines(
        directory / relative_path, emit_rows(source, anomalies, counts, n_rows), header
    )
    return WrittenFile(source_name, relative_path, rows, sha256, counts)


def write_manifest(directory: Path, header: dict[str, Any], files: list[WrittenFile]) -> None:
    manifest = {**header, "files": [asdict(written) for written in files]}
    text = json.dumps(manifest, indent=2, ensure_ascii=False)
    write_lines(directory / "_MANIFEST.json", [text])
