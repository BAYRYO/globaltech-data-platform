"""docs/performance.md against the committed evidence of docs/perf-evidence: every p-value the
document gives equals what the tools compute from the committed runs, and every committed report
is what the tools write from them."""

import json
import re
from functools import cache
from pathlib import Path

import pytest

from gtdp.perf import bench, storage
from gtdp.perf.bench import compare, load_runs
from gtdp.perf.mr_compare import mann_whitney

DOCS = Path(__file__).resolve().parents[2] / "docs"
EVIDENCE = DOCS / "perf-evidence"
DOC = (DOCS / "performance.md").read_text(encoding="utf-8")
SERIES = [p.parent for p in sorted(EVIDENCE.rglob("runs.json"))]


@cache
def tool_p(experiment: str, reference: str, variant: str, key: str = "p", first_run: int = 1):
    """The p that ``bench.compare`` gives for one pair of a committed series."""
    _, runs = load_runs(EVIDENCE / experiment / "runs.json")
    return compare(runs, [(reference, variant)], first_run)[0][key]


def storage_p(times: str) -> float:
    result = json.loads((EVIDENCE / "storage" / "result.json").read_text(encoding="utf-8"))
    return mann_whitney(result[times]["rep3"]["runs"], result[times]["ec"]["runs"])[2]


def lever(label: str) -> str:
    return rf"^\| {re.escape(label)} \|.* \| (\d,\d{{3}}) \|$"


def skew_row(variant: str, median: str) -> str:
    return rf"^\| `{variant}` \| {median} \| [^|]+ \| (\d,\d{{3}}) \|"


def partitions_row(label: str) -> str:
    cell = r"[^|]+"
    return (
        rf"^\| {re.escape(label)} \| {cell} \| {cell} \| (\d,\d{{3}}) \| {cell} \| {cell} "
        rf"\| {cell} \| (\d,\d{{3}}) \| {cell} \| (\d,\d{{3}}) \|$"
    )


def sweep(name: str):
    return [("partitions", "sp-16", name, key) for key in ("p", "scan_p", "key_p")]


F, S4, S8, S1, C = "formats", "skew-4slots", "skew-8slots", "skew-first-txid-salt", "cache"
CHECKS = [
    # Summary table.
    (r"zstd contre snappy : [^|]*\(p = (\d,\d{3})\)", [(F, "parquet_part", "parquet_part_zstd")]),
    (r"pas de différence détectable à 6 \(p = (\d,\d{3})\)", [(C, "no-cache-6", "cache-6")]),
    (r"200 : \+9 % \(p = (\d,\d{3})\)", [("partitions", "sp-16", "sp-200")]),
    (r"pas de différence de lecture détectable \(p = (\d,\d{3})\)", ["read_s"]),
    # KAN-44.
    (lever("Format : CSV vers Parquet"), [(F, "csv", "parquet")]),
    (lever("Partitionnement : Parquet vers Parquet partitionné"), [(F, "parquet", "parquet_part")]),
    (
        lever("Partitionnement, listage par le driver"),
        [(F, "parquet", "parquet_part+driver-listing")],
    ),
    (lever("Partitionnement, table déjà listée"), [(F, "parquet", "parquet_part+listed")]),
    (lever("Compression : snappy vers zstd"), [(F, "parquet_part", "parquet_part_zstd")]),
    (lever("Ensemble : CSV vers Parquet partitionné zstd"), [(F, "csv", "parquet_part_zstd")]),
    (r"\(\+1,4 %, p = (\d,\d{3})\)", [(F, "parquet", "parquet_part+driver-listing")]),
    (r"\(\u22125,6 %, p = (\d,\d{3})\)", [(F, "parquet_part", "parquet_part_zstd")]),
    # KAN-45.
    (skew_row("salted-16", "3,17"), [(S4, "baseline", "salted-16")]),
    (skew_row("aqe-skew-join", "2,63"), [(S4, "baseline", "aqe-skew-join")]),
    (skew_row("broadcast", "1,37"), [(S4, "baseline", "broadcast")]),
    (skew_row("salted-16", "2,81"), [(S8, "baseline", "salted-16")]),
    (skew_row("aqe-skew-join", "2,16"), [(S8, "baseline", "aqe-skew-join")]),
    (skew_row("broadcast", "1,07"), [(S8, "baseline", "broadcast")]),
    (r"\(\+25,9 %, p = (\d,\d{3})\)", [(S1, "baseline", "salted-16")]),
    (r"\(\u221250,4 %, p = (\d,\d{3})\)", [(S1, "baseline", "broadcast")]),
    (r"\(\+0,0 %, p = (\d,\d{3})\)", [(S4, "baseline", "aqe-skew-join")]),
    (r"AQE gagne 0,16 s \(\u22126,0 %,\sp = (\d,\d{3})\)", [(S1, "baseline", "aqe-skew-join")]),
    (r"AQE gagne 0,23 s \(\u22129,9 %, p = (\d,\d{3})\)", [(S8, "baseline", "aqe-skew-join")]),
    (
        r"\(p = (\d,\d{3}) à 4 et 8 tâches,\sp = (\d,\d{3}) dans la première série\)",
        [(S4, "baseline", "salted-16"), (S1, "baseline", "salted-16")],
    ),
    (
        r"\(p = (\d,\d{3}) à 4 et 8 tâches,\sp = (\d,\d{3}) dans la première série\)",
        [(S8, "baseline", "salted-16"), (S1, "baseline", "salted-16")],
    ),
    (r"à 4 contre 4, p = (\d,\d{3})\.", [(S8, "baseline", "salted-16", "p", 2)]),
    # KAN-46.
    (
        r"^\| Jointure diffusée \(celle de gold\) \| 3 \|(?:[^|]+\|){3} (\d,\d{3}) \|",
        [(C, "no-cache-3", "cache-3")],
    ),
    (r"^\| Jointure diffusée \| 6 \|(?:[^|]+\|){3} (\d,\d{3}) \|", [(C, "no-cache-6", "cache-6")]),
    (
        r"^\| Jointure triée [^|]+\| 3 \|(?:[^|]+\|){3} (\d,\d{3}) \|",
        [(C, "no-cache-3-sort-merge", "cache-3-sort-merge")],
    ),
    (r"\(\+0,4 %, p = (\d,\d{3})\)", [(C, "no-cache-6", "cache-6")]),
    (partitions_row("4"), sweep("sp-4")),
    (partitions_row("8"), sweep("sp-8")),
    (partitions_row("64"), sweep("sp-64")),
    (partitions_row("200"), sweep("sp-200")),
    (partitions_row("200, regroupement AQE"), sweep("sp-200-aqe-coalesce")),
    (r"\(6,29 à 6,85 s, p = (\d,\d{3})\)", [("partitions", "sp-16", "sp-200")]),
    (r"\(p = (\d,\d{3}) au mieux, pour 200\)", [("partitions", "sp-16", "sp-200", "scan_p")]),
    (
        r"à 4 \(p = (\d,\d{3})\) et à 200 \(p = (\d,\d{3})\)",
        [("partitions", "sp-16", "sp-4", "key_p"), ("partitions", "sp-16", "sp-200", "key_p")],
    ),
    (
        r"pas à 8 ni à 64\s\(p = (\d,\d{3})\)",
        [("partitions", "sp-16", "sp-8", "key_p"), ("partitions", "sp-16", "sp-64", "key_p")],
    ),
    (
        r"16 partitions \(p = (\d,\d{3})\), comme à 4",
        [("partitions", "sp-16", "sp-200-aqe-coalesce", "key_p")],
    ),
    (r"\(\+2,9 %, p = (\d,\d{3})\)", [("partitions", "sp-16", "sp-200-aqe-coalesce")]),
    # KAN-47.
    (
        r"^\| p bilatéral \| \| (\d,\d{3}) \| (\d,\d{3}) \| (\d,\d{3}) \|$",
        ["read_s", "small_read_s", "copy_s"],
    ),
    # Series not reported.
    (r"cache \+55,0 % \(p = (\d,\d{3})\)", [("superseded/cache-0411", "no-cache", "cache")]),
    (
        r"cache \+58,8 % à 3 indicateurs \(p = (\d,\d{3})\)",
        [("superseded/cache-0412", "no-cache-3", "cache-3")],
    ),
    (
        r"\+6,7 % à 6 \(p = (\d,\d{3}), non retenu\)",
        [("superseded/cache-0412", "no-cache-6", "cache-6")],
    ),
]


def expected(spec) -> float:
    if isinstance(spec, str):
        return storage_p(spec)
    return tool_p(*spec)


def fr(p: float) -> str:
    return f"{p:.3f}".replace(".", ",")


@pytest.mark.parametrize(("pattern", "specs"), CHECKS, ids=[c[0][:60] for c in CHECKS])
def test_each_p_value_of_the_document_is_the_one_the_tools_compute(pattern, specs):
    [match] = list(re.finditer(pattern, DOC, re.MULTILINE))
    assert [match.group(i + 1) for i in range(len(specs))] == [fr(expected(s)) for s in specs]


def documented_p_values() -> set[int]:
    """Where the document gives a p: after "p = " in the text, in a table column whose header
    starts with "p", and in a table row whose first cell starts with "p"."""
    found = {m.start(1) for m in re.finditer(r"p = (\d,\d{3})", DOC)}
    offset, header = 0, None
    for line in DOC.splitlines(keepends=True):
        if line.startswith("|"):
            cells = line.strip().strip("|").split("|")
            if header is None:
                header = [cell.strip() for cell in cells]
            elif not set(line) <= set("|-: \n"):
                position = offset + line.index("|") + 1
                for i, cell in enumerate(cells):
                    column = header[i] if i < len(header) else ""
                    value = re.search(r"\d,\d{3}", cell)
                    p_cell = (
                        column == "p"
                        or column.startswith("p ")
                        or cells[0].strip().startswith("p ")
                    )
                    if value and p_cell:
                        found.add(position + value.start())
                    position += len(cell) + 1
        else:
            header = None
        offset += len(line)
    return found


def test_every_p_value_of_the_document_is_checked():
    checked = {
        match.start(i + 1)
        for pattern, specs in CHECKS
        for match in re.finditer(pattern, DOC, re.MULTILINE)
        for i in range(len(specs))
    }
    unchecked = sorted(documented_p_values() - checked)
    assert unchecked == [], [DOC[max(0, i - 60) : i + 6] for i in unchecked]


@pytest.mark.parametrize("folder", SERIES, ids=[str(p.relative_to(EVIDENCE)) for p in SERIES])
def test_each_committed_report_is_what_the_tool_writes_from_its_runs(folder, tmp_path):
    (tmp_path / "runs.json").write_bytes((folder / "runs.json").read_bytes())
    assert bench.main(["report", "--out", str(tmp_path)]) == 0
    assert (tmp_path / "report.md").read_text() == (folder / "report.md").read_text()


def test_the_storage_report_is_what_the_tool_writes_from_its_result(tmp_path):
    (tmp_path / "result.json").write_bytes((EVIDENCE / "storage" / "result.json").read_bytes())
    assert storage.main(["--report", str(tmp_path)]) == 0
    expected_report = (EVIDENCE / "storage" / "report.md").read_text()
    assert (tmp_path / "report.md").read_text() == expected_report


def test_the_evidence_stays_under_200_kb():
    assert sum(p.stat().st_size for p in EVIDENCE.rglob("*") if p.is_file()) < 200_000
