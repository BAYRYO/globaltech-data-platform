"""The verifier fails on what a generator bug would write, even when the manifest is re-signed."""

import hashlib
import json
import shutil
from collections import Counter
from pathlib import Path

import pytest

from gtdp.datagen.verify import verify

J1, J2, J3 = "2026-01-01", "2026-01-02", "2026-01-03"


def landing_file(root: Path, day: str, source: str) -> Path:
    extension = "log" if source == "weblogs" else "csv"
    return root / "landing" / day / source / f"{source}_{day.replace('-', '')}.{extension}"


def lines_of(root: Path, day: str, source: str) -> list[str]:
    return landing_file(root, day, source).read_text(encoding="utf-8").splitlines()


def write_signed(path: Path, lines: list[str]) -> None:
    """Write the file and sign it in its manifest, as a buggy generator would."""
    data = ("\n".join(lines) + "\n").encode()
    path.write_bytes(data)
    directory = next(parent for parent in path.parents if (parent / "_MANIFEST.json").is_file())
    manifest = json.loads((directory / "_MANIFEST.json").read_text(encoding="utf-8"))
    entry = next(entry for entry in manifest["files"] if directory / entry["path"] == path)
    entry["sha256"] = hashlib.sha256(data).hexdigest()
    entry["rows"] = len(lines) - (path.suffix == ".csv")
    (directory / "_MANIFEST.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def rewrite(root: Path, day: str, source: str, lines: list[str]) -> None:
    write_signed(landing_file(root, day, source), lines)


def native_j2_orders(root: Path) -> tuple[list[str], list[int]]:
    """J2 orders and the indexes of its rows whose id J1 does not hold."""
    j1_ids = {line.split(",", 1)[0] for line in lines_of(root, J1, "orders")}
    lines = lines_of(root, J2, "orders")
    return lines, [i for i, line in enumerate(lines) if i and line.split(",", 1)[0] not in j1_ids]


def initial_version(lines: list[str], product_id: str) -> str:
    return next(
        line
        for line in lines
        if line.startswith(f"{product_id},") and line.endswith(",2024-12-31T00:00:00")
    )


def delete_j3_manifest(root: Path) -> None:
    (root / "landing" / J3 / "_MANIFEST.json").unlink()


def delete_j3_drop(root: Path) -> None:
    shutil.rmtree(root / "landing" / J3)


def share_an_id_within_j2(root: Path) -> None:
    lines, native = native_j2_orders(root)
    first, second = native[:2]
    lines[second] = lines[first].split(",", 1)[0] + "," + lines[second].split(",", 1)[1]
    rewrite(root, J2, "orders", lines)


def reuse_a_j1_id_with_other_content(root: Path) -> None:
    kit_row = lines_of(root, J1, "orders")[1]
    lines, native = native_j2_orders(root)
    lines[native[0]] = kit_row.split(",", 1)[0] + "," + lines[native[0]].split(",", 1)[1]
    rewrite(root, J2, "orders", lines)


def resend_a_malformed_j1_row(root: Path) -> None:
    j1 = lines_of(root, J1, "orders")
    malformed = next(line for line in j1[1:] if line.count(",") > 7)
    lines = lines_of(root, J2, "orders")
    resent = next(i for i, line in enumerate(lines) if i and line in set(j1))
    lines[resent] = ",".join(malformed.split(",")[:8])
    rewrite(root, J2, "orders", lines)


def empty_a_customer_id(root: Path) -> None:
    lines = lines_of(root, J2, "customers")
    lines[1] = "," + lines[1].split(",", 1)[1]
    rewrite(root, J2, "customers", lines)


def drop_the_j3_schema_change(root: Path) -> None:
    """Every row one field shorter, `discount_pct` gone: the malformed rows stay malformed."""
    lines = []
    for line in lines_of(root, J3, "orders"):
        fields = line.split(",")
        lines.append(",".join(fields[:-1]) if len(fields) in (8, 9) else line)
    rewrite(root, J3, "orders", lines)


def null_category_on_a_first_version(root: Path) -> None:
    lines = lines_of(root, J1, "products")
    null_category = next(i for i, line in enumerate(lines) if line.split(",")[2] == "")
    fields = lines[null_category].split(",")
    fields[2] = fields[1].split(" ")[0]
    lines[null_category] = ",".join(fields)
    first = lines.index(initial_version(lines, "P00042"))
    fields = lines[first].split(",")
    fields[2] = ""
    lines[first] = ",".join(fields)
    rewrite(root, J1, "products", lines)


def duplicate_a_first_version(root: Path) -> None:
    lines = lines_of(root, J1, "products")
    repeated = next(line for line, n in Counter(lines).items() if n > 1)
    copy = len(lines) - 1 - lines[::-1].index(repeated)
    lines[copy] = initial_version(lines, "P00042")
    rewrite(root, J1, "products", lines)


def is_json(line: str) -> bool:
    try:
        json.loads(line)
    except json.JSONDecodeError:
        return False
    return True


def single_quote_a_malformed_sensor_line(root: Path) -> None:
    path = next(
        p
        for p in sorted((root / "sensors" / J1).glob("*.jsonl"))
        if not all(is_json(line) for line in p.read_text(encoding="utf-8").splitlines())
    )
    lines = path.read_text(encoding="utf-8").splitlines()
    malformed = next(i for i, line in enumerate(lines) if not is_json(line))
    lines[malformed] = next(line for line in lines if is_json(line)).replace('"', "'")
    write_signed(path, lines)


MUTATIONS = {
    "m0-missing-manifest": (delete_j3_manifest, "landing/2026-01-03/_MANIFEST.json: missing"),
    "m1-missing-drop": (delete_j3_drop, "landing/2026-01-03/_MANIFEST.json: missing"),
    "m2-id-shared-in-drop": (share_an_id_within_j2, "repeated with other content"),
    "m3-cross-day-id-other-content": (
        reuse_a_j1_id_with_other_content,
        "T000000001 of an earlier drop, not a copy of its sane row",
    ),
    "m4-resend-of-rejected-row": (
        resend_a_malformed_j1_row,
        "of an earlier drop, not a copy of its sane row",
    ),
    "m5-empty-customer-id": (empty_a_customer_id, "line 2: empty customer_id"),
    "m6-no-schema-change": (
        drop_the_j3_schema_change,
        "orders_20260103.csv: unexpected header",
    ),
    "m7-null-category-on-first-version": (
        null_category_on_a_first_version,
        "NULL_CATEGORY on the first version of P00042",
    ),
    "m8-duplicate-of-first-version": (
        duplicate_a_first_version,
        "DUPLICATE_EXACT on the first version of P00042",
    ),
    "single-quoted-sensor-line": (
        single_quote_a_malformed_sensor_line,
        "single-quoted JSON, which Spark reads as valid",
    ),
}


@pytest.mark.parametrize(("mutate", "expected"), MUTATIONS.values(), ids=MUTATIONS.keys())
def test_verifier_rejects_a_generator_bug(ci_root, tmp_path, mutate, expected):
    root = tmp_path / "ci"
    shutil.copytree(ci_root, root)
    mutate(root)
    problems = verify(root).problems
    assert any(expected in problem for problem in problems), problems
