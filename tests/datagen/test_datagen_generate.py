import gzip
import hashlib
import json
import re
import shutil
from bisect import bisect_right
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

from gtdp.datagen.__main__ import main
from gtdp.datagen.verify import verify

J1, J2, J3 = "2026-01-01", "2026-01-02", "2026-01-03"
EXPECTED_CODES = {
    "orders": {
        "DUPLICATE_EXACT", "DUPLICATE_CROSS_DAY", "NULL_CUSTOMER", "UNKNOWN_CUSTOMER",
        "UNKNOWN_PRODUCT", "BAD_QUANTITY", "BAD_PRICE", "BAD_TIMESTAMP", "FUTURE_TIMESTAMP",
        "BAD_ENUM", "MALFORMED_ROW",
    },
    "customers": {
        "DUPLICATE_EXACT", "DUPLICATE_KEY", "MISSING_EMAIL", "BAD_EMAIL", "MISSING_REGION",
        "BAD_DATE",
    },
    "products": {"DUPLICATE_EXACT", "BAD_PRICE", "NULL_CATEGORY", "CONFLICTING_VERSION"},
    "weblogs": {"MALFORMED_LINE", "HTTP_404", "HTTP_500"},
}  # fmt: skip
SENSOR_CODES = {
    "LATE_EVENT", "VERY_LATE_EVENT", "DUPLICATE_EVENT", "MALFORMED_JSON", "NULL_VALUE", "SPIKE",
}  # fmt: skip


def tree_digest(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def data_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()[1:]


def orders_file(root: Path, day: str) -> Path:
    return root / "landing" / day / "orders" / f"orders_{day.replace('-', '')}.csv"


def manifest_anomalies(root: Path, day: str, source: str) -> dict[str, int]:
    manifest = json.loads((root / "landing" / day / "_MANIFEST.json").read_text())
    return next(f["anomalies"] for f in manifest["files"] if f["source"] == source)


def test_runs_with_the_same_seed_write_identical_bytes(ci_root, generate_ci, tmp_path):
    second = generate_ci(tmp_path)
    assert tree_digest(second) == tree_digest(ci_root)
    generate_ci(tmp_path)
    assert tree_digest(second) == tree_digest(ci_root)
    assert not [p for p in second.rglob("*.tmp")]


def test_another_seed_writes_different_data(ci_root, generate_ci, tmp_path):
    other = generate_ci(tmp_path, seed=7)
    assert tree_digest(other) != tree_digest(ci_root)


def test_verifier_recount_matches_every_manifest(ci_root):
    report = verify(ci_root)
    assert report.problems == []
    assert len(report.checked) == 12 + 48
    assert main(["verify", "--path", str(ci_root)]) == 0


def test_verifier_rejects_a_damaged_row_and_a_lost_row(ci_root, tmp_path):
    damaged = tmp_path / "ci"
    shutil.copytree(ci_root, damaged)
    path = orders_file(damaged, J1)
    lines = path.read_text(encoding="utf-8").splitlines()
    fields = lines[1].split(",")
    fields[2] = ""
    lines[1] = ",".join(fields)
    del lines[-1]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    problems = "\n".join(verify(damaged).problems)

    assert "rows, manifest says 20000" in problems
    assert "'NULL_CUSTOMER': 101" in problems
    assert "sha256 differs" in problems
    assert main(["verify", "--path", str(damaged)]) == 1


def test_every_anomaly_type_appears_in_each_landing_file_and_in_the_sensor_day(ci_root):
    report = verify(ci_root)
    sensor_codes = set()
    for label, counted in report.checked:
        if label.startswith("sensors/"):
            sensor_codes |= set(counted.anomalies)
            continue
        source = label.split("/")[1]
        expected = EXPECTED_CODES[source] - ({"DUPLICATE_CROSS_DAY"} if label[:10] == J1 else set())
        assert set(counted.anomalies) == expected, label
    assert sensor_codes == SENSOR_CODES


def test_j1_keeps_the_first_kit_transactions_verbatim_and_in_order(ci_root, kit_dir):
    with gzip.open(kit_dir / "transactions.csv.gz", "rt", encoding="utf-8", newline="") as kit:
        kit_lines = kit.read().splitlines()[1:10_001]
    j1 = data_lines(orders_file(ci_root, J1))
    position = {line: index for index, line in reversed(list(enumerate(j1)))}
    assert all(line in position for line in kit_lines)
    first_seen = [position[line] for line in kit_lines]
    assert first_seen == sorted(first_seen)
    synthetic_ids = {line[:10] for line in j1} - {line[:10] for line in kit_lines}
    assert min(synthetic_ids) == "T000200001"


def test_j3_appends_discount_pct_as_last_column(ci_root):
    j3 = orders_file(ci_root, J3).read_text(encoding="utf-8").splitlines()
    assert j3[0].split(",")[-1] == "discount_pct"
    assert orders_file(ci_root, J2).read_text().splitlines()[0].split(",")[-1] == "status"
    discounts = [line.split(",")[8] for line in j3[1:] if line.count(",") == 8]
    assert all(d == "" or 0 <= float(d) <= 0.30 for d in discounts)
    assert len({d for d in discounts if d}) > 5


def test_malformed_order_rows_never_have_8_or_9_fields(ci_root):
    for day in (J1, J2, J3):
        lines = orders_file(ci_root, day).read_text(encoding="utf-8").splitlines()
        width = lines[0].count(",") + 1
        widths = Counter(line.count(",") + 1 for line in lines[1:])
        malformed = {n: rows for n, rows in widths.items() if n != width}
        assert malformed, day
        assert not malformed.keys() & {8, 9}, (day, malformed)


def test_cross_day_duplicates_resend_rows_of_earlier_drops(ci_root):
    earlier: dict[str, list[str]] = {}
    for day in (J1, J2, J3):
        rows = [line.split(",") for line in data_lines(orders_file(ci_root, day))]
        resent = [row for row in rows if row[0] in earlier]
        assert len(resent) == manifest_anomalies(ci_root, day, "orders").get(
            "DUPLICATE_CROSS_DAY", 0
        )
        for row in resent:
            assert row[:8] == earlier[row[0]]
            assert row[8:] == ([""] if day == J3 else [])
        earlier.update((row[0], row[:8]) for row in rows if len(row) >= 8)


def test_daily_drops_carry_late_orders_from_earlier_days(ci_root):
    earlier = {line.split(",", 1)[0] for line in data_lines(orders_file(ci_root, J1))}
    for day, window in ((J2, ("2025-12-29", "2026-01-01")), (J3, ("2025-12-30", "2026-01-02"))):
        rows = [line.split(",") for line in data_lines(orders_file(ci_root, day))]
        native = [row for row in rows if len(row) > 1 and row[0] not in earlier]
        assert any(window[0] <= row[1] < window[1] for row in native)
        earlier |= {row[0] for row in rows}


def _catalog(root: Path) -> dict[str, list[tuple[str, set[float]]]]:
    """Valid versions per product; a key restated with another price keeps every candidate."""
    path = root / "landing" / J1 / "products" / "products_20260101.csv"
    versions: dict[str, dict[str, set[float]]] = {}
    for product_id, _, category, price, updated_at in (
        line.split(",") for line in data_lines(path)
    ):
        if category and re.fullmatch(r"\d+(\.\d+)?", price) and float(price) > 0:
            versions.setdefault(product_id, {}).setdefault(updated_at, set()).add(float(price))
    return {pid: sorted(stamps.items()) for pid, stamps in versions.items()}


def test_synthetic_orders_follow_the_catalog_price_and_the_hot_product(ci_root):
    catalog = _catalog(ci_root)
    rows = [line.split(",") for line in data_lines(orders_file(ci_root, J1))]
    synthetic = [
        row
        for row in rows
        if len(row) == 8
        and row[0] >= "T000200001"
        and row[3] in catalog
        and row[1].startswith("2025-")
        and re.fullmatch(r"\d+\.\d{2}", row[5])
    ]
    assert len(synthetic) > 9_000
    for row in synthetic:
        history = catalog[row[3]]
        _, prices = history[bisect_right([stamp for stamp, _ in history], row[1]) - 1]
        assert any(0.9 * p - 0.005 <= float(row[5]) <= p + 0.005 for p in prices), row
    hot_share = sum(row[3] == "P00042" for row in synthetic) / len(synthetic)
    assert 0.17 < hot_share < 0.23


def test_product_anomalies_never_touch_a_first_version(ci_root):
    first_versions: dict[str, list[str]] = {}
    for day in (J1, J2, J3):
        path = ci_root / "landing" / day / "products" / f"products_{day.replace('-', '')}.csv"
        for line in data_lines(path):
            product_id, _, _, _, updated_at = line.split(",")
            if updated_at == "2024-12-31T00:00:00" or product_id >= "P01001":
                first_versions.setdefault(product_id, []).append(line)
    assert {f"P{n:05d}" for n in range(1, 1001)} < first_versions.keys()
    repeated = {pid: lines for pid, lines in first_versions.items() if len(lines) > 1}
    assert repeated == {}
    for (line,) in first_versions.values():
        _, _, category, price, _ = line.split(",")
        assert category
        assert re.fullmatch(r"\d+(\.\d{1,2})?", price)


def test_customers_start_from_the_kit_base_and_new_ids_follow_it(ci_root):
    def ids(day: str) -> set[str]:
        path = ci_root / "landing" / day / "customers" / f"customers_{day.replace('-', '')}.csv"
        return {line.split(",", 1)[0] for line in data_lines(path)}

    kit_base = {f"C{n:06d}" for n in range(1, 5001)}
    assert ids(J1) == kit_base
    new_ids = (ids(J2) | ids(J3)) - kit_base
    assert min(new_ids) == "C005001"


def test_sensor_day_spikes_one_machine_per_site_for_two_minutes(ci_root):
    files = sorted((ci_root / "sensors" / J1).glob("sensors_*.jsonl"))
    assert len(files) == 48
    above: dict[tuple[str, str], list[int]] = {}
    very_late_files = []
    for index, path in enumerate(files):
        start = datetime(2026, 1, 1) + timedelta(seconds=5 * index)
        for line in path.read_text().splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            value, metric = event["value"], event["metric"]
            if value is not None and (
                (metric == "temperature" and value > 85)
                or (metric == "vibration" and value > 7.1)
                or (metric == "pressure" and not 4 <= value <= 8)
            ):
                above.setdefault((event["site"], event["sensor_id"]), []).append(index)
            event_time = datetime.strptime(event["event_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if (start - event_time).total_seconds() >= 300:
                very_late_files.append(index)
    assert sorted(site for site, _ in above) == [
        "DE-HAMBURG", "ES-BILBAO", "FR-LYON", "IT-TORINO", "UK-LEEDS",
    ]  # fmt: skip
    for indexes in above.values():
        assert indexes[0] >= 24
        assert len(indexes) >= 24
        assert indexes == list(range(indexes[0], indexes[0] + len(indexes)))
    assert very_late_files
    assert min(very_late_files) >= 36
