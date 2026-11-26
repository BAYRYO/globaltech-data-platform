"""Independent checker of a generated profile, the oracle later layers test against.

It imports nothing from the generator: it re-reads every file, recounts each anomaly with its own
rules (regular expressions, reference sets of the identifiers seen in the reference files, duplicate
detection) and compares the counts, row numbers and checksums with the manifests.
"""

import csv
import hashlib
import json
import re
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

ORDERS_HEADER = (
    "transaction_id,event_time,customer_id,product_id,quantity,unit_price_eur,channel,status"
)
ORDERS_HEADERS = (ORDERS_HEADER, f"{ORDERS_HEADER},discount_pct")
CUSTOMERS_HEADER = "customer_id,full_name,email,phone,region,segment,signup_date,updated_at"
PRODUCTS_HEADER = "product_id,product_name,category,unit_price_eur,updated_at"
CHANNELS = frozenset(("web", "store", "partner"))
STATUSES = frozenset(("COMPLETED", "CANCELLED", "RETURNED"))
TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
QUANTITY = re.compile(r"[1-9]\d*")
AMOUNT = re.compile(r"\d+(\.\d{1,2})?")
DISCOUNT = re.compile(r"0\.\d{3}")
EMAIL = re.compile(r"[^@\s]+@[^@\s]+\.[a-z]{2,}")
REGION = re.compile(r"[A-Z]{2}-[A-Z]{2,4}")
COMBINED_LOG = re.compile(
    r"\d{1,3}(?:\.\d{1,3}){3} \S+ \S+ \[\d{2}/[A-Z][a-z]{2}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4}\] "
    r'"[A-Z]+ \S+ HTTP/1\.[01]" (\d{3}) \d+ "[^"]*" "[^"]*"'
)
SENSORS_PER_FILE = 60
SENSOR_FILE_SECONDS = 5
ALERTS: dict[str, Callable[[float], bool]] = {
    "temperature": lambda value: value > 85,
    "vibration": lambda value: value > 7.1,
    "pressure": lambda value: value < 4 or value > 8,
}
LANDING_ORDER = ("customers", "products", "orders", "weblogs")
MAX_PROBLEMS_PER_FILE = 5


@dataclass
class Counted:
    rows: int = 0
    anomalies: Counter = field(default_factory=Counter)
    problems: list[str] = field(default_factory=list)

    def add(self, line_number: int, codes: list[str]) -> None:
        self.anomalies.update(codes)
        if len(codes) > 1:
            self.problem(f"line {line_number} carries several anomalies: {codes}")

    def problem(self, message: str) -> None:
        if len(self.problems) < MAX_PROBLEMS_PER_FILE:
            self.problems.append(message)


@dataclass
class References:
    """Identifiers seen so far in the reference files, whatever the row's quality."""

    customers: set[str] = field(default_factory=set)
    products: set[str] = field(default_factory=set)
    orders_files: list[Path] = field(default_factory=list)


@dataclass
class Report:
    checked: list[tuple[str, Counted]] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)


def _lines(path: Path) -> Iterator[str]:
    with path.open(encoding="utf-8", newline="") as handle:
        for raw in handle:
            yield raw[:-1] if raw.endswith("\n") else raw


def _fields(line: str) -> list[str]:
    return next(csv.reader([line])) if '"' in line else line.split(",")


def _is_timestamp(text: str) -> bool:
    if not TIMESTAMP.fullmatch(text):
        return False
    try:
        datetime.fromisoformat(text)
    except ValueError:
        return False
    return True


def _is_date(text: str) -> bool:
    if not DATE.fullmatch(text):
        return False
    try:
        date.fromisoformat(text)
    except ValueError:
        return False
    return True


def _is_amount(text: str) -> bool:
    return bool(AMOUNT.fullmatch(text)) and float(text) > 0


def _rows(path: Path, expected_headers: tuple[str, ...], counted: Counted) -> Iterator[str]:
    lines = _lines(path)
    header = next(lines, "")
    if header not in expected_headers:
        counted.problem(f"unexpected header {header!r}")
    for line in lines:
        counted.rows += 1
        if line.endswith("\r"):
            counted.problem(f"line {counted.rows + 1} ends with CRLF")
        yield line


def _skip_header(lines: Iterator[str]) -> Iterator[str]:
    next(lines, None)
    return lines


def _earlier_orders(paths: list[Path], wanted: set[str]) -> dict[str, list[str]]:
    found = {}
    for path in paths:
        for line in _skip_header(_lines(path)):
            transaction_id = line.split(",", 1)[0]
            if transaction_id in wanted:
                found[transaction_id] = _fields(line)[:8]
    return found


def _order_value_codes(row: list[str], refs: References, future: str) -> list[str]:
    _, stamp, customer, product, quantity, price, channel, status = row[:8]
    codes = []
    if not customer:
        codes.append("NULL_CUSTOMER")
    elif customer not in refs.customers:
        codes.append("UNKNOWN_CUSTOMER")
    if product not in refs.products:
        codes.append("UNKNOWN_PRODUCT")
    if not QUANTITY.fullmatch(quantity):
        codes.append("BAD_QUANTITY")
    if not _is_amount(price):
        codes.append("BAD_PRICE")
    if not _is_timestamp(stamp):
        codes.append("BAD_TIMESTAMP")
    elif stamp > future:
        codes.append("FUTURE_TIMESTAMP")
    if channel not in CHANNELS or status not in STATUSES:
        codes.append("BAD_ENUM")
    return codes


def count_orders(path: Path, ingestion_date: date, refs: References) -> Counted:
    counted = Counted()
    future = f"{ingestion_date + timedelta(days=1)}T00:00:00"
    resent: dict[str, list[str]] = {}
    if refs.orders_files:
        wanted = {line.split(",", 1)[0] for line in _skip_header(_lines(path))}
        resent = _earlier_orders(refs.orders_files, wanted)
    with path.open(encoding="utf-8") as handle:
        width = len(handle.readline().rstrip("\n").split(","))
    seen: set[int] = set()
    for number, line in enumerate(_rows(path, ORDERS_HEADERS, counted), start=2):
        codes = []
        key = hash(line)
        if key in seen:
            codes.append("DUPLICATE_EXACT")
        seen.add(key)
        row = _fields(line)
        if len(row) != width:
            counted.add(number, [*codes, "MALFORMED_ROW"])
            continue
        discount = row[8] if width == 9 else ""
        if resent.get(row[0]) == row[:8]:
            codes.append("DUPLICATE_CROSS_DAY")
            if discount:
                counted.problem(f"line {number}: resent row carries a discount")
        if discount and not (DISCOUNT.fullmatch(discount) and float(discount) <= 0.3):
            counted.problem(f"line {number}: discount_pct {discount!r} out of contract")
        counted.add(number, codes + _order_value_codes(row, refs, future))
    refs.orders_files.append(path)
    return counted


def count_customers(path: Path, ingestion_date: date, refs: References) -> Counted:
    counted = Counted()
    seen: set[str] = set()
    emails: dict[tuple[str, str], str] = {}
    for number, line in enumerate(_rows(path, (CUSTOMERS_HEADER,), counted), start=2):
        row = _fields(line)
        if len(row) != 8:
            counted.problem(f"line {number}: {len(row)} fields")
            continue
        customer_id, _, email, _, region, _, signup, updated_at = row
        email = email.strip().lower()
        codes = []
        if line in seen:
            codes.append("DUPLICATE_EXACT")
        elif (customer_id, updated_at) in emails:
            codes.append("DUPLICATE_KEY")
            if emails[customer_id, updated_at] != email:
                counted.problem(f"line {number}: {customer_id} restated with another email")
        seen.add(line)
        emails.setdefault((customer_id, updated_at), email)
        if not email:
            codes.append("MISSING_EMAIL")
        elif not EMAIL.fullmatch(email):
            codes.append("BAD_EMAIL")
        if not region:
            codes.append("MISSING_REGION")
        elif not REGION.fullmatch(region):
            counted.problem(f"line {number}: region {region!r} out of contract")
        if not _is_date(signup):
            codes.append("BAD_DATE")
        if not _is_timestamp(updated_at):
            counted.problem(f"line {number}: updated_at {updated_at!r} out of contract")
        counted.add(number, codes)
        refs.customers.add(customer_id)
    return counted


def count_products(path: Path, ingestion_date: date, refs: References) -> Counted:
    counted = Counted()
    seen: set[str] = set()
    first_line_of_key: dict[tuple[str, str], str] = {}
    for number, line in enumerate(_rows(path, (PRODUCTS_HEADER,), counted), start=2):
        row = _fields(line)
        if len(row) != 5:
            counted.problem(f"line {number}: {len(row)} fields")
            continue
        product_id, _, category, price, updated_at = row
        codes = []
        if line in seen:
            codes.append("DUPLICATE_EXACT")
        elif first_line_of_key.setdefault((product_id, updated_at), line) != line:
            codes.append("CONFLICTING_VERSION")
        seen.add(line)
        if not _is_amount(price):
            codes.append("BAD_PRICE")
        if not category:
            codes.append("NULL_CATEGORY")
        if not _is_timestamp(updated_at):
            counted.problem(f"line {number}: updated_at {updated_at!r} out of contract")
        counted.add(number, codes)
        refs.products.add(product_id)
    return counted


def count_weblogs(path: Path, ingestion_date: date, refs: References) -> Counted:
    counted = Counted()
    seen: set[int] = set()
    for number, line in enumerate(_lines(path), start=1):
        counted.rows += 1
        if hash(line) in seen:
            counted.problem(f"line {number}: repeats an earlier line, which silver would drop")
        seen.add(hash(line))
        match = COMBINED_LOG.fullmatch(line)
        if match is None:
            counted.add(number, ["MALFORMED_LINE"])
        elif match[1] == "404":
            counted.add(number, ["HTTP_404"])
        elif match[1] == "500":
            counted.add(number, ["HTTP_500"])
        elif match[1] != "200":
            counted.problem(f"line {number}: unexpected status {match[1]}")
    return counted


COUNTERS = {
    "customers": count_customers,
    "products": count_products,
    "orders": count_orders,
    "weblogs": count_weblogs,
}


def _sensor_codes(
    event: dict[str, Any], start: datetime, earlier_ids: set[str], counted: Counted, number: int
) -> tuple[list[str], bool]:
    """Anomaly codes of a parsed event, and whether it is the sensor's reading of this file."""
    codes = []
    duplicate = event["event_id"] in earlier_ids
    if duplicate:
        codes.append("DUPLICATE_EVENT")
    value = event["value"]
    if value is None:
        codes.append("NULL_VALUE")
    elif ALERTS[event["metric"]](value):
        codes.append("SPIKE")
    event_time = datetime.strptime(event["event_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
    lag = (start - event_time).total_seconds()
    on_time = -SENSOR_FILE_SECONDS < lag <= 0
    if 10 <= lag <= 90:
        codes.append("LATE_EVENT")
    elif 300 <= lag <= 900:
        codes.append("VERY_LATE_EVENT")
    elif not (duplicate or on_time):
        counted.problem(f"line {number}: event_time {lag:.1f} s before the file interval")
    return codes, on_time and not duplicate


def count_sensor_day(directory: Path, names: list[str]) -> dict[str, Counted]:
    earlier_ids: set[str] = set()
    results = {}
    for name in sorted(names):
        counted = Counted()
        start = datetime.strptime(name, "sensors_%Y%m%dT%H%M%S.jsonl")
        file_ids: set[str] = set()
        readings: list[str] = []
        malformed = 0
        for number, line in enumerate(_lines(directory / name), start=1):
            counted.rows += 1
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                event = None
            if not isinstance(event, dict):
                malformed += 1
                counted.add(number, ["MALFORMED_JSON"])
                continue
            codes, reading = _sensor_codes(event, start, earlier_ids, counted, number)
            counted.add(number, codes)
            file_ids.add(event["event_id"])
            if reading:
                readings.append(event["sensor_id"])
        if len(set(readings)) != len(readings) or len(readings) + malformed != SENSORS_PER_FILE:
            counted.problem(f"{len(readings)} readings + {malformed} malformed, not one per sensor")
        earlier_ids |= file_ids
        results[name] = counted
    return results


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _compare(
    report: Report, label: str, entry: dict[str, Any], path: Path, counted: Counted
) -> None:
    report.checked.append((label, counted))
    report.problems += [f"{label}: {problem}" for problem in counted.problems]
    if counted.rows != entry["rows"]:
        report.problems.append(f"{label}: {counted.rows} rows, manifest says {entry['rows']}")
    expected = {code: n for code, n in entry["anomalies"].items() if n}
    if dict(counted.anomalies) != expected:
        report.problems.append(f"{label}: counted {dict(counted.anomalies)}, manifest {expected}")
    if _sha256(path) != entry["sha256"]:
        report.problems.append(f"{label}: sha256 differs from the manifest")


def _unlisted(directory: Path, listed: set[str]) -> list[str]:
    present = {
        path.relative_to(directory).as_posix() for path in directory.rglob("*") if path.is_file()
    }
    return sorted(present - listed - {"_MANIFEST.json"})


def verify(root: Path) -> Report:
    report = Report()
    refs = References()
    manifests = sorted(root.glob("landing/*/_MANIFEST.json"))
    if not manifests:
        report.problems.append(f"no landing manifest under {root}")
    for manifest_path in manifests:
        directory = manifest_path.parent
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        ingestion_date = date.fromisoformat(directory.name)
        if manifest["ingestion_date"] != directory.name:
            report.problems.append(f"{manifest_path}: ingestion_date {manifest['ingestion_date']}")
        entries = sorted(manifest["files"], key=lambda e: LANDING_ORDER.index(e["source"]))
        for entry in entries:
            path = directory / entry["path"]
            label = f"{directory.name}/{entry['path']}"
            if not path.is_file():
                report.problems.append(f"{label}: missing")
                continue
            counted = COUNTERS[entry["source"]](path, ingestion_date, refs)
            _compare(report, label, entry, path, counted)
        listed = {entry["path"] for entry in manifest["files"]}
        report.problems += [
            f"{directory.name}/{p}: not in the manifest" for p in _unlisted(directory, listed)
        ]
    sensor_manifests = sorted(root.glob("sensors/*/_MANIFEST.json"))
    if not sensor_manifests:
        report.problems.append(f"no sensor manifest under {root}")
    for manifest_path in sensor_manifests:
        directory = manifest_path.parent
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        entries = {entry["path"]: entry for entry in manifest["files"]}
        present = [name for name in entries if (directory / name).is_file()]
        report.problems += [
            f"sensors/{directory.name}/{n}: missing" for n in entries if n not in present
        ]
        for name, counted in count_sensor_day(directory, present).items():
            label = f"sensors/{directory.name}/{name}"
            _compare(report, label, entries[name], directory / name, counted)
        report.problems += [
            f"sensors/{directory.name}/{p}: not in the manifest"
            for p in _unlisted(directory, set(entries))
        ]
    return report
