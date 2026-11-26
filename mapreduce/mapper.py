"""Hadoop Streaming mapper of Lot 6: revenue per country and invalid order lines.

Usage: python3 mapper.py CUSTOMERS_FILE [CUSTOMERS_FILE ...] < order lines

For each valid COMPLETED order line it writes `country<TAB>amount<TAB>1`. The amount is the
invoiced amount in units of 1e-5 EUR (price cents times thousandths of discount), so sums stay
exact integers. Every line is counted once, in the counter group Quality under the first rule of
INVALID_RULES it fails, or in the group Lines (HEADER, COMPLETED, OTHER_STATUS). The counters go
to Hadoop at the end of the task, one `reporter:counter:<group>,<name>,<n>` line each on stderr.

The rules are the ones of src/gtdp/perf/revenue_by_country.py; tests/mapreduce runs both engines
on the same files and compares their outputs.
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from collections.abc import Iterable, Iterator
from datetime import datetime

TIME, CUSTOMER, QUANTITY, PRICE, CHANNEL, STATUS, DISCOUNT = 1, 2, 4, 5, 6, 7, 8
ORDER_WIDTHS = (8, 9)
CUSTOMER_ID, REGION, UPDATED_AT = 0, 4, 7
CUSTOMER_WIDTH = 8
CHANNELS = frozenset(("web", "store", "partner"))
STATUSES = frozenset(("COMPLETED", "CANCELLED", "RETURNED"))
MAX_DISCOUNT_MILLI = 300
QUANTITY_FORMAT = re.compile(r"[1-9][0-9]{0,8}")
PRICE_FORMAT = re.compile(r"([0-9]{1,8})(?:\.([0-9]{1,2}))?")
TIMESTAMP_FORMAT = re.compile(r"[1-9][0-9]{3}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}")
DISCOUNT_FORMAT = re.compile(r"0\.([0-9]{3})")
HEADER = ("Lines", "HEADER")
COMPLETED = ("Lines", "COMPLETED")
OTHER_STATUS = ("Lines", "OTHER_STATUS")


def load_countries(lines: Iterable[str]) -> dict[str, str]:
    """customer_id -> country, from customer lines of any number of files.

    A customer's row is the one with the greatest (updated_at, region); its country is the region
    prefix before `-`, or UNKNOWN when that prefix is empty. Lines without 8 fields, the header
    and empty ids are ignored.
    """
    latest: dict[str, tuple[str, str]] = {}
    for line in lines:
        fields = line.rstrip("\n").split(",")
        if len(fields) != CUSTOMER_WIDTH or fields[CUSTOMER_ID] in ("", "customer_id"):
            continue
        version = (fields[UPDATED_AT], fields[REGION])
        latest[fields[CUSTOMER_ID]] = max(version, latest.get(fields[CUSTOMER_ID], version))
    return {cid: region.split("-", 1)[0] or "UNKNOWN" for cid, (_, region) in latest.items()}


def price_cents(text: str) -> int:
    """The price in cents; 0 when it is not a decimal with at most 2 decimals."""
    match = PRICE_FORMAT.fullmatch(text)
    if match is None:
        return 0
    return int(match[1]) * 100 + int((match[2] or "").ljust(2, "0"))


def discount_milli(fields: list[str]) -> int | None:
    """Thousandths of discount: 0 when the field is absent or empty, None when invalid."""
    text = fields[DISCOUNT] if len(fields) > DISCOUNT else ""
    if text == "":
        return 0
    match = DISCOUNT_FORMAT.fullmatch(text)
    if match is None or int(match[1]) > MAX_DISCOUNT_MILLI:
        return None
    return int(match[1])


def is_timestamp(text: str) -> bool:
    if TIMESTAMP_FORMAT.fullmatch(text) is None:
        return False
    try:
        datetime.fromisoformat(text)
    except ValueError:
        return False
    return True


INVALID_RULES = (
    ("MALFORMED_ROW", lambda fields, countries: len(fields) not in ORDER_WIDTHS),
    ("BAD_QUANTITY", lambda fields, countries: QUANTITY_FORMAT.fullmatch(fields[QUANTITY]) is None),
    ("BAD_PRICE", lambda fields, countries: price_cents(fields[PRICE]) == 0),
    ("BAD_TIMESTAMP", lambda fields, countries: not is_timestamp(fields[TIME])),
    ("NULL_CUSTOMER", lambda fields, countries: fields[CUSTOMER] == ""),
    ("UNKNOWN_CUSTOMER", lambda fields, countries: fields[CUSTOMER] not in countries),
    (
        "BAD_ENUM",
        lambda fields, countries: fields[CHANNEL] not in CHANNELS or fields[STATUS] not in STATUSES,
    ),
    ("BAD_DISCOUNT", lambda fields, countries: discount_milli(fields) is None),
)
"""Precedence order: a line is counted once, under the first rule it fails."""


def classify(fields: list[str], countries: dict[str, str]) -> tuple[str, str]:
    """The counter (group, name) of an order line split on commas."""
    if fields[0] == "transaction_id":
        return HEADER
    for reason, fails in INVALID_RULES:
        if fails(fields, countries):
            return ("Quality", reason)
    return COMPLETED if fields[STATUS] == "COMPLETED" else OTHER_STATUS


def amount(fields: list[str]) -> int:
    """Invoiced amount of a valid line, in 1e-5 EUR."""
    milli = discount_milli(fields) or 0
    return int(fields[QUANTITY]) * price_cents(fields[PRICE]) * (1000 - milli)


def _read_files(paths: Iterable[str]) -> Iterator[str]:
    for path in paths:
        with open(path, encoding="utf-8", errors="replace") as handle:
            yield from handle


def main(customer_files: list[str]) -> int:
    for stream in (sys.stdin, sys.stdout, sys.stderr):
        stream.reconfigure(encoding="utf-8", errors="replace")
    countries = load_countries(_read_files(customer_files))
    counters: Counter[tuple[str, str]] = Counter()
    for line in sys.stdin:
        fields = line.rstrip("\n").split(",")
        counter = classify(fields, countries)
        counters[counter] += 1
        if counter == COMPLETED:
            sys.stdout.write(f"{countries[fields[CUSTOMER]]}\t{amount(fields)}\t1\n")
    for (group, name), count in sorted(counters.items()):
        sys.stderr.write(f"reporter:counter:{group},{name},{count}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
