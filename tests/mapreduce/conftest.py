import json
import shlex
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pytest

from gtdp.datagen.__main__ import main as datagen

REPO = Path(__file__).resolve().parents[2]
MAPPER = REPO / "mapreduce" / "mapper.py"
REDUCER = REPO / "mapreduce" / "reducer.py"
DAYS = ("2026-01-01", "2026-01-02", "2026-01-03")

CUSTOMERS_J1 = """\
customer_id,full_name,email,phone,region,segment,signup_date,updated_at
C1,Ana Lopez,a@x.io,+1,FR-IDF,SMB,2024-01-01,2025-12-31T00:00:00
C2,Bo Li,b@x.io,+2,DE-BY,SMB,2024-01-01,2025-12-31T00:00:00
C3,Cy Ng,c@x.io,+3,,SMB,2024-01-01,2025-12-31T00:00:00
C4,Dé Wu,d@x.io,+4,ES-MD,SMB,2024-01-01,2025-12-31T00:00:00
C6,Ed Ox,e@x.io,+6,-XX,SMB,2024-01-01,2025-12-31T00:00:00
C9,too,few,fields
,No Id,n@x.io,+0,UK-LON,SMB,2024-01-01,2025-12-31T00:00:00
"""
CUSTOMERS_J2 = """\
customer_id,full_name,email,phone,region,segment,signup_date,updated_at
C4,Dé Wu,d@x.io,+4,IT-LAZ,MID,2024-01-01,2026-01-02T08:00:00
C2,Bo Li,b@x.io,+2,ES-MD,SMB,2024-01-01,2025-06-30T00:00:00
C5,Fa Wu,f@x.io,+5,FR-IDF,SMB,2024-01-01,2026-01-02T09:00:00
C5,Fa Wu, F@X.IO,+5,UK-LON,SMB,2024-01-01,2026-01-02T09:00:00
"""
ORDERS_8_COLUMNS = """\
transaction_id,event_time,customer_id,product_id,quantity,unit_price_eur,channel,status
T01,2025-03-01T10:00:00,C1,P1,2,10.50,web,COMPLETED
T02,2025-03-01T10:00:00,C1,P1,1,26.9,store,COMPLETED
T03,2025-03-01T10:00:00,C2,P1,3,0.01,partner,COMPLETED
T04,2025-03-01T10:00:00,C3,P1,1,5,web,COMPLETED
T05,2025-03-01T10:00:00,C4,P1,1,7.00,web,COMPLETED
T06,2025-03-01T10:00:00,C4,P1,12,12345678.99,web,COMPLETED
T07,2025-03-01T10:00:00,C6,P1,2,1.25,web,COMPLETED
T08,2025-03-01T10:00:00,C1,P1,1,99.99,web,CANCELLED
T09,2024-02-29T12:00:00,C1,P1,1,99.99,web,RETURNED
T10,2025-03-01T10:00:00,C1,P1
T11;2025-03-01T10:00:00;C1;P1;1;1.00;web;COMPLETED

T12,2025-03-01T10:00:00,,P1,0,0,fax,COMPLETED,x,y
T13,2025-03-01T10:00:00,C1,P1,0,1.00,web,COMPLETED
T14,2025-03-01T10:00:00,C1,P1,-3,1.00,web,COMPLETED
T15,2025-03-01T10:00:00,C1,P1,abc,1.00,web,COMPLETED
T16,2025-03-01T10:00:00,C1,P1,,1.00,web,COMPLETED
T17,2025-03-01T10:00:00,C1,P1,007,1.00,web,COMPLETED
T18,2025-03-01T10:00:00,C1,P1,1234567890,1.00,web,COMPLETED
T19,2025-03-01T10:00:00,C1,P1,5\N{LINE SEPARATOR},1.00,web,COMPLETED
T20,2025-03-01T10:00:00,C1,P1,1,-12.50,web,COMPLETED
T21,2025-03-01T10:00:00,C1,P1,1,0,web,COMPLETED
T22,2025-03-01T10:00:00,C1,P1,1,,web,COMPLETED
T23,2025-03-01T10:00:00,C1,P1,1,0.00,web,COMPLETED
T24,2025-03-01T10:00:00,C1,P1,1,1.234,web,COMPLETED
T25,2025-03-01T10:00:00,C1,P1,1,1e3,web,COMPLETED
T26,2025/03/04 10:00,C1,P1,1,1.00,web,COMPLETED
T27,N/A,C1,P1,1,1.00,web,COMPLETED
T28,,C1,P1,1,1.00,web,COMPLETED
T29,2025-02-30T10:00:00,C1,P1,1,1.00,web,COMPLETED
T30,0000-01-01T00:00:00,C1,P1,1,1.00,web,COMPLETED
T31,2025-01-01T24:00:00,C1,P1,1,1.00,web,COMPLETED
T32,2025-03-01 10:00:00,C1,P1,1,1.00,web,COMPLETED
T33,2025-03-01T10:00:00Z,C1,P1,1,1.00,web,COMPLETED
T34,N/A,,P1,1,1.00,web,COMPLETED
T35,2025-03-01T10:00:00,,P1,1,1.00,web,COMPLETED
T36,2025-03-01T10:00:00,C999,P1,1,1.00,web,COMPLETED
T37,2025-03-01T10:00:00,C9,P1,1,1.00,web,COMPLETED
T38,2025-03-01T10:00:00,C1,P1,1,1.00,fax,COMPLETED
T39,2025-03-01T10:00:00,C1,P1,1,1.00,web,UNKNOWN
T40,2025-03-01T10:00:00,C1,P1,1,1.00,web,completed
T41,2025-03-01T10:00:00,,P1,1,1.00,fax,COMPLETED
"""
ORDERS_9_COLUMNS = """\
transaction_id,event_time,customer_id,product_id,quantity,unit_price_eur,channel,status,discount_pct
U01,2026-01-02T00:00:00,C1,P1,3,10.00,web,COMPLETED,0.250
U02,2026-01-02T00:00:00,C1,P1,1,10.00,web,COMPLETED,
U03,2026-01-02T00:00:00,C2,P1,1,0.05,web,COMPLETED,0.100
U04,2026-01-02T00:00:00,C2,P1,1,0.05,web,COMPLETED,0.100
U05,2026-01-02T00:00:00,C1,P1,1,10.00,web,COMPLETED,0.300
U06,2026-01-02T00:00:00,C5,P1,1,0.05,web,COMPLETED,0.100
U07,2026-01-02T00:00:00,C1,P1,1,10.00,web,COMPLETED,0.301
U08,2026-01-02T00:00:00,C1,P1,1,10.00,web,COMPLETED,0.5
U09,2026-01-02T00:00:00,C1,P1,1,10.00,web,COMPLETED,abc
U10,2026-01-02T00:00:00,C1,P1,1,10.00,web,COMPLETED,-0.100
U11,2026-01-02T00:00:00,C1,P1,1,10.00,web,CANCELLED,0.999
U12,2026-01-02T00:00:00,C1,P1,1,10.00,web,COMPLETED
"""
EDGE_REVENUE = [
    "DE\t0.12\t3",
    "FR\t97.40\t6",
    "IT\t148148154.88\t2",
    "UK\t0.05\t1",
    "UNKNOWN\t7.50\t2",
]
"""Rounded once per country: DE is 0.03 + 0.045 + 0.045, UK 0.045 rounded half up."""
EDGE_COUNTERS = {
    "Lines.HEADER": 2,
    "Lines.COMPLETED": 14,
    "Lines.OTHER_STATUS": 2,
    "Quality.MALFORMED_ROW": 4,
    "Quality.BAD_QUANTITY": 7,
    "Quality.BAD_PRICE": 6,
    "Quality.BAD_TIMESTAMP": 9,
    "Quality.NULL_CUSTOMER": 2,
    "Quality.UNKNOWN_CUSTOMER": 2,
    "Quality.BAD_ENUM": 3,
    "Quality.BAD_DISCOUNT": 5,
}

QUALITY = {
    "MALFORMED_ROW",
    "BAD_QUANTITY",
    "BAD_PRICE",
    "BAD_TIMESTAMP",
    "NULL_CUSTOMER",
    "UNKNOWN_CUSTOMER",
    "BAD_ENUM",
}
"""The contract's order anomalies that are invalid lines here; the others are valid lines."""


@dataclass(frozen=True)
class Drop:
    """Order and customer files read together, and what both engines must find in them."""

    orders: list[Path]
    customers: list[Path]
    revenue: list[str] | None
    counters: dict[str, int]


@dataclass(frozen=True)
class Output:
    revenue: list[str]
    counters: dict[str, int]


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")
    return path


@pytest.fixture
def edge_drop(tmp_path) -> Drop:
    return Drop(
        orders=[
            _write(tmp_path / "orders_8.csv", ORDERS_8_COLUMNS),
            _write(tmp_path / "orders_9.csv", ORDERS_9_COLUMNS),
        ],
        customers=[
            _write(tmp_path / "customers_j1.csv", CUSTOMERS_J1),
            _write(tmp_path / "customers_j2.csv", CUSTOMERS_J2),
        ],
        revenue=EDGE_REVENUE,
        counters=EDGE_COUNTERS,
    )


@pytest.fixture(scope="session")
def ci_landing(tmp_path_factory) -> Path:
    """The landing drops J1 to J3 of the generated ``ci`` profile."""
    out = tmp_path_factory.mktemp("generated")
    args = ["generate", "--profile", "ci", "--seed", "42", "--out", str(out)]
    assert datagen([*args, "--kit", str(REPO / "data" / "kit")]) == 0
    return out / "ci" / "landing"


@pytest.fixture(scope="session")
def ci_drop(ci_landing) -> Drop:
    """J1 to J3 of the ``ci`` profile; the manifests give the expected Quality counters."""
    expected = Counter()
    for day in DAYS:
        manifest = json.loads((ci_landing / day / "_MANIFEST.json").read_text(encoding="utf-8"))
        [orders] = [f for f in manifest["files"] if f["source"] == "orders"]
        expected.update(
            {f"Quality.{code}": n for code, n in orders["anomalies"].items() if code in QUALITY}
        )
    return Drop(
        orders=[next((ci_landing / day / "orders").iterdir()) for day in DAYS],
        customers=[next((ci_landing / day / "customers").iterdir()) for day in DAYS],
        revenue=None,
        counters=dict(expected),
    )


def _quote(paths) -> str:
    return " ".join(shlex.quote(str(path)) for path in paths)


def _counters(stderr: str) -> dict[str, int]:
    totals = Counter()
    for line in stderr.splitlines():
        if line.startswith("reporter:counter:"):
            group, name, value = line.removeprefix("reporter:counter:").split(",")
            totals[f"{group}.{name}"] += int(value)
    return dict(totals)


@pytest.fixture(scope="session")
def mapreduce_pipe():
    return _mapreduce_pipe


def _mapreduce_pipe(drop: Drop, *, combine_halves: bool = False) -> Output:
    """``(mapper.py customers < orders, for each file) | sort | reducer.py``, as Hadoop chains them.

    Each order file goes to its own mapper, as each split goes to its own map task. With
    ``combine_halves``, each mapper's output also goes through ``sort | reducer.py --combine``, as
    each map task's output goes through the combiner, before the final sort and reduce.
    """
    python = shlex.quote(sys.executable)
    mapper = f"{python} {shlex.quote(str(MAPPER))} {_quote(drop.customers)}"
    reducer = f"{python} {shlex.quote(str(REDUCER))}"
    combiner = f" | LC_ALL=C sort | {reducer} --combine" if combine_halves else ""
    maps = " && ".join(f"{mapper} < {_quote([f])}{combiner}" for f in drop.orders)
    command = f"({maps}) | LC_ALL=C sort | {reducer}"
    done = subprocess.run(
        ["bash", "-c", f"set -o pipefail; {command}"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    return Output(done.stdout.splitlines(), _counters(done.stderr))
