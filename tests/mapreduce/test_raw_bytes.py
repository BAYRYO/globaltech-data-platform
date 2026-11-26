"""Both engines agree on raw bytes beyond what the data contract promises.

Hadoop Streaming and Spark both split files with Hadoop's LineRecordReader: a line ends with LF,
CR LF or a lone CR, and a UTF-8 BOM at the start of a file is dropped. The local pipe must give the
mapper the same lines, one orders file per mapper as one split per map task. A region whose
country prefix holds an ASCII control character gives the country UNKNOWN in both engines.
"""

import subprocess
import sys
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

from gtdp.perf.revenue_by_country import (
    classify_orders,
    counters,
    customer_countries,
    revenue_lines,
    summarize,
)

MAPPER = Path(__file__).resolve().parents[2] / "mapreduce" / "mapper.py"
BOM = b"\xef\xbb\xbf"
ORDERS_HEADER = (
    b"transaction_id,event_time,customer_id,product_id,quantity,unit_price_eur,channel,status\n"
)
CUSTOMERS_HEADER = b"customer_id,full_name,email,phone,region,segment,signup_date,updated_at\n"


def order(customer: bytes = b"C1", end: bytes = b"\n", transaction: bytes = b"T1") -> bytes:
    return transaction + b",2025-03-01T10:00:00," + customer + b",P1,2,10.50,web,COMPLETED" + end


def customer(cid: bytes, region: bytes, end: bytes = b"\n") -> bytes:
    return cid + b",N,e@x.io,+1," + region + b",SMB,2024-01-01,2025-12-31T00:00:00" + end


CUSTOMERS = CUSTOMERS_HEADER + customer(b"C1", b"FR-IDF") + customer(b"C2", b"DE-BY")
ONE_FR_LINE = (["FR\t21.00\t1"], {"Lines.HEADER": 1, "Lines.COMPLETED": 1})
CASES = {
    "BOM before the orders header": ([BOM + ORDERS_HEADER + order()], [CUSTOMERS], ONE_FR_LINE),
    "BOM before customers without header": (
        [ORDERS_HEADER + order()],
        [BOM + customer(b"C1", b"FR-IDF")],
        ONE_FR_LINE,
    ),
    "BOM before the customers header": (
        [ORDERS_HEADER + order() + order(BOM + b"customer_id")],
        [BOM + CUSTOMERS],
        (
            ["FR\t21.00\t1"],
            {"Lines.HEADER": 1, "Lines.COMPLETED": 1, "Quality.UNKNOWN_CUSTOMER": 1},
        ),
    ),
    "CR LF line ends": (
        [(ORDERS_HEADER + order() + order(b"C2")).replace(b"\n", b"\r\n")],
        [CUSTOMERS.replace(b"\n", b"\r\n")],
        (["DE\t21.00\t1", "FR\t21.00\t1"], {"Lines.HEADER": 1, "Lines.COMPLETED": 2}),
    ),
    "lone CR line ends": (
        [
            ORDERS_HEADER
            + order(transaction=b"T\rX")
            + order(end=b"\r")
            + order(end=b"\r\r\n")
            + order()
        ],
        [CUSTOMERS.replace(b"\n", b"\r")],
        (
            ["FR\t84.00\t4"],
            {"Lines.HEADER": 1, "Lines.COMPLETED": 4, "Quality.MALFORMED_ROW": 2},
        ),
    ),
    "an orders file without final newline": (
        [ORDERS_HEADER + order(end=b""), ORDERS_HEADER + order(b"C2")],
        [CUSTOMERS.removesuffix(b"\n")],
        (["DE\t21.00\t1", "FR\t21.00\t1"], {"Lines.HEADER": 2, "Lines.COMPLETED": 2}),
    ),
    "control characters in a country": (
        [ORDERS_HEADER + b"".join(order(cid) for cid in (b"C1", b"C2", b"C3", b"C4", b"C5"))],
        [
            CUSTOMERS_HEADER
            + customer(b"C1", b"F\tR-IDF")
            + customer(b"C2", b"A\x01-1")
            + customer(b"C3", b"A-1")
            + customer(b"C4", b"F\x7fR-1")
            + customer(b"C5", b"FR-\t1")
        ],
        (
            ["A\t21.00\t1", "FR\t21.00\t1", "UNKNOWN\t63.00\t3"],
            {"Lines.HEADER": 1, "Lines.COMPLETED": 5},
        ),
    ),
}
"""Order files, customer files, and the (revenue, counters) both engines must give."""


def write(directory: Path, name: str, blobs: list[bytes]) -> list[Path]:
    paths = [directory / f"{name}_{index}.csv" for index in range(len(blobs))]
    for path, blob in zip(paths, blobs, strict=True):
        path.write_bytes(blob)
    return paths


def spark_output(spark, orders: list[Path], customers: list[Path]):
    lines = spark.read.text([str(path) for path in orders])
    countries = customer_countries(spark.read.text([str(path) for path in customers]))
    summary = summarize(classify_orders(lines, countries)).collect()
    totals = {f"{group}.{name}": n for (group, name), n in counters(summary).items()}
    return revenue_lines(summary), totals


def mapper(customers: list[Path], stdin: bytes) -> tuple[bytes, dict[str, int]]:
    done = subprocess.run(
        [sys.executable, str(MAPPER), *map(str, customers)],
        input=stdin,
        capture_output=True,
        check=True,
    )
    totals = Counter()
    for line in done.stderr.decode().splitlines():
        group, name, value = line.removeprefix("reporter:counter:").split(",")
        totals[f"{group}.{name}"] += int(value)
    return done.stdout, dict(totals)


@pytest.mark.parametrize("case", CASES)
def test_both_engines_give_the_expected_result(case, tmp_path, spark, mapreduce_pipe):
    orders, customers, expected = CASES[case]
    drop = SimpleNamespace(
        orders=write(tmp_path, "orders", orders), customers=write(tmp_path, "customers", customers)
    )

    mr = mapreduce_pipe(drop)
    assert (mr.revenue, mr.counters) == expected
    assert spark_output(spark, drop.orders, drop.customers) == expected


@pytest.mark.parametrize("case", CASES)
def test_the_mapper_reads_a_file_as_hadoop_splits_it(case, tmp_path, spark):
    orders, customers, _ = CASES[case]
    customer_files = write(tmp_path, "customers", customers)
    for path in write(tmp_path, "orders", orders):
        hadoop_lines = spark.sparkContext.textFile(str(path)).collect()

        assert mapper(customer_files, path.read_bytes()) == mapper(
            customer_files, "".join(f"{line}\n" for line in hadoop_lines).encode()
        )
