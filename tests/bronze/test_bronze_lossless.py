import hashlib
import re
import shutil
from collections import Counter

import pytest
from pyspark.sql import functions as F

from gtdp.bronze.report import SAMPLES, unmatched
from gtdp.io import read_table
from gtdp.schemas import SOURCES, bronze_schema

pytestmark = pytest.mark.slow

J1, J2, J3 = "2026-01-01", "2026-01-02", "2026-01-03"
CUSTOMERS = "customers/customers_20260102.csv"
TAIL = "x@example.com,+33,FR-IDF,SMB,2025-01-01,2026-01-01T00:00:00"
HEX_SHA256 = r"^[0-9a-f]{64}$"


def sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


def land_customers(lake, lines):
    """Land J2 with ``lines`` as the data lines of its customers file."""
    manifest = lake.land(J2)
    header = lake.landing_file(J2, CUSTOMERS).read_text().split("\n", 1)[0]
    lake.rewrite(J2, manifest, CUSTOMERS, ("\n".join([header, *lines]) + "\n").encode(), len(lines))


def test_quotes_stay_in_the_values_and_each_record_hash_is_the_sha256_of_its_line(lake):
    lines = {
        "quoted": f'C900001,"Jean",{TAIL}',
        "bare": f"C900001,Jean,{TAIL}",
        "empty quotes": f'C900002,"",{TAIL}',
        "empty": f"C900002,,{TAIL}",
        "doubled quotes": f'C900003,"Jean ""le Bon""",{TAIL}',
        "quoted comma": f'C900004,"Dupont, Jean",{TAIL}',
    }
    land_customers(lake, list(lines.values()))

    lake.run(J2, source="customers")

    rows = lake.table("customers").collect()
    assert Counter(row._record_hash for row in rows) == Counter(map(sha, lines.values()))
    names = {row._record_hash: (row.full_name, row._corrupt_record) for row in rows}
    assert names[sha(lines["quoted"])] == ('"Jean"', None)
    assert names[sha(lines["bare"])] == ("Jean", None)
    assert names[sha(lines["empty quotes"])] == ('""', None)
    assert names[sha(lines["empty"])] == (None, None)
    assert names[sha(lines["doubled quotes"])] == ('"Jean ""le Bon"""', None)
    assert names[sha(lines["quoted comma"])][1] == lines["quoted comma"]


@pytest.mark.parametrize(("days", "merge_schema"), [((J1, J2, J3), False), ((J1, J2), True)])
def test_read_table_with_bronze_schema_gives_its_columns_discount_pct_null_before_j3(
    lake, days, merge_schema
):
    for day in days:
        lake.land(day)
        lake.run(day)
    schema = bronze_schema(SOURCES["orders"])

    orders = read_table(
        lake.spark, f"{lake.cfg.paths.bronze}/orders", schema, merge_schema=merge_schema
    )

    assert orders.schema == schema
    discounts = {
        row.ingestion_date.isoformat(): row.n
        for row in orders.groupBy("ingestion_date")
        .agg(F.count("discount_pct").alias("n"))
        .collect()
    }
    assert discounts[J1] == discounts[J2] == 0
    assert discounts.get(J3, 1) > 0


def test_the_report_shows_bronze_records_without_landing_by_their_hashes_only(lake):
    manifest = lake.land(J2)
    lake.run(J2)
    weblogs = next(e for e in manifest["files"] if e["source"] == "weblogs")["rows"]
    shutil.rmtree(lake.landing_file(J2, "weblogs/x").parent)
    customers = lake.landing_file(J2, CUSTOMERS)
    header, *lines = customers.read_text().splitlines()
    customers.write_text(
        "\n".join([header, *(f"{line} " for line in lines[:3]), *lines[3:]]) + "\n"
    )
    paths = lake.cfg.paths

    report = {
        row.source: row for row in unmatched(lake.spark, paths.landing, paths.bronze).collect()
    }

    gone = report["weblogs"]
    assert (gone.landing_lines, gone.bronze_rows) == (0, weblogs)
    assert (gone.landing_not_in_bronze, gone.bronze_not_in_landing) == (0, weblogs)
    changed = report["customers"]
    assert (changed.landing_not_in_bronze, changed.bronze_not_in_landing) == (1 + 3, 3)
    samples = [
        *gone.bronze_not_in_landing_hashes,
        *changed.landing_not_in_bronze_hashes,
        *changed.bronze_not_in_landing_hashes,
    ]
    assert len(samples) == SAMPLES + (1 + 3) + 3
    assert all(re.fullmatch(HEX_SHA256, value) for value in samples)
    assert set(changed.bronze_not_in_landing_hashes) <= {sha(line) for line in lines[:3]}
    strings = [value for row in report.values() for value in row if isinstance(value, str)]
    assert set(strings) <= set(SOURCES)
