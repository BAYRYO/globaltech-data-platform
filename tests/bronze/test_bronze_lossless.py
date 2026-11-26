import hashlib
from collections import Counter

import pytest
from pyspark.sql import functions as F

from gtdp.io import read_table
from gtdp.schemas import SOURCES, bronze_schema

pytestmark = pytest.mark.slow

J1, J2, J3 = "2026-01-01", "2026-01-02", "2026-01-03"
CUSTOMERS = "customers/customers_20260102.csv"
TAIL = "x@example.com,+33,FR-IDF,SMB,2025-01-01,2026-01-01T00:00:00"


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
