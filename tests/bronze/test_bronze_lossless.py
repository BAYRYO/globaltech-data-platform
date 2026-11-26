import pytest
from pyspark.sql import functions as F

from gtdp.io import read_table
from gtdp.schemas import SOURCES, bronze_schema

pytestmark = pytest.mark.slow

J1, J2, J3 = "2026-01-01", "2026-01-02", "2026-01-03"


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
