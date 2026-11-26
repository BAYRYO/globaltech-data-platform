"""KAN-30: no personal data in clear in gold, on the generated ci drops of seed 42."""

import pytest
from pyspark.sql import functions as F

from gtdp.gold.kpis import KPIS
from gtdp.gold.tables import STAR, table_path
from gtdp.io import read_table
from gtdp.schemas import SOURCES, bronze_schema

pytestmark = pytest.mark.slow

PERSONAL_COLUMNS = {"full_name", "name", "email", "phone", "ip", "ip_address"}
GOLD_TABLES = [table.name for table in STAR] + list(KPIS)


def gold(lake, name):
    return read_table(lake.spark, table_path(lake.cfg.paths.gold, name))


def bronze(lake, source):
    return read_table(
        lake.spark, f"{lake.cfg.paths.bronze}/{source}", bronze_schema(SOURCES[source])
    )


def personal_values(lake):
    """Every name, email (as written and normalised), phone and IP address of bronze."""
    customers = bronze(lake, "customers")
    values = [customers.select(F.col(c).alias("value")) for c in ("full_name", "email", "phone")]
    values.append(customers.select(F.lower(F.trim("email")).alias("value")))
    values.append(
        bronze(lake, "weblogs").select(F.regexp_extract("value", r"^(\S+) ", 1).alias("value"))
    )
    union = values[0]
    for frame in values[1:]:
        union = union.unionByName(frame)
    return union.where(F.length("value") > 0).distinct()


def test_no_gold_table_has_a_personal_data_column(generated):
    columns = {name: set(gold(generated, name).columns) for name in GOLD_TABLES}

    assert {name: names & PERSONAL_COLUMNS for name, names in columns.items()} == {
        name: set() for name in GOLD_TABLES
    }
    assert "email_hash" in columns["dim_customer"]


def test_no_bronze_personal_value_appears_in_a_gold_string_column(generated):
    personal = personal_values(generated).cache()
    found = {}
    for name in GOLD_TABLES:
        table = gold(generated, name)
        strings = [column for column, kind in table.dtypes if kind == "string"]
        if strings:
            values = table.select(F.explode(F.array(*strings)).alias("value")).distinct()
            found[name] = values.join(personal, "value").count()

    assert personal.count() > 5000
    assert {"dim_customer", "fact_sales", "fact_web_events"} <= found.keys()
    assert found == dict.fromkeys(found, 0)
