"""The data dictionary describes each table and JSON file the code writes, column for column.

A section of a page in ``docs/data-dictionary`` documents the paths named in backticks in its
heading, relative to the lake root; a ``<name>`` segment stands for any value. Its column tables
have the header ``Colonne | Type | Règle | Exemple``, or ``Champ | ...`` for the fields of a JSON
file. A column whose rule starts with ``Partition`` is a partition column.

The code side is read from the registries: the declared schemas of bronze, silver, the star and
the sensor stream, the indicators built on empty inputs, and the ops tables and JSON files written
by a tiny run in a temporary lake.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql.types import StructType

from gtdp.audit import Layer, audit_path, audited_run
from gtdp.config import LakePaths, Secret, load_config
from gtdp.gold.kpis import KPIS, Star
from gtdp.gold.state import STATE, State, Status, write_state
from gtdp.gold.tables import (
    DIM_CUSTOMER,
    DIM_PRODUCT,
    FACT_SALES,
    FACT_WEB_EVENTS,
    ORDER_DATE,
    STAR,
)
from gtdp.gold.tables import table_path as gold_path
from gtdp.io import INGESTION_DATE, read_table
from gtdp.jobs.registry import JobContext
from gtdp.jobs.smoke import smoke
from gtdp.schemas import SILVER_ORDERS, SILVER_REJECTS, SOURCES, bronze_schema
from gtdp.silver.days import COMPLETE, days_path, mark_complete, mark_source
from gtdp.silver.pipelines import PIPELINES, rejects_path
from gtdp.silver.pipelines import table_path as silver_path
from gtdp.silver.pseudonymize import ensure_fingerprint, fingerprint_path
from gtdp.streaming.sensors import BATCH_ID
from gtdp.streaming.stream import OUTPUTS, SensorPaths

pytestmark = pytest.mark.slow

DICTIONARY = Path(__file__).resolve().parents[2] / "docs" / "data-dictionary"
HEADERS = ("| Colonne | Type | Règle | Exemple |", "| Champ | Type | Règle | Exemple |")
DAY = date(2026, 1, 1)


@dataclass
class Shape:
    columns: dict[str, str] = field(default_factory=dict)
    """Column name -> Spark SQL type (``simpleString``)."""
    partitions: set[str] = field(default_factory=set)

    @classmethod
    def of(cls, schema: StructType, *partitions: str) -> Shape:
        return cls({f.name: f.dataType.simpleString() for f in schema.fields}, set(partitions))


def _names(cell: str) -> list[str]:
    return re.findall(r"`([^`]+)`", cell)


def documented() -> dict[str, Shape]:
    """Every path a section heading names, with the columns of the section's column tables."""
    shapes: dict[str, Shape] = {}
    for page in sorted(DICTIONARY.glob("*.md")):
        paths: list[str] = []
        in_table = False
        for line in page.read_text(encoding="utf-8").splitlines():
            if line.startswith("#"):
                paths, in_table = _names(line), False
                for path in paths:
                    shapes.setdefault(path, Shape())
            elif line in HEADERS:
                in_table = True
            elif in_table and line.startswith("| ---"):
                continue
            elif in_table and line.startswith("|"):
                names, kind, rule = (c.strip() for c in re.split(r"(?<!\\)\|", line)[1:4])
                for path in paths:
                    for name in _names(names):
                        shapes[path].columns[name] = kind
                        if rule.startswith("Partition"):
                            shapes[path].partitions.add(name)
            else:
                in_table = False
    return shapes


def _relative(path: str) -> str:
    """``path`` from the root of the temporary lake."""
    return path.rsplit("/lake/", 1)[1]


def _matches(pattern: str, path: str) -> bool:
    literal = (re.escape(part) for part in re.split(r"<[^>]+>", pattern))
    return re.fullmatch("[^/]+".join(literal), path) is not None


def _json_type(value: object) -> str:
    if isinstance(value, list):
        return f"array<{_json_type(value[0])}>"
    if isinstance(value, dict):
        return f"map<string,{_json_type(next(iter(value.values())))}>"
    return {str: "string", int: "bigint", float: "double", bool: "boolean"}[type(value)]


def _partitions(directory: Path) -> list[str]:
    """The partition columns of the table at ``directory``: its ``<column>=<value>`` levels."""
    names = []
    while levels := sorted(d for d in directory.iterdir() if d.is_dir() and "=" in d.name):
        directory = levels[0]
        names.append(directory.name.split("=")[0])
    return names


def _declared(spark, paths: LakePaths, settings) -> dict[str, Shape]:
    tables = {
        f"{paths.bronze}/{source.name}": Shape.of(bronze_schema(source), INGESTION_DATE)
        for source in SOURCES.values()
    }
    for source, pipeline in PIPELINES.items():
        day = (INGESTION_DATE,) if pipeline.partitioned else ()
        tables[silver_path(paths.silver, source)] = Shape.of(pipeline.schema, *day)
        tables[rejects_path(paths.silver, source)] = Shape.of(SILVER_REJECTS, INGESTION_DATE)
    for table in STAR:
        partitions = (ORDER_DATE,) if table is FACT_SALES else ()
        tables[gold_path(paths.gold, table.name)] = Shape.of(table.schema, *partitions)
    empty = [
        spark.createDataFrame([], schema)
        for schema in (
            FACT_SALES.schema,
            FACT_WEB_EVENTS.schema,
            DIM_CUSTOMER.schema,
            DIM_PRODUCT.schema,
            SILVER_ORDERS,
            SILVER_REJECTS,
        )
    ]
    for name, build in KPIS.items():
        tables[gold_path(paths.gold, name)] = Shape.of(build(Star(*empty), settings).schema)
    sensors = SensorPaths.of(paths, "streaming-sensors")
    for name, output in OUTPUTS.items():
        tables[sensors.tables[name]] = Shape.of(output.schema, output.date_column, BATCH_ID)
    return tables


def _written(spark, cfg, lake: Path) -> dict[str, Shape]:
    """The ops tables and the JSON files, as a tiny run writes them in ``lake``."""
    paths = cfg.paths
    with audited_run(spark, cfg, "smoke", Layer.OPS) as run:
        smoke(JobContext(spark, cfg, run))
    write_state(spark, paths.gold, State(Status.PUBLISHED, run.run_id, (DAY,)))
    mark_source(spark, paths.silver, DAY, "orders", run.run_id)
    mark_complete(spark, paths.silver, DAY, ["orders"], run.run_id)
    ensure_fingerprint(spark, paths.ops, Secret("a-test-salt-of-16+chars"), run.run_id)

    shapes = {}
    for table in (audit_path(cfg), f"{paths.ops}/smoke"):
        local = lake / _relative(table)
        shapes[table] = Shape.of(read_table(spark, table).schema, *_partitions(local))
    day = f"{days_path(paths.silver)}/{INGESTION_DATE}={DAY}"
    files = (f"{paths.gold}/{STATE}", f"{day}/orders.json", f"{day}/{COMPLETE}")
    for path in (*files, fingerprint_path(paths.ops)):
        record = json.loads((lake / _relative(path)).read_text(encoding="utf-8"))
        shapes[path] = Shape({key: _json_type(value) for key, value in record.items()})
    return shapes


@pytest.fixture(scope="module")
def code(spark, tmp_path_factory) -> dict[str, Shape]:
    """Every table and JSON file the code writes, by path relative to the lake root."""
    lake = tmp_path_factory.mktemp("dictionary") / "lake"
    areas = {f"paths.{area.name}": str(lake / area.name) for area in fields(LakePaths)}
    cfg = load_config("local", overrides=areas)
    shapes = _declared(spark, cfg.paths, cfg.gold) | _written(spark, cfg, lake)
    return {_relative(path): shape for path, shape in shapes.items()}


@pytest.fixture(scope="module")
def doc() -> dict[str, Shape]:
    return documented()


def _documented(path: str, doc: dict[str, Shape]) -> Shape:
    """The columns of ``path`` in every section naming it."""
    merged = Shape()
    for pattern, shape in doc.items():
        if _matches(pattern, path):
            merged.columns |= shape.columns
            merged.partitions |= shape.partitions
    return merged


def _report(drift: dict[str, str]) -> str:
    return "\n" + "\n".join(f"{path}: {what}" for path, what in sorted(drift.items()))


def test_every_table_and_file_of_the_code_has_a_dictionary_section(code, doc):
    missing = sorted(path for path in code if not any(_matches(p, path) for p in doc))
    assert not missing, f"no dictionary section names {missing}"


def test_every_dictionary_section_names_a_table_or_file_of_the_code(code, doc):
    unknown = sorted(pattern for pattern in doc if not any(_matches(pattern, p) for p in code))
    assert not unknown, f"the code writes nothing at {unknown}"


def test_the_dictionary_lists_exactly_the_columns_of_the_code(code, doc):
    drift = {}
    for path, shape in code.items():
        documented = _documented(path, doc).columns
        missing = sorted(shape.columns.keys() - documented.keys())
        extra = sorted(documented.keys() - shape.columns.keys())
        if missing or extra:
            drift[path] = f"in the code only {missing}, in the dictionary only {extra}"
    assert not drift, _report(drift)


def test_the_dictionary_gives_each_column_the_type_of_the_code(code, doc):
    drift = {}
    for path, shape in code.items():
        documented = _documented(path, doc).columns
        differ = {
            name: f"{documented[name]} documented, {kind} in the code"
            for name, kind in shape.columns.items()
            if name in documented and documented[name] != kind
        }
        if differ:
            drift[path] = str(differ)
    assert not drift, _report(drift)


def test_the_dictionary_marks_the_partition_columns_of_the_code(code, doc):
    drift = {
        path: f"{sorted(documented)} documented, {sorted(shape.partitions)} in the code"
        for path, shape in code.items()
        if (documented := _documented(path, doc).partitions) != shape.partitions
    }
    assert not drift, _report(drift)
