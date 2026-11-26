"""Silver counts against the generator's manifests, through the disposition table of
data-model.md ("Règles de qualité et disposition").

A manifest counts the anomalies injected in each file; the table says which silver measure
(``gtdp.silver.quality.measures``) each one must land in. Every reject and duplicate measure is
compared, so a record counted under the wrong code shows twice.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType

from gtdp.io import INGESTION_DATE, read_table
from gtdp.schemas import REJECT_REASONS, SILVER_REJECTS
from gtdp.silver.pipelines import PIPELINES, rejects_path, table_path
from gtdp.silver.quality import Disposition

REJECTED, CLEAN = Disposition.REJECTED.value, Disposition.CLEAN.value


@dataclass(frozen=True)
class Outcome:
    measure: str | None
    """The measure that counts the anomaly's records; ``None`` for a record kept as is."""
    records: int = 1
    """Records per anomaly the manifest counts."""


KEPT = Outcome(None)


def _rejected(*codes: str) -> dict[str, Outcome]:
    return {code: Outcome(f"rejected.{code}") for code in codes}


DISPOSITION_TABLE: Mapping[str, Mapping[str, Outcome]] = MappingProxyType(
    {
        "orders": {
            "DUPLICATE_EXACT": Outcome("duplicated.exact"),
            "DUPLICATE_CROSS_DAY": Outcome("duplicated.cross_day"),
            **_rejected(
                "NULL_CUSTOMER",
                "UNKNOWN_CUSTOMER",
                "UNKNOWN_PRODUCT",
                "BAD_QUANTITY",
                "BAD_PRICE",
                "BAD_TIMESTAMP",
                "FUTURE_TIMESTAMP",
                "BAD_ENUM",
                "MALFORMED_ROW",
            ),
        },
        "customers": {
            "DUPLICATE_EXACT": Outcome("duplicated.exact"),
            "DUPLICATE_KEY": Outcome("duplicated.key"),
            **{
                code: Outcome(f"flagged.{code}")
                for code in ("MISSING_EMAIL", "BAD_EMAIL", "MISSING_REGION", "BAD_DATE")
            },
        },
        "products": {
            "DUPLICATE_EXACT": Outcome("duplicated.exact"),
            **_rejected("BAD_PRICE", "NULL_CATEGORY", "MALFORMED_ROW"),
            # The manifest counts the added record; silver rejects it with the version it restates.
            "CONFLICTING_VERSION": Outcome("rejected.CONFLICTING_VERSION", records=2),
        },
        "weblogs": {**_rejected("MALFORMED_LINE"), "HTTP_404": KEPT, "HTTP_500": KEPT},
    }
)


@dataclass(frozen=True)
class Mismatch:
    measure: str
    expected: int
    measured: int


def expected(source: str, rows: int, anomalies: Mapping[str, int]) -> dict[str, int]:
    """The measures silver must produce for a file of ``rows`` records and these anomalies."""
    table = DISPOSITION_TABLE[source]
    if unknown := sorted(anomalies.keys() - table.keys()):
        raise ValueError(f"the disposition table of {source} has no row for {unknown}")
    counts = {outcome.measure: 0 for outcome in table.values() if outcome.measure}
    for code, count in anomalies.items():
        if measure := table[code].measure:
            counts[measure] += count * table[code].records
    rejected = sum(n for name, n in counts.items() if name.startswith(f"{REJECTED}."))
    duplicated = sum(n for name, n in counts.items() if name.startswith("duplicated."))
    return {**counts, REJECTED: rejected, CLEAN: rows - rejected - duplicated}


def compare(expected: Mapping[str, int], measured: Mapping[str, int]) -> list[Mismatch]:
    """Every measure of ``expected``, and every reject or duplicate measure, that differs."""
    names = expected.keys() | {
        name for name in measured if name.startswith((REJECTED, "duplicated."))
    }
    return [
        Mismatch(name, expected.get(name, 0), measured.get(name, 0))
        for name in sorted(names)
        if expected.get(name, 0) != measured.get(name, 0)
    ]


def read_manifest(spark: SparkSession, landing: str, day: date) -> dict[str, Any]:
    """The ``_MANIFEST.json`` landed for ``day``. Spark's readers skip ``_*`` files: read it
    through the Hadoop filesystem."""
    uri = f"{landing}/_manifests/{INGESTION_DATE}={day}/_MANIFEST.json"
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.open(path)
    try:
        return json.loads(bytes(stream.readAllBytes()).decode("utf-8"))
    finally:
        stream.close()


def manifest_expected(manifest: Mapping[str, Any]) -> dict[str, dict[str, int]]:
    """The expected measures of each source of a manifest."""
    by_source: dict[str, dict[str, int]] = {}
    for entry in manifest["files"]:
        counts = expected(entry["source"], entry["rows"], entry["anomalies"])
        merged = by_source.setdefault(entry["source"], {})
        for name, count in counts.items():
            merged[name] = merged.get(name, 0) + count
    return by_source


_SOURCE_METRICS = StructType(
    [
        StructField(
            "sources",
            MapType(
                StringType(),
                StructType(
                    [
                        StructField("rows_in", LongType()),
                        StructField("measures", MapType(StringType(), LongType())),
                    ]
                ),
            ),
        )
    ]
)


def audit_measures(audit: DataFrame) -> DataFrame:
    """Per source and day, the counts of the latest successful silver run that covered it:
    ``source, ingestion_date, run_id, started_at, rows_in, measures``."""
    runs = audit.where((F.col("job_name") == "silver") & (F.col("status") == "SUCCESS")).select(
        "run_id",
        "started_at",
        INGESTION_DATE,
        F.explode(F.from_json("metrics_json", _SOURCE_METRICS).sources).alias("source", "m"),
    )
    latest = runs.groupBy("source", INGESTION_DATE).agg(
        F.max_by(F.struct("run_id", "started_at", "m.rows_in", "m.measures"), "started_at").alias(
            "run"
        )
    )
    return latest.select("source", INGESTION_DATE, "run.*")


def table_measures(spark: SparkSession, silver: str, source: str, day: date) -> dict[str, int]:
    """The measures that the published tables of ``day`` show: rejects per reason and in total,
    and the clean rows of a partitioned table."""
    of_day = F.col(INGESTION_DATE) == F.lit(day)
    rejects = read_table(spark, rejects_path(silver, source), SILVER_REJECTS).where(of_day)
    counts = {
        f"{REJECTED}.{row['code']}": row["count"]
        for row in rejects.select(F.explode(REJECT_REASONS).alias("code"))
        .groupBy("code")
        .count()
        .collect()
    }
    counts[REJECTED] = rejects.count()
    pipeline = PIPELINES[source]
    if pipeline.partitioned:
        published = read_table(spark, table_path(silver, source), pipeline.schema)
        counts[CLEAN] = published.where(of_day).count()
    return counts
