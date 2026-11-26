"""What landed for a day: the manifest's files, checked against the landing partitions, and their
raw records read without loss."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from gtdp.io import INGESTION_DATE
from gtdp.schemas import CORRUPT_RECORD, SOURCES, Format, Source

# Spark's reading defaults, spelled out because changing them loses data: no whitespace trimming
# (it would turn a key duplicate with a padded email into an exact duplicate) and one record per
# physical line.
CSV_OPTIONS = {
    "header": "true",
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": CORRUPT_RECORD,
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
    "multiLine": "false",
    "encoding": "UTF-8",
}


class LandingError(RuntimeError):
    """The landing of a day does not match its manifest."""


@dataclass(frozen=True)
class LandedFile:
    source: Source
    key: str
    """The file's ``path`` in the manifest, ``<source>/<name>``."""
    uri: str
    manifest_rows: int


def manifest_uri(landing: str, day: date) -> str:
    return f"{landing}/_manifests/{INGESTION_DATE}={day}/_MANIFEST.json"


def landed_files(
    spark: SparkSession, landing: str, day: date, source: str | None = None
) -> list[LandedFile]:
    """The files of ``source``, or of every source of the manifest, that landed for ``day``.

    The manifest is written last by ``land.sh``, so a day without one is not fully landed:
    ``FileNotFoundError``. ``LandingError`` when a source's landing partition does not hold
    exactly the files the manifest lists for it.
    """
    if source is not None and source not in SOURCES:
        raise ValueError(f"unknown source {source!r}, expected one of {sorted(SOURCES)}")
    manifest = json.loads(_read_text(spark, manifest_uri(landing, day)))
    if manifest.get("ingestion_date") != day.isoformat():
        raise LandingError(
            f"{manifest_uri(landing, day)} is for {manifest.get('ingestion_date')}, not {day}"
        )
    listed: dict[str, dict[str, int]] = {}
    for entry in manifest["files"]:
        listed.setdefault(entry["source"], {})[entry["path"]] = int(entry["rows"])
    wanted = [source] if source is not None else sorted(listed)
    if unknown := sorted(set(wanted) - SOURCES.keys()):
        raise LandingError(f"the manifest of {day} lists unknown sources {unknown}")
    if missing := [name for name in wanted if name not in listed]:
        raise LandingError(f"the manifest of {day} lists no file for {missing}")

    files = []
    for name in wanted:
        partition = f"{landing}/{name}/{INGESTION_DATE}={day}"
        expected = {key.rsplit("/", 1)[-1]: (key, rows) for key, rows in listed[name].items()}
        present = set(_file_names(spark, partition))
        if present != expected.keys():
            raise LandingError(
                f"{partition} does not match the manifest: "
                f"missing {sorted(expected.keys() - present)}, "
                f"not in the manifest {sorted(present - expected.keys())}"
            )
        files += [
            LandedFile(SOURCES[name], key, f"{partition}/{file_name}", rows)
            for file_name, (key, rows) in sorted(expected.items())
        ]
    return files


def count_data_lines(spark: SparkSession, file: LandedFile) -> int:
    """Physical lines of ``file``, its header line excepted."""
    lines = spark.read.text(file.uri).count()
    return lines - 1 if file.source.format is Format.CSV else lines


def read_header(spark: SparkSession, file: LandedFile) -> tuple[str, ...]:
    """The column names of ``file``, split by the same CSV parser that reads its records."""
    if file.source.format is Format.TEXT:
        return file.source.columns
    return tuple(spark.read.options(**CSV_OPTIONS).csv(file.uri).columns)


def read_records(spark: SparkSession, file: LandedFile, columns: tuple[str, ...]) -> DataFrame:
    """One row per data line of ``file``: ``columns`` as strings, then ``_corrupt_record``.

    A CSV line whose field count differs from ``columns`` keeps the fields it has and its whole
    text in ``_corrupt_record``. A text line is never corrupt.
    """
    if file.source.format is Format.TEXT:
        return spark.read.text(file.uri).withColumn(CORRUPT_RECORD, F.lit(None).cast(StringType()))
    schema = StructType(
        [StructField(column, StringType()) for column in (*columns, CORRUPT_RECORD)]
    )
    return spark.read.schema(schema).options(**CSV_OPTIONS).csv(file.uri)


def _file_names(spark: SparkSession, directory: str) -> list[str]:
    fs, path = _filesystem(spark, directory)
    if not fs.exists(path):
        return []
    names = [status.getPath().getName() for status in fs.listStatus(path)]
    return [name for name in names if not name.startswith(("_", "."))]


def _read_text(spark: SparkSession, uri: str) -> str:
    """The UTF-8 text of a small file. Spark reads a ``_*`` file such as the manifest as empty."""
    fs, path = _filesystem(spark, uri)
    if not fs.exists(path):
        raise FileNotFoundError(f"{uri} not found: the day is not landed, or not completely")
    stream = fs.open(path)
    try:
        return bytes(stream.readAllBytes()).decode("utf-8")
    finally:
        stream.close()


def _filesystem(spark: SparkSession, uri: str) -> tuple[Any, Any]:
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), path
