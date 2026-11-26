"""What landed for a day: the manifest's files, checked against the landing partitions and their
manifest entries, and their raw records read without loss."""

from __future__ import annotations

import codecs
import hashlib
import json
from collections.abc import Mapping, Sequence
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
CHUNK_BYTES = 1 << 20


class LandingError(RuntimeError):
    """The landing of a day does not match its manifest."""


@dataclass(frozen=True)
class LandedFile:
    source: Source
    key: str
    """The file's ``path`` in the manifest, ``<source>/<name>``."""
    uri: str
    manifest_rows: int
    manifest_sha256: str


@dataclass(frozen=True)
class Scan:
    """What one read of a landing file's bytes found."""

    sha256: str
    data_lines: int
    """Physical lines, a last line without line break included, the CSV header excepted."""
    problem: str | None
    """The first line that is not UTF-8."""


def manifest_uri(landing: str, day: date) -> str:
    return f"{landing}/_manifests/{INGESTION_DATE}={day}/_MANIFEST.json"


def landed_files(
    spark: SparkSession, landing: str, day: date, source: str | None = None
) -> list[LandedFile]:
    """The files of ``source``, or of every source of the manifest, that landed for ``day``.

    The manifest is written last by ``land.sh``, so a day without one is not fully landed:
    ``FileNotFoundError``. ``LandingError`` when the manifest lists no file for ``source``, or when
    a source's landing partition does not hold exactly the files the manifest lists for it.
    """
    if source is not None and source not in SOURCES:
        raise ValueError(f"unknown source {source!r}, expected one of {sorted(SOURCES)}")
    manifest = json.loads(_read_text(spark, manifest_uri(landing, day)))
    if manifest.get("ingestion_date") != day.isoformat():
        raise LandingError(
            f"{manifest_uri(landing, day)} is for {manifest.get('ingestion_date')}, not {day}"
        )
    listed: dict[str, dict[str, tuple[int, str]]] = {}
    for entry in manifest["files"]:
        listed.setdefault(entry["source"], {})[entry["path"]] = (
            int(entry["rows"]),
            str(entry.get("sha256") or ""),
        )
    wanted = [source] if source is not None else sorted(listed)
    if unknown := sorted(set(wanted) - SOURCES.keys()):
        raise LandingError(f"the manifest of {day} lists unknown sources {unknown}")
    if missing := [name for name in wanted if name not in listed]:
        raise LandingError(f"the manifest of {day} lists no file for {missing}")

    files = []
    for name in wanted:
        partition = f"{landing}/{name}/{INGESTION_DATE}={day}"
        expected = {key.rsplit("/", 1)[-1]: (key, entry) for key, entry in listed[name].items()}
        present = set(_file_names(spark, partition))
        if present != expected.keys():
            raise LandingError(
                f"{partition} does not match the manifest: "
                f"missing {sorted(expected.keys() - present)}, "
                f"not in the manifest {sorted(present - expected.keys())}"
            )
        files += [
            LandedFile(SOURCES[name], key, f"{partition}/{file_name}", rows, sha256)
            for file_name, (key, (rows, sha256)) in sorted(expected.items())
        ]
    return files


def scan(spark: SparkSession, file: LandedFile) -> Scan:
    """Read the bytes of ``file`` once, on the driver: SHA-256, data lines, text problem."""
    fs, path = _filesystem(spark, file.uri)
    is_csv = file.source.format is Format.CSV
    digest, decoder = hashlib.sha256(), codecs.getincrementaldecoder("utf-8")()
    lines, last, problem = 0, b"", None
    stream = fs.open(path)
    try:
        while chunk := bytes(stream.readNBytes(CHUNK_BYTES)):
            digest.update(chunk)
            problem = problem or _text_problem(decoder, chunk, lines)
            lines += chunk.count(b"\n")
            last = chunk[-1:]
    finally:
        stream.close()
    if last not in (b"", b"\n"):
        lines += 1
    if problem is None:
        try:
            decoder.decode(b"", final=True)
        except UnicodeDecodeError:
            problem = f"not UTF-8 (line {lines})"
    return Scan(digest.hexdigest(), max(lines - is_csv, 0), problem)


def check_scans(files: Sequence[LandedFile], scans: Mapping[str, Scan]) -> None:
    """``LandingError`` naming each file whose bytes are not those of its manifest entry, or
    not text that the readers keep intact."""
    problems = []
    for file in files:
        found = scans[file.key]
        if found.sha256 != file.manifest_sha256:
            expected = file.manifest_sha256 or "missing"
            problems.append(f"{file.key}: sha256 {found.sha256}, manifest {expected}")
        if found.problem:
            problems.append(f"{file.key}: {found.problem}")
    if problems:
        raise LandingError("; ".join(problems))


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


def _text_problem(decoder: Any, chunk: bytes, lines: int) -> str | None:
    """Why ``chunk``, which starts after ``lines`` line breaks, is not UTF-8, if it is not."""
    pending = len(decoder.getstate()[0])
    try:
        decoder.decode(chunk)
    except UnicodeDecodeError as exc:
        line = lines + chunk.count(b"\n", 0, max(exc.start - pending, 0)) + 1
        return f"not UTF-8 (line {line})"
    return None


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
