"""Hand-made silver tables, written where the gold job reads them, and the gold job run on them.

Rows are dicts placed into the silver schemas of ``gtdp.schemas`` by column name: a column a row
does not name is null, so a column added to or dropped from silver leaves these helpers valid.
"""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType

from gtdp.audit import Run, audited_run
from gtdp.config import AppConfig
from gtdp.gold.tables import table_path
from gtdp.io import INGESTION_DATE, read_table, replace_table, write_partitions
from gtdp.jobs.catalog import JOBS
from gtdp.jobs.registry import JobContext
from gtdp.schemas import SILVER_REJECTS
from gtdp.silver.pipelines import PIPELINES, rejects_path
from gtdp.silver.pipelines import table_path as silver_path

J1, J2, J3 = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)
OPEN = datetime(9999, 12, 31)


def frame(spark: SparkSession, schema: StructType, rows: list[dict]) -> DataFrame:
    """``rows`` in the columns of ``schema``, by name, every column nullable."""
    nullable = StructType([StructField(f.name, f.dataType) for f in schema.fields])
    values = [tuple(row.get(field.name) for field in schema.fields) for row in rows]
    return spark.createDataFrame(values, nullable)


def order(
    transaction_id: str,
    event_time: str,
    *,
    day: date = J1,
    product: str = "P1",
    customer: str = "C1",
    quantity: int = 1,
    price: str = "10.00",
    discount: str = "0",
    status: str = "COMPLETED",
    channel: str = "web",
) -> dict:
    """A ``silver/orders`` row."""
    return {
        "transaction_id": transaction_id,
        "event_time": datetime.fromisoformat(event_time),
        "customer_id": customer,
        "product_id": product,
        "quantity": quantity,
        "unit_price_eur": Decimal(price),
        "channel": channel,
        "status": status,
        "discount_pct": Decimal(discount),
        INGESTION_DATE: day,
    }


def customer(
    customer_id: str, *, country: str = "FR", segment: str = "B2C", signup: str = "2024-01-01"
) -> dict:
    """A ``silver/customers`` row."""
    return {
        "customer_id": customer_id,
        "email_hash": f"hash-{customer_id}",
        "region": f"{country}-IDF",
        "country": country,
        "segment": segment,
        "signup_date": date.fromisoformat(signup),
        "updated_at": datetime(2025, 12, 31),
        "quality_flags": [],
        INGESTION_DATE: J1,
    }


def versions(product_id: str, *changes: tuple[str, ...], category: str = "Lighting") -> list:
    """``silver/products_scd2`` rows of ``product_id``: ``(valid_from, price[, category])`` per
    version, ``valid_to`` the next version's ``valid_from``; the first ``valid_from`` is usually
    1900."""
    starts = [datetime.fromisoformat(change[0]) for change in changes]
    ends = [*starts[1:], OPEN]
    return [
        {
            "product_sk": f"{product_id}-{start:%Y%m%d%H%M%S}",
            "product_id": product_id,
            "product_name": f"{category} {product_id}",
            "category": change[2] if len(change) > 2 else category,
            "unit_price_eur": Decimal(change[1]),
            "valid_from": start,
            "valid_to": end,
            "is_current": end == OPEN,
            "version": number,
            INGESTION_DATE: J1,
        }
        for number, (start, end, change) in enumerate(
            zip(starts, ends, changes, strict=True), start=1
        )
    ]


def request(
    log_id: str,
    event_ts: str,
    event_type: str,
    *,
    status: int = 200,
    device: str = "mobile",
    product: str | None = None,
    customer_id: str | None = "C1",
    day: date = J1,
) -> dict:
    """A ``silver/weblogs`` row."""
    return {
        "log_id": log_id,
        "event_ts": datetime.fromisoformat(event_ts),
        "customer_id": customer_id,
        "http_status": status,
        "device": device,
        "event_type": event_type,
        "product_id": product,
        INGESTION_DATE: day,
    }


def reject(source: str, day: date, *reasons: str) -> dict:
    """A ``silver/rejects/<source>`` row."""
    return {"source": source, "reject_reasons": list(reasons), INGESTION_DATE: day}


@dataclass
class SilverLake:
    spark: SparkSession
    cfg: AppConfig

    def _write(self, source: str, rows: tuple[dict, ...], day: date | None = None) -> None:
        path = silver_path(self.cfg.paths.silver, source)
        rows_frame = frame(self.spark, PIPELINES[source].schema, list(rows))
        if day is None:
            replace_table(rows_frame, path)
        else:
            write_partitions(rows_frame, path, {INGESTION_DATE: [day]})

    def orders(self, day: date, *rows: dict) -> None:
        self._write("orders", rows, day)

    def weblogs(self, day: date, *rows: dict) -> None:
        self._write("weblogs", rows, day)

    def customers(self, *rows: dict) -> None:
        self._write("customers", rows)

    def products(self, *rows: dict) -> None:
        self._write("products", rows)

    def rejects(self, source: str, day: date, *rows: dict) -> None:
        rows_frame = frame(self.spark, SILVER_REJECTS, list(rows))
        write_partitions(
            rows_frame, rejects_path(self.cfg.paths.silver, source), {INGESTION_DATE: [day]}
        )

    def run_gold(self, day: date) -> Run:
        spec = JOBS["gold"]
        with audited_run(self.spark, self.cfg, spec.name, spec.layer, None, day) as run:
            spec.func(JobContext(self.spark, self.cfg, run))
        return run

    def gold(self, name: str, schema: StructType | None = None) -> DataFrame:
        return read_table(self.spark, table_path(self.cfg.paths.gold, name), schema)
