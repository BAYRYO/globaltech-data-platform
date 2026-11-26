"""Hand-made silver tables, written where the gold job reads them, and the gold job run on them."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

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
EPOCH, OPEN = datetime(1900, 1, 1), datetime(9999, 12, 31)


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
) -> tuple:
    """A ``silver/orders`` row."""
    return (
        transaction_id,
        datetime.fromisoformat(event_time),
        customer,
        product,
        quantity,
        Decimal(price),
        channel,
        status,
        Decimal(discount),
        f"h-{transaction_id}",
        day,
    )


def customer(
    customer_id: str,
    *,
    country: str = "FR",
    segment: str = "B2C",
    signup: str = "2024-01-01",
) -> tuple:
    """A ``silver/customers`` row."""
    return (
        customer_id,
        f"hash-{customer_id}",
        f"{country}-IDF",
        country,
        segment,
        date.fromisoformat(signup),
        datetime(2025, 12, 31),
        [],
        f"h-{customer_id}",
        J1,
    )


def versions(product_id: str, *changes: tuple[str, ...], category: str = "Lighting") -> list:
    """``silver/products_scd2`` rows of ``product_id``: ``(valid_from, price[, category])`` per
    version, ``valid_to`` the next version's ``valid_from``; the first ``valid_from`` is usually
    1900."""
    starts = [datetime.fromisoformat(change[0]) for change in changes]
    ends = [*starts[1:], OPEN]
    return [
        (
            f"{product_id}-{start:%Y%m%d%H%M%S}",
            product_id,
            f"{category} {product_id}",
            change[2] if len(change) > 2 else category,
            Decimal(change[1]),
            start,
            end,
            end == OPEN,
            number,
            f"h-{product_id}-{number}",
            J1,
        )
        for number, (start, end, change) in enumerate(
            zip(starts, ends, changes, strict=False), start=1
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
) -> tuple:
    """A ``silver/weblogs`` row."""
    return (
        log_id,
        datetime.fromisoformat(event_ts),
        "ip-hash",
        "198.18.0.0/24",
        customer_id,
        "GET",
        "/",
        "HTTP/1.1",
        status,
        100,
        None,
        f"Mozilla/5.0 ({device})",
        device,
        event_type,
        product,
        day,
    )


def reject(source: str, day: date, *reasons: str) -> tuple:
    """A ``silver/rejects/<source>`` row."""
    return (source, list(reasons), "raw,line", f"h-{reasons}", "file.csv", "run", day)


@dataclass
class SilverLake:
    spark: SparkSession
    cfg: AppConfig

    def _frame(self, source: str, rows: list) -> DataFrame:
        return self.spark.createDataFrame(rows, PIPELINES[source].schema)

    def orders(self, day: date, *rows: tuple) -> None:
        path = silver_path(self.cfg.paths.silver, "orders")
        write_partitions(self._frame("orders", list(rows)), path, {INGESTION_DATE: [day]})

    def weblogs(self, day: date, *rows: tuple) -> None:
        path = silver_path(self.cfg.paths.silver, "weblogs")
        write_partitions(self._frame("weblogs", list(rows)), path, {INGESTION_DATE: [day]})

    def customers(self, *rows: tuple) -> None:
        path = silver_path(self.cfg.paths.silver, "customers")
        replace_table(self._frame("customers", list(rows)), path)

    def products(self, *rows: tuple) -> None:
        path = silver_path(self.cfg.paths.silver, "products")
        replace_table(self._frame("products", list(rows)), path)

    def rejects(self, source: str, day: date, *rows: tuple) -> None:
        frame = self.spark.createDataFrame(list(rows), SILVER_REJECTS)
        write_partitions(
            frame, rejects_path(self.cfg.paths.silver, source), {INGESTION_DATE: [day]}
        )

    def run_gold(self, day: date) -> Run:
        spec = JOBS["gold"]
        with audited_run(self.spark, self.cfg, spec.name, spec.layer, None, day) as run:
            spec.func(JobContext(self.spark, self.cfg, run))
        return run

    def gold(self, name: str, schema: StructType | None = None) -> DataFrame:
        return read_table(self.spark, table_path(self.cfg.paths.gold, name), schema)
