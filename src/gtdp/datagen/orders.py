"""Orders: J1 history (kit transactions verbatim, then synthetic rows), J2 and J3 daily drops."""

import random
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import chain
from pathlib import Path

from gtdp.datagen.engine import (
    Anomaly,
    RowSource,
    WrittenFile,
    anomaly_counts,
    file_rng,
    write_source_file,
)
from gtdp.datagen.model import HISTORY_SECONDS, Drop, Profile, history_stamp, iso_stamp
from gtdp.datagen.world import HOT_PRODUCT, World

HEADER = "transaction_id,event_time,customer_id,product_id,quantity,unit_price_eur,channel,status"
DISCOUNT_HEADER = f"{HEADER},discount_pct"
TIME, CUSTOMER, PRODUCT, QUANTITY, PRICE, CHANNEL, STATUS = range(1, 8)
CHANNELS = ("web", "store", "partner")
HOT_SHARE = 0.20
MAX_COMMERCIAL_DISCOUNT = 0.10
LATE_SHARE = 0.03
FIRST_SYNTHETIC_ID = 200_001


@dataclass
class OrdersHistory:
    """What later drops need from earlier ones: the next free id and resendable clean rows."""

    next_id: int = FIRST_SYNTHETIC_ID
    resendable: list[str] = field(default_factory=list)


class Orders(RowSource):
    def __init__(
        self,
        rng: random.Random,
        world: World,
        drop: Drop,
        kit_rows: list[str],
        history: OrdersHistory,
        n_clean: int,
        n_resend: int,
    ) -> None:
        super().__init__(rng)
        self.drop = drop
        self.next_id = history.next_id
        self._random = rng.random
        self._catalog = world.catalog
        self._customers = world.customer_ids
        self._products = world.product_ids
        self._suffix = "," if drop.with_discount else ""
        self._resend = rng.sample(history.resendable, n_resend)
        self.clean_lines: list[str] = []
        if drop.is_initial:
            self._clean = chain(kit_rows, self._history_rows())
        else:
            late = set(rng.sample(range(n_clean), max(1, round(LATE_SHARE * n_clean))))
            self._clean = self._daily_rows(late)

    def next_clean(self) -> str:
        return next(self._clean)

    def remember(self, line: str) -> None:
        super().remember(line)
        if not self.drop.is_initial:
            self.clean_lines.append(line)

    def synthetic(self, timestamp: str) -> list[str]:
        r = self._random
        product = HOT_PRODUCT if r() < HOT_SHARE else self._products[int(r() * len(self._products))]
        price = self._catalog.price_at(product, timestamp) * (1 - MAX_COMMERCIAL_DISCOUNT * r())
        draw = r()
        status = "COMPLETED" if draw < 0.94 else "CANCELLED" if draw < 0.97 else "RETURNED"
        row = [
            f"T{self.next_id:09d}",
            timestamp,
            self._customers[int(r() * len(self._customers))],
            product,
            str(1 + int(r() * 12)),
            f"{price:.2f}",
            CHANNELS[int(r() * 3)],
            status,
        ]
        self.next_id += 1
        if self.drop.with_discount:
            row.append("0.000" if r() < 0.5 else f"{0.01 + 0.29 * r():.3f}")
        return row

    def fresh(self) -> list[str]:
        """A synthetic row of this drop's activity, for an injector to damage."""
        if self.drop.is_initial:
            return self.synthetic(history_stamp(int(self._random() * HISTORY_SECONDS)))
        return self.synthetic(iso_stamp(self.drop.business_day, self.rng.randrange(86_400)))

    def _history_rows(self) -> Iterator[str]:
        while True:
            yield ",".join(self.fresh())

    def _daily_rows(self, late: set[int]) -> Iterator[str]:
        index = 0
        while True:
            day = self.drop.business_day
            if index in late:
                day -= timedelta(days=self.rng.randint(1, 3))
            yield ",".join(self.synthetic(iso_stamp(day, self.rng.randrange(86_400))))
            index += 1

    def mutated(self, column: int, value: str) -> str:
        row = self.fresh()
        row[column] = value
        return ",".join(row)

    def resend(self) -> str:
        return self._resend.pop() + self._suffix


def _bad_timestamp(src: Orders) -> str:
    row = src.fresh()
    stamp = row[TIME]
    row[TIME] = src.rng.choice(
        (f"{stamp[:4]}/{stamp[5:7]}/{stamp[8:10]} {stamp[11:16]}", "N/A", "")
    )
    return ",".join(row)


def _future_timestamp(src: Orders) -> str:
    day = src.drop.ingestion_date + timedelta(days=src.rng.randint(2, 30))
    return src.mutated(TIME, iso_stamp(day, src.rng.randrange(86_400)))


def _bad_enum(src: Orders) -> str:
    if src.rng.random() < 0.5:
        return src.mutated(CHANNEL, "fax")
    return src.mutated(STATUS, "UNKNOWN")


def _malformed(src: Orders) -> str:
    row = src.fresh()
    variant = src.rng.randrange(3)
    if variant == 0:
        return ",".join(row[:-1])
    if variant == 1:
        return ",".join([*row, "1"])
    return ";".join(row)


ANOMALIES = (
    Anomaly("DUPLICATE_EXACT", 0.01, Orders.recent),
    Anomaly("DUPLICATE_CROSS_DAY", 0.005, Orders.resend, needs_history=True),
    Anomaly("NULL_CUSTOMER", 0.005, lambda s: s.mutated(CUSTOMER, "")),
    Anomaly(
        "UNKNOWN_CUSTOMER",
        0.003,
        lambda s: s.mutated(CUSTOMER, f"C999{s.rng.randrange(1000):03d}"),
    ),
    Anomaly(
        "UNKNOWN_PRODUCT", 0.003, lambda s: s.mutated(PRODUCT, f"P9{s.rng.randrange(10000):04d}")
    ),
    Anomaly(
        "BAD_QUANTITY", 0.005, lambda s: s.mutated(QUANTITY, s.rng.choice(("0", "-3", "abc", "")))
    ),
    Anomaly("BAD_PRICE", 0.002, lambda s: s.mutated(PRICE, s.rng.choice(("-12.50", "0", "")))),
    Anomaly("BAD_TIMESTAMP", 0.003, _bad_timestamp),
    Anomaly("FUTURE_TIMESTAMP", 0.001, _future_timestamp),
    Anomaly("BAD_ENUM", 0.002, _bad_enum),
    Anomaly("MALFORMED_ROW", 0.001, _malformed),
)


def write_orders(
    landing: Path, seed: int, profile: Profile, drop: Drop, world: World, history: OrdersHistory
) -> WrittenFile:
    relative = f"orders/orders_{drop.tag}.csv"
    n_rows = profile.orders_j1 if drop.is_initial else profile.orders_daily
    counts = anomaly_counts(ANOMALIES, n_rows, has_history=not drop.is_initial)
    kit_rows = world.kit.transactions[: profile.kit_orders] if drop.is_initial else []
    source = Orders(
        file_rng(seed, relative),
        world,
        drop,
        kit_rows,
        history,
        n_clean=n_rows - sum(counts.values()),
        n_resend=counts.get("DUPLICATE_CROSS_DAY", 0),
    )
    written = write_source_file(
        landing,
        relative,
        "orders",
        source,
        ANOMALIES,
        counts,
        n_rows,
        DISCOUNT_HEADER if drop.with_discount else HEADER,
    )
    history.next_id = source.next_id
    history.resendable += kit_rows if drop.is_initial else source.clean_lines
    return written
