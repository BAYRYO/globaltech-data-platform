"""Customers: kit customers enriched with synthetic personal data, then daily new and updated rows.

Silver keeps damaged customer rows with a quality flag, so damage applies to the planned rows
themselves; only the duplicates add rows.
"""

import random
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from gtdp.datagen.engine import (
    Anomaly,
    RowSource,
    WrittenFile,
    anomaly_counts,
    file_rng,
    write_source_file,
)
from gtdp.datagen.model import Drop, iso_stamp
from gtdp.datagen.world import CUSTOMERS_J1_UPDATED_AT, World, make_person

HEADER = "customer_id,full_name,email,phone,region,segment,signup_date,updated_at"
EMAIL, REGION, SEGMENT, SIGNUP, UPDATED = 2, 4, 5, 6, 7
SEGMENTS = ("SMB", "MID", "ENTERPRISE")
NEW_CUSTOMERS_PER_DAY = 12
UPDATED_CUSTOMERS_PER_DAY = 30
FIRST_NEW_CUSTOMER = 5001


@dataclass
class CustomersHistory:
    next_number: int = FIRST_NEW_CUSTOMER
    latest: dict[str, list[str]] = field(default_factory=dict)


class Customers(RowSource):
    def __init__(
        self, rng: random.Random, world: World, drop: Drop, history: CustomersHistory
    ) -> None:
        super().__init__(rng)
        self.drop = drop
        self.history = history
        self._regions = sorted({row[1] for row in world.kit.customers})
        if drop.is_initial:
            self.planned_rows = world.customers
        else:
            self.planned_rows = self._delta(world.customer_ids)
        self._planned = iter(self.planned_rows)
        self._key_duplicates: set[str] = set()

    def next_clean(self) -> str:
        return ",".join(next(self._planned))

    def _stamp(self) -> str:
        if self.drop.is_initial:
            return CUSTOMERS_J1_UPDATED_AT
        return iso_stamp(self.drop.business_day, self.rng.randrange(86_400))

    def _new_customer(self) -> list[str]:
        rng = self.rng
        number = self.history.next_number
        self.history.next_number += 1
        region = rng.choice(self._regions)
        full_name, email, phone = make_person(rng, region.split("-", 1)[0], number)
        return [
            f"C{number:06d}",
            full_name,
            email,
            phone,
            region,
            rng.choice(SEGMENTS),
            self.drop.business_day.isoformat(),
            self._stamp(),
        ]

    def _delta(self, kit_ids: list[str]) -> list[list[str]]:
        rows = []
        for customer_id in self.rng.sample(kit_ids, UPDATED_CUSTOMERS_PER_DAY):
            row = list(self.history.latest[customer_id])
            if self.rng.random() < 0.5:
                row[REGION] = self.rng.choice([r for r in self._regions if r != row[REGION]])
            else:
                row[SEGMENT] = self.rng.choice([s for s in SEGMENTS if s != row[SEGMENT]])
            row[UPDATED] = self._stamp()
            rows.append(row)
        rows += [self._new_customer() for _ in range(NEW_CUSTOMERS_PER_DAY)]
        return sorted(rows, key=lambda row: row[UPDATED])

    def key_duplicate(self) -> str:
        """A valid row restated with its email in another case or padded with spaces."""
        while True:
            row = self.recent().split(",")
            email = row[EMAIL]
            variants = (email.upper(), email.capitalize(), f" {email}", f"{email} ")
            row[EMAIL] = self.rng.choice(variants)
            line = ",".join(row)
            if line not in self._key_duplicates:
                self._key_duplicates.add(line)
                return line

    def damaged(self, column: int, value: str | None = None) -> str:
        """The next planned row with `column` emptied (`value`) or garbled."""
        row = list(next(self._planned))
        row[column] = _garbled(self.rng, column, row[column]) if value is None else value
        return ",".join(row)


def _garbled(rng: random.Random, column: int, value: str) -> str:
    if column == EMAIL:
        local = value.split("@")[0]
        return rng.choice(
            (
                value.replace("@", ""),
                value.replace("@", "@@"),
                f"{local}@",
                value.replace(".", " ", 1),
            )
        )
    day = date.fromisoformat(value)
    return f"{day.day:02d}/{day.month:02d}/{day.year}"


ANOMALIES = (
    Anomaly("DUPLICATE_EXACT", 0.01, Customers.recent),
    Anomaly("DUPLICATE_KEY", 0.005, Customers.key_duplicate),
    Anomaly("MISSING_EMAIL", 0.02, lambda s: s.damaged(EMAIL, "")),
    Anomaly("BAD_EMAIL", 0.005, lambda s: s.damaged(EMAIL)),
    Anomaly("MISSING_REGION", 0.01, lambda s: s.damaged(REGION, "")),
    Anomaly("BAD_DATE", 0.005, lambda s: s.damaged(SIGNUP)),
)


def write_customers(
    landing: Path, seed: int, drop: Drop, world: World, history: CustomersHistory
) -> WrittenFile:
    relative = f"customers/customers_{drop.tag}.csv"
    source = Customers(file_rng(seed, relative), world, drop, history)
    n_planned = len(source.planned_rows)
    counts = anomaly_counts(ANOMALIES, n_planned, has_history=not drop.is_initial)
    n_rows = n_planned + counts["DUPLICATE_EXACT"] + counts["DUPLICATE_KEY"]
    written = write_source_file(
        landing, relative, "customers", source, ANOMALIES, counts, n_rows, HEADER
    )
    history.latest.update((row[0], row) for row in source.planned_rows)
    return written
