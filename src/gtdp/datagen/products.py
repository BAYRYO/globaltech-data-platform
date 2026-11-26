"""Products: J1 version history of the kit catalog, then daily deltas.

Anomalies never touch a product's first version. BAD_PRICE and NULL_CATEGORY rows are extra
versions at timestamps no valid version uses; a CONFLICTING_VERSION row restates the key of a
valid later version with another price.
"""

import random
from dataclasses import replace
from pathlib import Path

from gtdp.datagen.engine import (
    Anomaly,
    RowSource,
    WrittenFile,
    anomaly_counts,
    file_rng,
    write_source_file,
)
from gtdp.datagen.model import HISTORY_SECONDS, Drop, history_stamp, iso_stamp
from gtdp.datagen.world import INITIAL_VERSION_AT, World, shifted_price

HEADER = "product_id,product_name,category,unit_price_eur,updated_at"
CATEGORY, PRICE = 2, 3


class Products(RowSource):
    def __init__(self, rng: random.Random, world: World, drop: Drop, n_conflicts: int) -> None:
        super().__init__(rng)
        self.drop = drop
        self.valid_rows = world.product_drops[drop.index]
        self._catalog = world.catalog
        self._product_ids = world.product_ids
        self._taken = {(v.product_id, v.updated_at) for v in self.valid_rows}
        self._clean = iter([v.line() for v in self.valid_rows])
        kit_ids = set(self._product_ids)
        later_versions = [
            v
            for v in self.valid_rows
            if v.product_id in kit_ids and v.updated_at != INITIAL_VERSION_AT
        ]
        self._conflicts = rng.sample(later_versions, n_conflicts)
        self._conflicting_lines = {v.line() for v in self._conflicts}

    def next_clean(self) -> str:
        return next(self._clean)

    def remember(self, line: str) -> None:
        if line not in self._conflicting_lines:
            super().remember(line)

    def noise(self) -> list[str]:
        """The valid attributes of a kit product, restated at a timestamp no version uses."""
        while True:
            product_id = self.rng.choice(self._product_ids)
            if self.drop.is_initial:
                stamp = history_stamp(self.rng.randrange(HISTORY_SECONDS))
            else:
                stamp = iso_stamp(self.drop.business_day, self.rng.randrange(86_400))
            if (product_id, stamp) not in self._taken:
                self._taken.add((product_id, stamp))
                version = self._catalog.version_at(product_id, stamp)
                return [product_id, version.name, version.category, version.price, stamp]

    def damaged(self, column: int, value: str) -> str:
        row = self.noise()
        row[column] = value
        return ",".join(row)

    def conflicting(self) -> str:
        version = self._conflicts.pop()
        return replace(version, price=shifted_price(self.rng, version.price)).line()


ANOMALIES = (
    Anomaly("DUPLICATE_EXACT", 0.01, Products.recent),
    Anomaly(
        "BAD_PRICE", 0.005, lambda s: s.damaged(PRICE, s.rng.choice(("-12.50", "0", "", "abc")))
    ),
    Anomaly("NULL_CATEGORY", 0.005, lambda s: s.damaged(CATEGORY, "")),
    Anomaly("CONFLICTING_VERSION", 0.002, Products.conflicting),
)


def write_products(landing: Path, seed: int, drop: Drop, world: World) -> WrittenFile:
    relative = f"products/products_{drop.tag}.csv"
    n_valid = len(world.product_drops[drop.index])
    counts = anomaly_counts(ANOMALIES, n_valid, has_history=not drop.is_initial)
    source = Products(file_rng(seed, relative), world, drop, counts["CONFLICTING_VERSION"])
    return write_source_file(
        landing,
        relative,
        "products",
        source,
        ANOMALIES,
        counts,
        n_valid + sum(counts.values()),
        HEADER,
    )
