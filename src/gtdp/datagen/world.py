"""Reference world shared by every file: the kit, the product history and customer identities.

It is derived from the kit and the seed alone, so every file of a run sees the same catalog
prices, product names and customer details whatever order the files are written in.
"""

import csv
import gzip
import io
import json
import random
import re
import unicodedata
from bisect import bisect_right
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

from gtdp.datagen.model import DROPS, HISTORY_SECONDS, HISTORY_START, history_stamp, iso_stamp

HOT_PRODUCT = "P00042"
INITIAL_VERSION_AT = "2024-12-31T00:00:00"
CUSTOMERS_J1_UPDATED_AT = "2025-12-31T00:00:00"
VERSIONED_SHARE = 0.15
CATEGORY_CHANGE_SHARE = 0.03
MODIFIED_PRODUCTS_PER_DAY = 10
NEW_PRODUCTS_PER_DAY = 3
FIRST_NEW_PRODUCT = 1001

COUNTRIES = ("FR", "DE", "ES", "IT", "UK")
FIRST_NAMES = {
    "FR": ("Jean", "Marie", "Hélène", "François", "Camille", "Léa", "Nicolas", "Chloé", "Théo"),
    "DE": ("Jürgen", "Anna", "Lukas", "Sophie", "Maximilian", "Lena", "Felix", "Jonas", "Laura"),
    "ES": ("José", "María", "Lucía", "Javier", "Carmen", "Pablo", "Sofía", "Alejandro", "Elena"),
    "IT": ("Giuseppe", "Giulia", "Luca", "Francesca", "Marco", "Chiara", "Niccolò", "Matteo"),
    "UK": ("Oliver", "Amelia", "George", "Isla", "Harry", "Ava", "Jack", "Emily", "Noah"),
}
LAST_NAMES = {
    "FR": ("Martin", "Bernard", "Dubois", "Thomas", "Robert", "Petit", "Durand", "Lefèvre"),
    "DE": ("Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker"),
    "ES": ("García", "Fernández", "González", "Rodríguez", "López", "Martínez", "Sánchez"),
    "IT": ("Rossi", "Russo", "Ferrari", "Esposito", "Bianchi", "Romano", "Colombo", "Ricci"),
    "UK": ("Smith", "Jones", "Taylor", "Brown", "Williams", "Wilson", "O'Brien", "Evans"),
}
PHONE_FORMATS = {
    "FR": ("+336", 8),
    "DE": ("+4915", 9),
    "ES": ("+346", 8),
    "IT": ("+393", 9),
    "UK": ("+447700900", 3),
}
EMAIL_DOMAINS = ("example.com", "example.org", "example.net")


@dataclass(frozen=True)
class KitEvent:
    offset: int
    """Seconds since 2025-01-01T00:00:00."""
    customer_id: str
    event_type: str
    device: str


@dataclass(frozen=True)
class Kit:
    transactions: list[str]
    """Data lines of transactions.csv, verbatim apart from the CRLF line ending."""
    customers: list[list[str]]
    """customer_id, region, segment, signup_date."""
    products: list[list[str]]
    """product_id, category, unit_price_eur."""
    events: list[KitEvent]


def _read_csv(path: Path) -> list[list[str]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        return list(csv.reader(handle))[1:]


def _event_offset(event_time: str) -> int:
    day = (date.fromisoformat(event_time[:10]) - HISTORY_START).days
    hours, minutes, seconds = (int(part) for part in event_time[11:19].split(":"))
    return day * 86_400 + hours * 3600 + minutes * 60 + seconds


def load_kit(directory: Path) -> Kit:
    with gzip.open(directory / "transactions.csv.gz", "rb") as handle:
        transactions = io.TextIOWrapper(handle, encoding="utf-8").read().splitlines()[1:]
    with gzip.open(directory / "events.jsonl.gz", "rt", encoding="utf-8") as handle:
        events = [
            KitEvent(_event_offset(e["event_time"]), e["customer_id"], e["event_type"], e["device"])
            for e in map(json.loads, handle)
        ]
    return Kit(
        transactions=transactions,
        customers=_read_csv(directory / "customers.csv.gz"),
        products=_read_csv(directory / "products.csv.gz"),
        events=events,
    )


@dataclass(frozen=True)
class ProductVersion:
    product_id: str
    name: str
    category: str
    price: str
    updated_at: str

    def line(self) -> str:
        return f"{self.product_id},{self.name},{self.category},{self.price},{self.updated_at}"


class Catalog:
    """Valid product versions of all drops, queried by the time an order or a log happens."""

    def __init__(self, versions: list[ProductVersion]) -> None:
        self._history: dict[str, list[ProductVersion]] = {}
        for version in sorted(versions, key=lambda v: (v.product_id, v.updated_at)):
            self._history.setdefault(version.product_id, []).append(version)
        self._stamps = {pid: [v.updated_at for v in hist] for pid, hist in self._history.items()}
        self._prices = {pid: [float(v.price) for v in hist] for pid, hist in self._history.items()}
        self._fixed_prices = {
            pid: prices[0] for pid, prices in self._prices.items() if len(prices) == 1
        }

    def version_at(self, product_id: str, timestamp: str) -> ProductVersion:
        index = bisect_right(self._stamps[product_id], timestamp) - 1
        return self._history[product_id][index]

    def price_at(self, product_id: str, timestamp: str) -> float:
        fixed = self._fixed_prices.get(product_id)
        if fixed is not None:
            return fixed
        return self._prices[product_id][bisect_right(self._stamps[product_id], timestamp) - 1]


@dataclass(frozen=True)
class World:
    kit: Kit
    product_drops: tuple[list[ProductVersion], ...]
    """Valid product rows of each drop: J1 history, then the J2 and J3 deltas."""
    catalog: Catalog
    customers: list[list[str]]
    """Kit customers in the J1 column order, enriched with synthetic personal data."""
    ips: dict[str, str]

    @property
    def customer_ids(self) -> list[str]:
        return [row[0] for row in self.kit.customers]

    @property
    def product_ids(self) -> list[str]:
        return [row[0] for row in self.kit.products]


def ascii_slug(text: str) -> str:
    folded = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", folded.lower())


def make_person(rng: random.Random, country: str, number: int) -> tuple[str, str, str]:
    """Synthetic (full_name, email, phone); the number keeps emails unique."""
    first = rng.choice(FIRST_NAMES[country])
    last = rng.choice(LAST_NAMES[country])
    email = f"{ascii_slug(first)}.{ascii_slug(last)}{number}@{rng.choice(EMAIL_DOMAINS)}"
    prefix, digits = PHONE_FORMATS[country]
    phone = prefix + "".join(str(rng.randrange(10)) for _ in range(digits))
    return f"{first} {last}", email, phone


def shifted_price(rng: random.Random, price: str) -> str:
    """Price moved up or down by 5 to 20 %."""
    factor = 1 + rng.choice((-1, 1)) * rng.uniform(0.05, 0.20)
    return f"{float(price) * factor:.2f}"


def _other_category(rng: random.Random, categories: list[str], category: str) -> str:
    return rng.choice([c for c in categories if c != category])


def _history_versions(rng: random.Random, kit: Kit, categories: list[str]) -> list[ProductVersion]:
    product_ids = [row[0] for row in kit.products]
    n_versioned = round(VERSIONED_SHARE * len(product_ids))
    others = [pid for pid in product_ids if pid != HOT_PRODUCT]
    versioned = {HOT_PRODUCT, *rng.sample(others, n_versioned - 1)}
    category_changes = set(
        rng.sample(sorted(versioned), round(CATEGORY_CHANGE_SHARE * len(product_ids)))
    )
    rows = []
    for product_id, category, price in kit.products:
        current = ProductVersion(
            product_id, f"{category} {product_id}", category, price, INITIAL_VERSION_AT
        )
        rows.append(current)
        if product_id not in versioned:
            continue
        offsets = sorted(rng.sample(range(HISTORY_SECONDS), rng.randint(1, 3)))
        change_at = rng.randrange(len(offsets)) if product_id in category_changes else -1
        for index, offset in enumerate(offsets):
            current = replace(
                current,
                category=_other_category(rng, categories, current.category)
                if index == change_at
                else current.category,
                price=shifted_price(rng, current.price),
                updated_at=history_stamp(offset),
            )
            rows.append(current)
    return rows


def _daily_deltas(
    rng: random.Random,
    history: list[ProductVersion],
    product_ids: list[str],
    categories: list[str],
) -> list[list[ProductVersion]]:
    latest = {version.product_id: version for version in history}
    next_number = FIRST_NEW_PRODUCT
    deltas = []
    for drop in DROPS[1:]:
        day = drop.business_day
        rows = []
        for product_id in rng.sample(product_ids, MODIFIED_PRODUCTS_PER_DAY):
            current = latest[product_id]
            category = current.category
            if rng.random() < CATEGORY_CHANGE_SHARE:
                category = _other_category(rng, categories, category)
            rows.append(
                replace(
                    current,
                    category=category,
                    price=shifted_price(rng, current.price),
                    updated_at=iso_stamp(day, rng.randrange(86_400)),
                )
            )
        for _ in range(NEW_PRODUCTS_PER_DAY):
            product_id = f"P{next_number:05d}"
            next_number += 1
            category = rng.choice(categories)
            rows.append(
                ProductVersion(
                    product_id,
                    f"{category} {product_id}",
                    category,
                    f"{rng.uniform(5, 1500):.2f}",
                    iso_stamp(day, rng.randrange(86_400)),
                )
            )
        rows.sort(key=lambda v: (v.product_id, v.updated_at))
        latest.update((v.product_id, v) for v in rows)
        deltas.append(rows)
    return deltas


def build_world(kit: Kit, seed: int) -> World:
    rng = random.Random(f"{seed}/world")
    categories = sorted({row[1] for row in kit.products})
    history = _history_versions(rng, kit, categories)
    deltas = _daily_deltas(rng, history, [row[0] for row in kit.products], categories)
    customers = []
    ips = {}
    for customer_id, region, segment, signup_date in kit.customers:
        country = region.split("-", 1)[0]
        full_name, email, phone = make_person(rng, country, int(customer_id[1:]))
        customers.append(
            [
                customer_id,
                full_name,
                email,
                phone,
                region,
                segment,
                signup_date,
                CUSTOMERS_J1_UPDATED_AT,
            ]
        )
        subnet = COUNTRIES.index(country) * 50 + rng.randrange(50)
        ips[customer_id] = f"198.18.{subnet}.{rng.randrange(1, 255)}"
    return World(
        kit=kit,
        product_drops=(history, *deltas),
        catalog=Catalog([v for drop in (history, *deltas) for v in drop]),
        customers=customers,
        ips=ips,
    )
