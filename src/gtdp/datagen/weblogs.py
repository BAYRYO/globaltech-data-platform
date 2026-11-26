"""Web logs in Combined Log Format, one request per kit event."""

import random
from datetime import date, timedelta
from pathlib import Path

from gtdp.datagen.engine import (
    Anomaly,
    RowSource,
    WrittenFile,
    anomaly_counts,
    file_rng,
    write_source_file,
)
from gtdp.datagen.model import HISTORY_SECONDS, HISTORY_START, Drop, Profile
from gtdp.datagen.world import HOT_PRODUCT, KitEvent, World

MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
SEARCH_TERMS = (
    "led+panel", "floodlight", "circuit+breaker", "contactor", "cable", "plc", "servo+drive",
    "hmi", "ip+camera", "badge+reader", "motion+detector", "managed+switch", "router",
    "patch+panel", "access+point",
)  # fmt: skip
HOT_SHARE = 0.20
ERROR_PATHS = ("/products/P99999", "/old-catalog")
FREE_TEXT = (
    "upstream timed out (110: Connection timed out) while reading response header",
    "client closed connection while waiting for request",
    "worker process exited on signal 9",
)
_DAYS = [
    f"{d.day:02d}/{MONTHS[d.month - 1]}/{d.year}"
    for d in (HISTORY_START + timedelta(days=n) for n in range(400))
]


class Weblogs(RowSource):
    def __init__(self, rng: random.Random, world: World, events: list[KitEvent]) -> None:
        super().__init__(rng)
        self._ips = world.ips
        self._products = world.product_ids
        self._events = iter(events)

    def next_clean(self) -> str:
        return self.line()

    def _product(self) -> str:
        if self.rng.random() < HOT_SHARE:
            return HOT_PRODUCT
        return self.rng.choice(self._products)

    def _request(self, event_type: str) -> str:
        if event_type == "view":
            return f"GET /products/{self._product()} HTTP/1.1"
        if event_type == "search":
            return f"GET /search?q={self.rng.choice(SEARCH_TERMS)} HTTP/1.1"
        if event_type == "cart":
            return f"POST /cart/add?product={self._product()} HTTP/1.1"
        return "POST /checkout HTTP/1.1"

    def line(self, request: str | None = None, status: int = 200) -> str:
        event = next(self._events)
        if request is None:
            request = self._request(event.event_type)
        size = self.rng.randint(150, 400) if status != 200 else self.rng.randint(300, 9000)
        day, seconds = divmod(event.offset, 86_400)
        hours, rest = divmod(seconds, 3600)
        stamp = f"{_DAYS[day]}:{hours:02d}:{rest // 60:02d}:{rest % 60:02d} +0000"
        return (
            f'{self._ips[event.customer_id]} - {event.customer_id} [{stamp}] "{request}" '
            f'{status} {size} "-" "Mozilla/5.0 ({event.device})"'
        )

    def malformed(self) -> str:
        """A damaged line that keeps its event's time and address, so no two are identical."""
        line = self.line()
        request_start = line.index('"')
        variant = self.rng.randrange(3)
        if variant == 0:
            return line[: self.rng.randrange(request_start + 5, len(line) - 10)]
        if variant == 1:
            return line.replace('"', "")
        stamp = line[line.index("[") + 1 : line.index("]")]
        return f"{stamp} {self.rng.choice(FREE_TEXT)}, client: {line.split(' ', 1)[0]}"


ANOMALIES = (
    Anomaly("MALFORMED_LINE", 0.005, Weblogs.malformed),
    Anomaly("HTTP_404", 0.02, lambda s: s.line(f"GET {s.rng.choice(ERROR_PATHS)} HTTP/1.1", 404)),
    Anomaly("HTTP_500", 0.005, lambda s: s.line(status=500)),
)


def _day_offset(day: date) -> int:
    return (day - HISTORY_START).days * 86_400


def select_events(
    rng: random.Random, kit_events: list[KitEvent], drop: Drop, n_rows: int
) -> list[KitEvent]:
    """J1 replays 2025 (kit events, then copies re-dated in 2025); J2 and J3 replay one day."""
    if drop.is_initial:
        events = kit_events[:n_rows]
        events += [
            KitEvent(rng.randrange(HISTORY_SECONDS), e.customer_id, e.event_type, e.device)
            for e in (kit_events[i % len(kit_events)] for i in range(n_rows - len(events)))
        ]
    else:
        start = _day_offset(drop.business_day)
        events = [
            KitEvent(start + e.offset % 86_400, e.customer_id, e.event_type, e.device)
            for e in rng.choices(kit_events, k=n_rows)
        ]
    return sorted(events, key=lambda event: event.offset)


def write_weblogs(
    landing: Path, seed: int, profile: Profile, drop: Drop, world: World
) -> WrittenFile:
    relative = f"weblogs/weblogs_{drop.tag}.log"
    rng = file_rng(seed, relative)
    n_rows = profile.weblogs_j1 if drop.is_initial else profile.weblogs_daily
    events = select_events(rng, world.kit.events, drop, n_rows)
    counts = anomaly_counts(ANOMALIES, n_rows, has_history=not drop.is_initial)
    source = Weblogs(rng, world, events)
    return write_source_file(landing, relative, "weblogs", source, ANOMALIES, counts, n_rows)
