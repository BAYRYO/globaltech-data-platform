"""Calendar and volume profiles of the generated data set."""

from dataclasses import dataclass
from datetime import date, timedelta

GENERATOR_VERSION = "1.0.0"


@dataclass(frozen=True)
class Profile:
    name: str
    orders_j1: int
    kit_orders: int
    weblogs_j1: int
    sensor_files: int

    @property
    def orders_daily(self) -> int:
        return round(self.orders_j1 / 365)

    @property
    def weblogs_daily(self) -> int:
        return round(self.weblogs_j1 / 365)


PROFILES = {
    profile.name: profile
    for profile in (
        Profile("ci", orders_j1=20_000, kit_orders=10_000, weblogs_j1=5_000, sensor_files=48),
        Profile(
            "dev", orders_j1=1_000_000, kit_orders=200_000, weblogs_j1=100_000, sensor_files=180
        ),
        Profile(
            "full", orders_j1=10_000_000, kit_orders=200_000, weblogs_j1=1_000_000, sensor_files=180
        ),
    )
}


@dataclass(frozen=True)
class Drop:
    index: int
    ingestion_date: date

    @property
    def is_initial(self) -> bool:
        return self.index == 0

    @property
    def with_discount(self) -> bool:
        """J3 changes the orders schema: `discount_pct` becomes the last column."""
        return self.index == 2

    @property
    def business_day(self) -> date:
        """Day whose activity the drop carries (J2 carries the orders of 2026-01-01)."""
        return self.ingestion_date - timedelta(days=1)

    @property
    def tag(self) -> str:
        return self.ingestion_date.strftime("%Y%m%d")


DROPS = tuple(Drop(index, date(2026, 1, 1) + timedelta(days=index)) for index in range(3))
SENSOR_DAY = date(2026, 1, 1)

HISTORY_START = date(2025, 1, 1)
HISTORY_SECONDS = 365 * 86_400
_HISTORY_DAYS = [(HISTORY_START + timedelta(days=day)).isoformat() for day in range(365)]


def iso_stamp(day: date, seconds: int) -> str:
    hours, rest = divmod(seconds, 3600)
    return f"{day.isoformat()}T{hours:02d}:{rest // 60:02d}:{rest % 60:02d}"


def history_stamp(offset: int) -> str:
    """ISO timestamp `offset` seconds after 2025-01-01T00:00:00 (offset < HISTORY_SECONDS)."""
    day, seconds = divmod(offset, 86_400)
    hours, rest = divmod(seconds, 3600)
    return f"{_HISTORY_DAYS[day]}T{hours:02d}:{rest // 60:02d}:{rest % 60:02d}"
