"""Sensor event stream: one JSON Lines file per 5-second interval of 2026-01-01.

Every file holds one base reading per sensor. Anomalies are planned over the whole day: extra
events (late, very late, duplicates) go to eligible files, damaged readings replace base ones,
and one machine per site spikes for two minutes from minute 3, or earlier when the stream is
too short to hold the whole spike.
"""

import json
import random
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import count
from pathlib import Path
from typing import Any

from gtdp.datagen.engine import WrittenFile, file_rng, write_lines, write_manifest
from gtdp.datagen.model import GENERATOR_VERSION, SENSOR_DAY, Profile

FILE_MS = 5_000
SITES = ("FR-LYON", "DE-HAMBURG", "ES-BILBAO", "IT-TORINO", "UK-LEEDS")
MACHINES_PER_SITE = 4
WATERMARK_FILES = 36
"""Three minutes of stream before any event arrives beyond the two-minute watermark."""
SPIKE_START_FILE = 36
SPIKE_FILES = 24
_MIDNIGHT = datetime(SENSOR_DAY.year, SENSOR_DAY.month, SENSOR_DAY.day)


@dataclass(frozen=True)
class Metric:
    suffix: str
    name: str
    unit: str
    mean: float
    sigma: float
    decimals: int
    spikes: tuple[tuple[float, float], ...]
    """Value ranges beyond the alert threshold."""

    def normal(self, rng: random.Random) -> float:
        """Gaussian reading clipped to 3 sigma, which keeps it inside the alert thresholds."""
        low = max(0.1, self.mean - 3 * self.sigma)
        value = min(self.mean + 3 * self.sigma, max(low, rng.gauss(self.mean, self.sigma)))
        return round(value, self.decimals)

    def spike(self, rng: random.Random) -> float:
        return round(rng.uniform(*rng.choice(self.spikes)), self.decimals)


METRICS = (
    Metric("TEMP", "temperature", "C", 60, 8, 1, ((86, 110),)),
    Metric("VIB", "vibration", "mm/s", 3, 1, 2, ((7.5, 15),)),
    Metric("PRES", "pressure", "bar", 6, 0.5, 2, ((2, 3.5), (8.5, 10))),
)
SENSORS = tuple(
    (site, f"{site}-M{machine:02d}", metric)
    for site in SITES
    for machine in range(1, MACHINES_PER_SITE + 1)
    for metric in METRICS
)


@dataclass(frozen=True)
class SensorAnomaly:
    code: str
    rate: float
    """Share of the day's base readings."""
    extra: bool
    """An additional event rather than a damaged base reading."""
    first_file: int = 0


ANOMALIES = (
    SensorAnomaly("LATE_EVENT", 0.05, extra=True),
    SensorAnomaly("VERY_LATE_EVENT", 0.01, extra=True, first_file=WATERMARK_FILES),
    SensorAnomaly("DUPLICATE_EVENT", 0.03, extra=True, first_file=1),
    SensorAnomaly("MALFORMED_JSON", 0.005, extra=False),
    SensorAnomaly("NULL_VALUE", 0.005, extra=False),
)
LATENESS_MS = {"LATE_EVENT": (10_500, 89_500), "VERY_LATE_EVENT": (300_500, 899_500)}


@dataclass
class DayPlan:
    extras: list[list[str]]
    """Codes of the extra events of each file."""
    damaged: list[dict[int, str]]
    """Per file, sensor index -> code of the damaged base reading."""
    spiking: list[frozenset[int]]
    """Per file, indexes of the sensors reading above threshold."""


def file_name(index: int) -> str:
    return f"sensors_{_MIDNIGHT + timedelta(milliseconds=index * FILE_MS):%Y%m%dT%H%M%S}.jsonl"


def _stamp(ms: int) -> str:
    moment = _MIDNIGHT + timedelta(milliseconds=ms)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{moment.microsecond // 1000:03d}Z"


def plan_day(rng: random.Random, n_files: int) -> DayPlan:
    spike_start = max(0, min(SPIKE_START_FILE, n_files - SPIKE_FILES))
    spike_sensors = frozenset(
        SENSORS.index(
            (site, f"{site}-M{rng.randint(1, MACHINES_PER_SITE):02d}", rng.choice(METRICS))
        )
        for site in SITES
    )
    spiking = [
        spike_sensors if spike_start <= index < spike_start + SPIKE_FILES else frozenset()
        for index in range(n_files)
    ]
    plan = DayPlan([[] for _ in range(n_files)], [{} for _ in range(n_files)], spiking)
    n_base = n_files * len(SENSORS)
    for anomaly in ANOMALIES:
        for _ in range(max(1, round(anomaly.rate * n_base))):
            if anomaly.extra:
                plan.extras[rng.randrange(anomaly.first_file, n_files)].append(anomaly.code)
                continue
            while True:
                index, sensor = divmod(rng.randrange(n_base), len(SENSORS))
                if sensor not in spiking[index] and sensor not in plan.damaged[index]:
                    plan.damaged[index][sensor] = anomaly.code
                    break
    return plan


class SensorFile:
    def __init__(self, rng: random.Random, index: int, sequence: count) -> None:
        self.rng = rng
        self._start = index * FILE_MS
        self._sequence = sequence

    def event(self, sensor: int, event_ms: int, emitted_ms: int, spiking: bool) -> dict[str, Any]:
        site, machine, metric = SENSORS[sensor]
        return {
            "event_id": "",
            "sensor_id": f"{machine}-{metric.suffix}",
            "site": site,
            "machine_id": machine,
            "metric": metric.name,
            "value": metric.spike(self.rng) if spiking else metric.normal(self.rng),
            "unit": metric.unit,
            "event_time": _stamp(event_ms),
            "emitted_at": _stamp(emitted_ms),
        }

    def rows(
        self, plan: DayPlan, index: int, previous_clean: list[str]
    ) -> list[tuple[str | None, dict[str, Any] | str]]:
        """(code, event or verbatim line) in file order; code is None for a clean reading."""
        rng = self.rng
        base = []
        for sensor in range(len(SENSORS)):
            event_ms = self._start + rng.randrange(FILE_MS)
            spiking = sensor in plan.spiking[index]
            code = "SPIKE" if spiking else plan.damaged[index].get(sensor)
            event = self.event(sensor, event_ms, event_ms + rng.randint(100, 1900), spiking)
            base.append((code, event))
        rows: list[tuple[str | None, dict[str, Any] | str]] = sorted(
            base, key=lambda row: row[1]["event_time"]
        )
        codes = plan.extras[index]
        copies = iter(rng.sample(previous_clean, codes.count("DUPLICATE_EVENT")))
        for code in codes:
            if code == "DUPLICATE_EVENT":
                extra: dict[str, Any] | str = next(copies)
            else:
                event_ms = self._start - rng.randint(*LATENESS_MS[code])
                emitted_ms = self._start + rng.randrange(FILE_MS)
                extra = self.event(rng.randrange(len(SENSORS)), event_ms, emitted_ms, False)
            rows.insert(rng.randrange(len(rows) + 1), (code, extra))
        return rows

    def render(self, code: str | None, payload: dict[str, Any] | str) -> str:
        """A MALFORMED_JSON line is cut in half, loses its closing brace or its value: invalid for
        every JSON reader, lenient ones included (Spark accepts single quotes by default)."""
        if isinstance(payload, str):
            return payload
        payload["event_id"] = f"S{SENSOR_DAY:%Y%m%d}-{next(self._sequence):06d}"
        if code == "NULL_VALUE":
            payload["value"] = None
        line = json.dumps(payload, separators=(",", ":"))
        if code != "MALFORMED_JSON":
            return line
        variant = self.rng.randrange(3)
        if variant == 0:
            return line[: len(line) // 2]
        if variant == 1:
            return line[:-1]
        head, tail = line.split('"value":', 1)
        return f'{head}"value":,{tail.split(",", 1)[1]}'


def write_sensors(root: Path, seed: int, profile: Profile) -> list[WrittenFile]:
    directory = root / "sensors" / SENSOR_DAY.isoformat()
    plan = plan_day(file_rng(seed, f"sensors/{SENSOR_DAY.isoformat()}"), profile.sensor_files)
    sequence = count(1)
    previous_clean: list[str] = []
    files = []
    for index in range(profile.sensor_files):
        name = file_name(index)
        sensor_file = SensorFile(file_rng(seed, f"sensors/{name}"), index, sequence)
        lines, clean, codes = [], [], Counter()
        for code, payload in sensor_file.rows(plan, index, previous_clean):
            line = sensor_file.render(code, payload)
            lines.append(line)
            if code is None:
                clean.append(line)
            else:
                codes[code] += 1
        rows, sha256 = write_lines(directory / name, lines)
        files.append(WrittenFile("sensors", name, rows, sha256, dict(sorted(codes.items()))))
        previous_clean = clean
    header = {
        "generator_version": GENERATOR_VERSION,
        "seed": seed,
        "profile": profile.name,
        "ingestion_date": SENSOR_DAY.isoformat(),
    }
    write_manifest(directory, header, files)
    return files
