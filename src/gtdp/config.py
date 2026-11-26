"""Layered configuration parsed once into frozen, typed settings.

Precedence, last wins: ``conf/app.toml`` < ``conf/env/<env>.toml`` < ``GTDP_<SECTION>__<KEY>``
environment variables < explicit overrides (``run.py --set section.key=value``). A nested table,
such as ``[streaming.thresholds]``, merges key by key: a layer replaces only the keys it sets. A
nested key is named ``GTDP_STREAMING__THRESHOLDS__TEMPERATURE__ABOVE`` or
``streaming.thresholds.temperature.above``.
"""

from __future__ import annotations

import logging
import os
import re
import tomllib
import types
import typing
from collections.abc import Mapping
from dataclasses import MISSING, dataclass, fields
from enum import StrEnum
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

logger = logging.getLogger(__name__)
_T = typing.TypeVar("_T")

ENV_PREFIX = "GTDP_"
CONF_DIR_VAR = "GTDP_CONF_DIR"
SALT_VAR = "GTDP_PSEUDO_SALT"
GIT_SHA_VAR = "GTDP_GIT_SHA"
DEV_SALT = "gtdp-local-dev-salt-not-secret"
_URI_SCHEME = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*:")
_MASTER = re.compile(r"local(\[([1-9][0-9]*|\*)\])?|yarn")


class ConfigError(ValueError):
    """The configuration is missing, malformed or inconsistent."""


class Env(StrEnum):
    LOCAL = "local"
    CLUSTER = "cluster"


class Secret:
    """A string that never shows its value in ``repr``, ``str`` or logs."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "***"

    __str__ = __repr__


@dataclass(frozen=True)
class AppSettings:
    name: str
    timezone: str

    def __post_init__(self) -> None:
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError):
            raise ConfigError(f"[app] timezone: unknown time zone {self.timezone!r}") from None


@dataclass(frozen=True)
class SparkSettings:
    shuffle_partitions: int
    adaptive: bool
    master: str | None = None

    def __post_init__(self) -> None:
        if self.shuffle_partitions < 1:
            raise ConfigError(
                f"[spark] shuffle_partitions must be >= 1, got {self.shuffle_partitions}"
            )
        if self.master is not None and not _MASTER.fullmatch(self.master):
            raise ConfigError(
                f"[spark] master must be local, local[N], local[*] or yarn, got {self.master!r}"
            )


@dataclass(frozen=True)
class LakePaths:
    """Root URI of each lake area: ``hdfs://...`` on the cluster, ``file://...`` locally."""

    landing: str
    bronze: str
    silver: str
    gold: str
    streaming: str
    ops: str
    checkpoints: str


@dataclass(frozen=True)
class _PathsSection(LakePaths):
    """``[paths]`` as written: the lake areas, and the filesystem of an absolute cluster path."""

    default_fs: str | None = None

    def __post_init__(self) -> None:
        if self.default_fs is not None and not re.fullmatch(
            r"[A-Za-z][A-Za-z0-9+.-]*://[^/]+/?", self.default_fs
        ):
            raise ConfigError(
                f"[paths] default_fs must be scheme://authority, got {self.default_fs!r}"
            )


@dataclass(frozen=True)
class Pseudonymization:
    salt: Secret


THRESHOLD_DIRECTIONS = ("above", "below")


@dataclass(frozen=True)
class StreamingSettings:
    """``[streaming]``: the sensor stream job (ADR-0008).

    ``thresholds`` maps each metric to its alert limits: ``{"above": 85.0}`` alerts on a value
    greater than 85, ``{"below": 4.0, "above": 8.0}`` on a value outside ``[4, 8]``.
    """

    trigger_interval_s: int
    max_files_per_trigger: int
    state_partitions: int
    stop_after_idle_triggers: int
    future_tolerance_s: int
    thresholds: Mapping[str, Mapping[str, float]]

    def __post_init__(self) -> None:
        for key, minimum in (
            ("trigger_interval_s", 0),
            ("max_files_per_trigger", 1),
            ("state_partitions", 1),
            ("stop_after_idle_triggers", 0),
            ("future_tolerance_s", 0),
        ):
            if getattr(self, key) < minimum:
                raise ConfigError(
                    f"[streaming] {key} must be >= {minimum}, got {getattr(self, key)}"
                )
        if not self.thresholds:
            raise ConfigError("[streaming] thresholds: no metric")
        for metric, limits in self.thresholds.items():
            label = f"[streaming] thresholds.{metric}"
            if not isinstance(limits, Mapping) or not limits:
                raise ConfigError(f"{label}: expected a table of {THRESHOLD_DIRECTIONS}")
            if unknown := sorted(limits.keys() - set(THRESHOLD_DIRECTIONS)):
                raise ConfigError(f"{label}: unknown directions {unknown}")
            for direction, value in limits.items():
                if isinstance(value, bool) or not isinstance(value, int | float):
                    raise ConfigError(f"{label}.{direction}: expected a number, got {value!r}")
            if limits.keys() == {"above", "below"} and limits["below"] >= limits["above"]:
                raise ConfigError(f"{label}: below must be lower than above")


@dataclass(frozen=True)
class AppConfig:
    """``streaming`` is ``None`` when the configuration has no ``[streaming]`` section."""

    env: Env
    app: AppSettings
    spark: SparkSettings
    paths: LakePaths
    pseudonymization: Pseudonymization
    git_sha: str | None
    streaming: StreamingSettings | None = None


_SECTIONS: dict[str, type] = {
    "app": AppSettings,
    "spark": SparkSettings,
    "paths": _PathsSection,
    "streaming": StreamingSettings,
}


def load_config(
    env: str,
    conf_dir: Path | None = None,
    overrides: Mapping[str, str] | None = None,
) -> AppConfig:
    """Read the configuration of ``env``; raise ``ConfigError`` on any invalid input.

    ``overrides`` maps ``"section.key"`` to a raw string value, typed like an environment
    variable. A ``[paths]`` value with a URI scheme is kept as is. On ``cluster``, an absolute
    path without scheme goes to ``paths.default_fs``. Any other path is a local file, relative to
    the parent of ``conf_dir``.
    """
    try:
        parsed_env = Env(env)
    except ValueError:
        valid = ", ".join(e.value for e in Env)
        raise ConfigError(f"unknown env {env!r}, expected one of: {valid}") from None

    conf_dir = conf_dir or _default_conf_dir()
    raw = _merge(_read_toml(conf_dir / "app.toml"), _read_toml(conf_dir / "env" / f"{env}.toml"))
    for name, value in os.environ.items():
        if name.startswith(ENV_PREFIX) and "__" in name:
            section, *keys = name.removeprefix(ENV_PREFIX).lower().split("__")
            _override(raw, section, keys, value, label=name)
    for dotted, value in (overrides or {}).items():
        section, *keys = dotted.split(".")
        _override(raw, section, keys, value, label=f"--set {dotted}")

    paths = _section("paths", _PathsSection, raw)
    root = conf_dir.resolve().parent
    return AppConfig(
        env=parsed_env,
        app=_section("app", AppSettings, raw),
        spark=_section("spark", SparkSettings, raw),
        paths=LakePaths(
            **{
                area.name: _to_uri(
                    area.name, getattr(paths, area.name), root, parsed_env, paths.default_fs
                )
                for area in fields(LakePaths)
            }
        ),
        pseudonymization=Pseudonymization(_salt(parsed_env)),
        git_sha=os.environ.get(GIT_SHA_VAR) or None,
        streaming=_section("streaming", StreamingSettings, raw) if "streaming" in raw else None,
    )


def _default_conf_dir() -> Path:
    if from_env := os.environ.get(CONF_DIR_VAR):
        return Path(from_env)
    # Walk up from this file so the repository's conf/ is found both from src/gtdp/ and from
    # dist/gtdp.zip/gtdp/, the archive that spark-submit receives with --py-files.
    for parent in Path(__file__).resolve().parents:
        if (parent / "conf" / "app.toml").is_file():
            return parent / "conf"
    raise ConfigError(f"no conf/app.toml found above {__file__}; set {CONF_DIR_VAR}")


def _read_toml(path: Path) -> dict[str, dict[str, object]]:
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except FileNotFoundError:
        raise ConfigError(f"configuration file not found: {path}") from None
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{path}: {exc}") from None
    for section, table in data.items():
        if section not in _SECTIONS:
            raise ConfigError(f"{path}: unknown section [{section}], expected one of {_valid()}")
        if not isinstance(table, dict):
            raise ConfigError(f"{path}: [{section}] must be a table")
    return data


def _merge(*layers: dict[str, dict[str, object]]) -> dict[str, dict[str, object]]:
    merged: dict[str, dict[str, object]] = {}
    for layer in layers:
        _merge_into(merged, layer)
    return merged


def _merge_into(target: dict[str, object], layer: Mapping[str, object]) -> None:
    """Copy ``layer`` into ``target``; a table merges into the table it replaces, key by key."""
    for key, value in layer.items():
        if isinstance(value, dict):
            if not isinstance(target.get(key), dict):
                target[key] = {}
            _merge_into(typing.cast(dict[str, object], target[key]), value)
        else:
            target[key] = value


def _override(
    raw: dict[str, dict[str, object]], section: str, keys: list[str], text: str, label: str
) -> None:
    """Set ``[section] keys[0].keys[1]...`` to ``text``, typed by the settings class."""
    if section not in _SECTIONS:
        raise ConfigError(f"{label}: unknown section {section!r}, expected one of {_valid()}")
    hints = typing.get_type_hints(_SECTIONS[section])
    key, *nested = keys
    if key not in hints:
        raise ConfigError(f"{label}: unknown key {key!r} in [{section}], expected {sorted(hints)}")
    table: dict[str, object] = raw.setdefault(section, {})
    hint = hints[key]
    for name in nested:
        if _base_type(hint) is not Mapping or not isinstance(table.setdefault(key, {}), dict):
            raise ConfigError(f"{label}: {key!r} in [{section}] is not a table")
        table, key, hint = (
            typing.cast(dict[str, object], table[key]),
            name,
            typing.get_args(hint)[1],
        )
    table[key] = _parse_text(text, _base_type(hint), label)


def _parse_text(text: str, typ: type, label: str) -> object:
    if typ is int:
        try:
            return int(text)
        except ValueError:
            raise ConfigError(f"{label}: expected an integer, got {text!r}") from None
    if typ is float:
        try:
            return float(text)
        except ValueError:
            raise ConfigError(f"{label}: expected a number, got {text!r}") from None
    if typ is bool:
        lowered = text.strip().lower()
        if lowered not in ("true", "false"):
            raise ConfigError(f"{label}: expected true or false, got {text!r}")
        return lowered == "true"
    return text


def _section(name: str, cls: type[_T], raw: dict[str, dict[str, object]]) -> _T:
    hints = typing.get_type_hints(cls)
    table = raw.get(name, {})
    if unknown := sorted(table.keys() - hints.keys()):
        raise ConfigError(f"[{name}]: unknown keys {unknown}, expected {sorted(hints)}")
    values: dict[str, object] = {}
    for field in fields(cls):
        if field.name not in table:
            if field.default is MISSING:
                raise ConfigError(f"[{name}]: missing key {field.name!r}")
            continue
        value, typ = table[field.name], _base_type(hints[field.name])
        # bool is a subclass of int: reject `shuffle_partitions = true`.
        if not isinstance(value, typ) or (typ is int and isinstance(value, bool)):
            raise ConfigError(
                f"[{name}] {field.name}: expected {typ.__name__}, got {type(value).__name__} "
                f"{value!r}"
            )
        values[field.name] = value
    return cls(**values)


def _base_type(hint: object) -> type:
    """Unwrap ``X | None`` to ``X``, and ``Mapping[K, V]`` to ``Mapping``."""
    if isinstance(hint, types.UnionType):
        return next(arg for arg in typing.get_args(hint) if arg is not type(None))
    return typing.cast(type, typing.get_origin(hint) or hint)


def _to_uri(area: str, value: str, root: Path, env: Env, default_fs: str | None) -> str:
    """Keep URIs as is; give every path an explicit filesystem.

    A scheme-less path would otherwise resolve against ``fs.defaultFS``, which is HDFS in any
    container where ``HADOOP_CONF_DIR`` is set.
    """
    if _URI_SCHEME.match(value):
        return value.rstrip("/")
    if env is Env.CLUSTER and value.startswith("/"):
        if default_fs is None:
            raise ConfigError(
                f"[paths] {area} = {value!r} names no filesystem: set paths.default_fs"
            )
        return default_fs.rstrip("/") + value.rstrip("/")
    return _file_uri(root / value)


def _file_uri(path: Path) -> str:
    """``file://`` URI of ``path``, without the percent-encoding of ``Path.as_uri()``.

    Hadoop reads the characters of a URI string literally: ``%20`` names a directory ``%20``.
    """
    return "file:///" + path.resolve().as_posix().lstrip("/")


def _salt(env: Env) -> Secret:
    if value := os.environ.get(SALT_VAR):
        return Secret(value)
    if env is Env.CLUSTER:
        raise ConfigError(f"{SALT_VAR} is required for env {env.value!r}")
    logger.warning("%s is not set: using the fixed development salt", SALT_VAR)
    return Secret(DEV_SALT)


def _valid() -> str:
    return ", ".join(_SECTIONS)
