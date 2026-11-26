"""Layered configuration parsed once into frozen, typed settings.

Precedence, last wins: ``conf/app.toml`` < ``conf/env/<env>.toml`` < ``GTDP_<SECTION>__<KEY>``
environment variables < explicit overrides (``run.py --set section.key=value``).
"""

from __future__ import annotations

import logging
import os
import tomllib
import types
import typing
from collections.abc import Mapping
from dataclasses import MISSING, dataclass, fields
from enum import StrEnum
from pathlib import Path

logger = logging.getLogger(__name__)
_T = typing.TypeVar("_T")

ENV_PREFIX = "GTDP_"
CONF_DIR_VAR = "GTDP_CONF_DIR"
SALT_VAR = "GTDP_PSEUDO_SALT"
GIT_SHA_VAR = "GTDP_GIT_SHA"
DEV_SALT = "gtdp-local-dev-salt-not-secret"


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
class Pseudonymization:
    salt: Secret


@dataclass(frozen=True)
class AppConfig:
    env: Env
    app: AppSettings
    spark: SparkSettings
    paths: LakePaths
    pseudonymization: Pseudonymization
    git_sha: str | None


_SECTIONS: dict[str, type] = {"app": AppSettings, "spark": SparkSettings, "paths": LakePaths}


def load_config(
    env: str,
    conf_dir: Path | None = None,
    overrides: Mapping[str, str] | None = None,
) -> AppConfig:
    """Read the configuration of ``env``; raise ``ConfigError`` on any invalid input.

    ``overrides`` maps ``"section.key"`` to a raw string value, typed like an environment
    variable. Relative paths in ``[paths]`` resolve against the parent of ``conf_dir``.
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
            section, _, key = name.removeprefix(ENV_PREFIX).lower().partition("__")
            _override(raw, section, key, value, label=name)
    for dotted, value in (overrides or {}).items():
        section, _, key = dotted.partition(".")
        _override(raw, section, key, value, label=f"--set {dotted}")

    paths = _section("paths", LakePaths, raw)
    root = conf_dir.resolve().parent
    return AppConfig(
        env=parsed_env,
        app=_section("app", AppSettings, raw),
        spark=_section("spark", SparkSettings, raw),
        paths=LakePaths(**{f.name: _to_uri(getattr(paths, f.name), root) for f in fields(paths)}),
        pseudonymization=Pseudonymization(_salt(parsed_env)),
        git_sha=os.environ.get(GIT_SHA_VAR) or None,
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
        for section, table in layer.items():
            merged.setdefault(section, {}).update(table)
    return merged


def _override(
    raw: dict[str, dict[str, object]], section: str, key: str, text: str, label: str
) -> None:
    if section not in _SECTIONS:
        raise ConfigError(f"{label}: unknown section {section!r}, expected one of {_valid()}")
    hints = typing.get_type_hints(_SECTIONS[section])
    if key not in hints:
        raise ConfigError(f"{label}: unknown key {key!r} in [{section}], expected {sorted(hints)}")
    raw.setdefault(section, {})[key] = _parse_text(text, _base_type(hints[key]), label)


def _parse_text(text: str, typ: type, label: str) -> object:
    if typ is int:
        try:
            return int(text)
        except ValueError:
            raise ConfigError(f"{label}: expected an integer, got {text!r}") from None
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
    """Unwrap ``X | None`` to ``X``."""
    if isinstance(hint, types.UnionType):
        return next(arg for arg in typing.get_args(hint) if arg is not type(None))
    return typing.cast(type, hint)


def _to_uri(value: str, root: Path) -> str:
    """Keep URIs as is; turn local paths into absolute ``file://`` URIs.

    A scheme-less path would otherwise resolve against ``fs.defaultFS``, which is HDFS in any
    container where ``HADOOP_CONF_DIR`` is set.
    """
    if "://" in value:
        return value.rstrip("/")
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
