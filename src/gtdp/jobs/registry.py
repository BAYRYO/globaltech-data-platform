"""Job registry: a job module registers its entry function with ``@job``."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from pyspark.sql import SparkSession

from gtdp.audit import Layer, Run
from gtdp.config import AppConfig


@dataclass(frozen=True)
class JobContext:
    """Everything a job receives: run.py opens the audited run before calling the job.

    ``force`` is run.py ``--force``: process ``--date`` even when the target holds a later day.
    """

    spark: SparkSession
    cfg: AppConfig
    run: Run
    force: bool = False


JobFunc = Callable[[JobContext], None]


@dataclass(frozen=True)
class JobSpec:
    """``pseudonymizes``: the job hashes personal data, so it needs the pseudonymization salt."""

    name: str
    layer: Layer
    func: JobFunc
    description: str
    pseudonymizes: bool = False


_REGISTRY: dict[str, JobSpec] = {}


def job(
    name: str, layer: Layer, description: str, *, pseudonymizes: bool = False
) -> Callable[[JobFunc], JobFunc]:
    """Register the decorated function as the job ``name`` of ``layer``."""

    def register(func: JobFunc) -> JobFunc:
        if name in _REGISTRY:
            raise ValueError(f"job {name!r} is registered twice")
        _REGISTRY[name] = JobSpec(name, layer, func, description, pseudonymizes)
        return func

    return register


def registered() -> Mapping[str, JobSpec]:
    return MappingProxyType(dict(_REGISTRY))
