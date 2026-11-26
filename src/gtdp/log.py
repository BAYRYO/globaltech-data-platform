"""JSON-lines logging to stderr, with the run context attached to every record.

Modules log through ``logging.getLogger(__name__)``. Fields passed with ``extra=`` and fields
bound with ``bind()`` become keys of the JSON object.
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from types import MappingProxyType

_CONTEXT: ContextVar[Mapping[str, object]] = ContextVar(
    "gtdp_log_context", default=MappingProxyType({})
)
_RECORD_ATTRIBUTES = set(vars(logging.makeLogRecord({}))) | {"message", "asctime", "taskName"}


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": None,
            "job": None,
            **_CONTEXT.get(),
        }
        entry.update((k, v) for k, v in vars(record).items() if k not in _RECORD_ATTRIBUTES)
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str, ensure_ascii=False)


class _JsonHandler(logging.StreamHandler):
    pass


def configure_logging(level: int | str = logging.INFO) -> None:
    """Send every record to stderr as one JSON object per line. Safe to call again."""
    root = logging.getLogger()
    for handler in [h for h in root.handlers if isinstance(h, _JsonHandler)]:
        root.removeHandler(handler)
    handler = _JsonHandler(sys.stderr)
    handler.setFormatter(_JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level)
    # py4j logs every gateway call at DEBUG and every connection at INFO.
    logging.getLogger("py4j").setLevel(logging.WARNING)


@contextmanager
def bind(**fields: object) -> Iterator[None]:
    """Add ``fields`` (``run_id``, ``job``, ...) to every record logged inside the block."""
    token = _CONTEXT.set({**_CONTEXT.get(), **fields})
    try:
        yield
    finally:
        _CONTEXT.reset(token)
