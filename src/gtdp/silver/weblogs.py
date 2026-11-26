"""Web logs: Combined Log Format lines parsed with native regular expressions, and the rows of
``silver/weblogs`` with the IP address pseudonymised.

404 and 500 responses are valid lines: they feed the HTTP error indicator, they are kept.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from gtdp.schemas import SILVER_WEBLOGS, SOURCES
from gtdp.silver.parse import null_if
from gtdp.silver.pseudonymize import salted
from gtdp.silver.quality import Action, Entity, Rule, conform, raw

COMBINED_LOG = (
    r"^(\d{1,3}(?:\.\d{1,3}){3}) (\S+) (\S+) \[([^\]]+)\] "
    r'"([A-Z]+) (\S+) (HTTP/\d\.\d)" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"$'
)
FIELDS = (
    "ip",
    "ident",
    "user",
    "time",
    "method",
    "path",
    "protocol",
    "status",
    "bytes",
    "referer",
    "user_agent",
)
TIME_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z"
EVENT_TYPES = (
    ("GET", "/products/", "view"),
    ("GET", "/search", "search"),
    ("POST", "/cart/add", "cart"),
    ("POST", "/checkout", "purchase"),
)
"""Method and path prefix of each request type the site logs; any other request is ``other``."""
OTHER_EVENT = "other"
PRODUCT_IN_PATH = r"(?:^/products/|[?&]product=)(P\d+)"
DEVICE_IN_AGENT = r"\(([^)]*)\)"
SUBNET = r"^(\d{1,3}\.\d{1,3}\.\d{1,3})\.\d{1,3}$"


def _field(name: str) -> Column:
    return F.regexp_extract(raw("value"), COMBINED_LOG, FIELDS.index(name) + 1)


def _event_type() -> Column:
    event_type = F.lit(OTHER_EVENT)
    for method, prefix, name in reversed(EVENT_TYPES):
        matches = (_field("method") == method) & _field("path").startswith(prefix)
        event_type = F.when(matches, F.lit(name)).otherwise(event_type)
    return event_type


def _columns() -> dict[str, Column]:
    return {
        # Salted like the address: an unsalted hash of the line is recomputed from the other
        # columns and the 256 addresses of the /24, which gives the address back.
        "log_id": salted(raw("value")),
        "ip_hash": salted(_field("ip")),
        "ip_prefix": F.concat(F.regexp_extract(_field("ip"), SUBNET, 1), F.lit(".0/24")),
        "event_ts": F.to_timestamp(_field("time"), TIME_FORMAT),
        "customer_id": null_if(_field("user"), "-"),
        "method": _field("method"),
        "path": _field("path"),
        "protocol": _field("protocol"),
        "http_status": _field("status").cast("int"),
        "bytes_sent": null_if(_field("bytes"), "-").cast("long"),
        "referer": null_if(_field("referer"), "-"),
        "user_agent": _field("user_agent"),
        "device": null_if(F.regexp_extract(_field("user_agent"), DEVICE_IN_AGENT, 1), ""),
        "event_type": _event_type(),
        "product_id": null_if(F.regexp_extract(_field("path"), PRODUCT_IN_PATH, 1), ""),
    }


ENTITY = Entity(
    SOURCES["weblogs"],
    _columns,
    (
        Rule(
            "MALFORMED_LINE",
            Action.REJECT,
            lambda: ~raw("value").rlike(COMBINED_LOG) | F.col("event_ts").isNull(),
        ),
    ),
)


def table(clean: DataFrame) -> DataFrame:
    """The parsed lines, the address replaced by its salted hash and its ``/24`` network."""
    return conform(clean, SILVER_WEBLOGS)
