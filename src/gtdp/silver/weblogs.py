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

IPV4 = r"\d{1,3}(?:\.\d{1,3}){3}"
IPV6 = r"[0-9A-Fa-f]{0,4}(?::[0-9A-Fa-f]{0,4}){2,7}"
QUOTED = r'"((?:[^"\\]|\\.)*)"'
"""A quoted field, in which Apache escapes a quote or a backslash with a backslash; the value
is kept as written."""
COMBINED_LOG = (
    rf"^({IPV4}|{IPV6}) (\S+) (\S+) \[([^\]]+)\] "
    rf'"(?:([A-Z]+) (\S+) (HTTP/\d\.\d)|-)" (\d{{3}}) (\d+|-) {QUOTED} {QUOTED}$'
)
"""A request line of ``-`` (a connection closed before any request) has no method, path or
protocol."""
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
CLF_TIME = r"^(\d{2})/([A-Z][a-z]{2})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-]\d{2})(\d{2})$"
"""``dd/MMM/yyyy:HH:mm:ss Z``, the time of a Combined Log Format line."""
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
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


def _event_ts() -> Column:
    """The line's time in UTC, null when it does not parse.

    The time is rewritten as ISO-8601 text and cast. ``to_timestamp`` and ``try_to_timestamp``
    both raise ``SparkUpgradeException`` under the default EXCEPTION parser policy on a time
    that only the legacy parser reads, such as ``1/Jan/2026:20:21:48 +0000``: one such line
    would fail the day instead of being rejected. A cast returns null on any invalid value.
    """
    time = _field("time")

    def part(group: int) -> Column:
        return F.regexp_extract(time, CLF_TIME, group)

    numbers = F.create_map(*[F.lit(v) for i, name in enumerate(MONTHS, 1) for v in (name, i)])
    month = numbers[part(2)]
    iso = F.concat(
        part(3),
        F.lit("-"),
        F.lpad(month.cast("string"), 2, "0"),
        F.lit("-"),
        part(1),
        F.lit("T"),
        F.concat_ws(":", part(4), part(5), part(6)),
        part(7),
        F.lit(":"),
        part(8),
    )
    return F.when(time.rlike(CLF_TIME), iso.cast("timestamp"))


def _ip_prefix() -> Column:
    """The ``/24`` network of an IPv4 address; the ``/48`` of an IPv6 one, its first three
    groups once ``::`` is expanded, such as ``2001:db8:0::/48``."""
    ip = _field("ip")
    leading = F.filter(F.split(F.split(ip, "::").getItem(0), ":"), lambda group: group != "")
    groups = F.slice(F.concat(leading, F.array(*[F.lit("0")] * 3)), 1, 3)
    hexadecimal = F.transform(groups, lambda group: F.lower(F.conv(group, 16, 16)))
    ipv6 = F.concat(F.concat_ws(":", hexadecimal), F.lit("::/48"))
    ipv4 = F.concat(F.regexp_extract(ip, SUBNET, 1), F.lit(".0/24"))
    return F.when(ip.contains(":"), ipv6).otherwise(ipv4)


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
        "ip_hash": salted(F.lower(_field("ip"))),
        "ip_prefix": _ip_prefix(),
        "event_ts": _event_ts(),
        "customer_id": null_if(_field("user"), "-"),
        "method": null_if(_field("method"), ""),
        "path": null_if(_field("path"), ""),
        "protocol": null_if(_field("protocol"), ""),
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
    # The same line twice is one request: an exact duplicate within a day, a cross-day
    # duplicate when a complete earlier day published it.
    key=("log_id",),
)


def table(clean: DataFrame) -> DataFrame:
    """The parsed lines, the address replaced by its salted hash and its ``/24`` network."""
    return conform(clean, SILVER_WEBLOGS)
