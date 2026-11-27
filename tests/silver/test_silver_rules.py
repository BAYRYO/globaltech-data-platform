"""KAN-26: each rule of each registry on its edge cases, and the order of the dispositions."""

import hashlib
import re
from datetime import date, datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from gtdp.bronze.transform import raw_line
from gtdp.config import Secret
from gtdp.schemas import RECORD_HASH, bronze_schema
from gtdp.silver import customers
from gtdp.silver.pipelines import PIPELINES
from gtdp.silver.pseudonymize import salt_frame
from gtdp.silver.quality import DISPOSITION, classify, with_references

from .lakes import DAYS, SALT

DAY = date(2026, 1, 2)


def salted(value: str) -> str:
    return hashlib.sha256(f"{SALT}{value}".encode()).hexdigest()


KNOWN = {"customer_id": ["C000001"], "product_id": ["P00001"]}
ORDER = {
    "transaction_id": "T000000001",
    "event_time": "2026-01-01T10:00:00",
    "customer_id": "C000001",
    "product_id": "P00001",
    "quantity": "2",
    "unit_price_eur": "10.50",
    "channel": "web",
    "status": "COMPLETED",
}
CUSTOMER = {
    "customer_id": "C000001",
    "full_name": "Hélène Lefèvre",
    "email": "helene.lefevre1@example.com",
    "phone": "+33612345678",
    "region": "FR-IDF",
    "segment": "SMB",
    "signup_date": "2024-02-27",
    "updated_at": "2026-01-01T08:00:00",
}
PRODUCT = {
    "product_id": "P00001",
    "product_name": "Lighting P00001",
    "category": "Lighting",
    "unit_price_eur": "26.90",
    "updated_at": "2025-06-01T00:00:00",
}
LOG = (
    '198.18.12.34 - C000001 [01/Jan/2026:20:21:48 +0000] "GET /products/P00042 HTTP/1.1" '
    '200 5123 "-" "Mozilla/5.0 (tablet)"'
)
ESCAPED_AGENT = 'Mozilla/5.0 (tablet) \\"Foo\\" \\\\'
"""A user agent holding a quote and a backslash, escaped as Apache writes them."""


def classified_frame(spark, source, rows, *, earlier=None, day=DAY):
    """``rows`` (dicts of raw strings, ``_corrupt_record`` and ``ingestion_date`` optional) as
    bronze would hold them, classified."""
    entity = PIPELINES[source].entity
    schema = bronze_schema(entity.source).add("_row", IntegerType())
    values = [{"ingestion_date": day, **row, "_row": i} for i, row in enumerate(rows)]
    records = (
        spark.createDataFrame([tuple(v.get(f.name) for f in schema) for v in values], schema)
        .withColumn("_source_file", F.lit("file"))
        .withColumn(RECORD_HASH, F.sha2(raw_line(entity.source.columns), 256))
    )
    identifiers = {
        column: spark.createDataFrame([(v,) for v in KNOWN[column]], f"{column} string")
        for column in entity.references
    }
    keys = None
    if earlier is not None:
        keys = spark.createDataFrame([(k,) for k in earlier], "transaction_id string")
    salt = salt_frame(spark, Secret(SALT))
    return classify(with_references(records, identifiers), entity, salt, keys)


def classified(spark, source, rows, **kwargs):
    """``classified_frame`` collected, in input order."""
    return classified_frame(spark, source, rows, **kwargs).orderBy("_row").collect()


def reasons(spark, source, base, change, **kwargs):
    (row,) = classified(spark, source, [{**base, **change}], **kwargs)
    return row.reject_reasons


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({}, []),
        ({"transaction_id": None}, ["MISSING_KEY"]),
        ({"transaction_id": ""}, ["MISSING_KEY"]),
        ({"transaction_id": "  "}, ["MISSING_KEY"]),
        ({"customer_id": None}, ["NULL_CUSTOMER"]),
        ({"customer_id": ""}, ["NULL_CUSTOMER"]),
        ({"customer_id": "  "}, ["NULL_CUSTOMER"]),
        ({"customer_id": "C999123"}, ["UNKNOWN_CUSTOMER"]),
        ({"product_id": "P90001"}, ["UNKNOWN_PRODUCT"]),
        ({"product_id": None}, ["UNKNOWN_PRODUCT"]),
        ({"quantity": "0"}, ["BAD_QUANTITY"]),
        ({"quantity": "-3"}, ["BAD_QUANTITY"]),
        ({"quantity": "abc"}, ["BAD_QUANTITY"]),
        ({"quantity": "1.5"}, ["BAD_QUANTITY"]),
        ({"quantity": None}, ["BAD_QUANTITY"]),
        ({"quantity": ""}, ["BAD_QUANTITY"]),
        ({"quantity": "  "}, ["BAD_QUANTITY"]),
        ({"quantity": "12"}, []),
        ({"unit_price_eur": "-12.50"}, ["BAD_PRICE"]),
        ({"unit_price_eur": "0"}, ["BAD_PRICE"]),
        ({"unit_price_eur": None}, ["BAD_PRICE"]),
        ({"unit_price_eur": ""}, ["BAD_PRICE"]),
        ({"unit_price_eur": "  "}, ["BAD_PRICE"]),
        ({"unit_price_eur": "abc"}, ["BAD_PRICE"]),
        ({"unit_price_eur": "12.345"}, ["BAD_PRICE"]),
        ({"unit_price_eur": "26.9"}, []),
        ({"event_time": "2025/03/04 10:00"}, ["BAD_TIMESTAMP"]),
        ({"event_time": "N/A"}, ["BAD_TIMESTAMP"]),
        ({"event_time": None}, ["BAD_TIMESTAMP"]),
        ({"event_time": ""}, ["BAD_TIMESTAMP"]),
        ({"event_time": "  "}, ["BAD_TIMESTAMP"]),
        ({"event_time": " 2026-01-01T10:00:00"}, ["BAD_TIMESTAMP"]),
        ({"event_time": "2025-03-04 10:00:00"}, ["BAD_TIMESTAMP"]),
        ({"event_time": "2025-02-30T10:00:00"}, ["BAD_TIMESTAMP"]),
        ({"event_time": "2025-13-01T10:00:00"}, ["BAD_TIMESTAMP"]),
        ({"event_time": "2025-01-01T25:00:00"}, ["BAD_TIMESTAMP"]),
        ({"event_time": "2026-02-29T10:00:00"}, ["BAD_TIMESTAMP"]),
        ({"event_time": "2024-02-29T10:00:00"}, []),
        ({"event_time": "2026-01-03T00:00:00"}, []),
        ({"event_time": "2026-01-03T00:00:01"}, ["FUTURE_TIMESTAMP"]),
        ({"event_time": "1899-12-31T23:59:59"}, ["ANCIENT_TIMESTAMP"]),
        ({"event_time": "1900-01-01T00:00:00"}, []),
        ({"channel": "fax"}, ["BAD_ENUM"]),
        ({"channel": "WEB"}, ["BAD_ENUM"]),
        ({"channel": None}, ["BAD_ENUM"]),
        ({"channel": ""}, ["BAD_ENUM"]),
        ({"status": "UNKNOWN"}, ["BAD_ENUM"]),
        ({"status": None}, ["BAD_ENUM"]),
        ({"discount_pct": "0.300"}, []),
        ({"discount_pct": "0.301"}, ["BAD_DISCOUNT"]),
        ({"discount_pct": "-0.100"}, ["BAD_DISCOUNT"]),
        ({"discount_pct": "abc"}, ["BAD_DISCOUNT"]),
        ({"customer_id": None, "quantity": "0"}, ["NULL_CUSTOMER", "BAD_QUANTITY"]),
    ],
)
def test_order_rules(spark, change, expected):
    assert reasons(spark, "orders", ORDER, change) == expected


def test_a_discount_absent_or_empty_is_zero(spark):
    discounts = [None, "0.125", "", "  "]
    rows = classified(
        spark,
        "orders",
        [{**ORDER, "transaction_id": f"T{i}", "discount_pct": d} for i, d in enumerate(discounts)],
    )

    assert [(str(row.discount_pct), row.reject_reasons) for row in rows] == [
        ("0.000", []),
        ("0.125", []),
        ("0.000", []),
        ("0.000", []),
    ]


def test_a_malformed_record_gets_that_reason_alone(spark):
    shifted = {"transaction_id": "T1;2026-01-01T10:00:00;C000001", "_corrupt_record": "T1;..."}

    (row,) = classified(spark, "orders", [shifted])

    assert row.reject_reasons == ["MALFORMED_ROW"]
    assert row[DISPOSITION] == "rejected"


def test_duplicates_exact_then_key_then_cross_day(spark):
    rows = classified(
        spark,
        "orders",
        [
            ORDER,
            ORDER,
            {**ORDER, "quantity": "3"},
            {**ORDER, "transaction_id": "T2", "quantity": "0"},
            {**ORDER, "transaction_id": "T2"},
            {**ORDER, "transaction_id": "T3"},
        ],
        earlier=["T3"],
    )
    by_id = {}
    for row in rows:
        by_id.setdefault(row.transaction_id, []).append(row[DISPOSITION])

    assert sorted(by_id["T000000001"]) == ["clean", "duplicated.exact", "duplicated.key"]
    assert by_id["T2"] == ["rejected", "clean"]
    assert by_id["T3"] == ["duplicated.cross_day"]


def test_the_key_survivor_does_not_depend_on_row_order(spark):
    one, two = ORDER, {**ORDER, "quantity": "3"}

    first = {r.quantity: r[DISPOSITION] for r in classified(spark, "orders", [one, two])}
    second = {r.quantity: r[DISPOSITION] for r in classified(spark, "orders", [two, one])}

    assert first == second


@pytest.mark.parametrize(
    ("change", "flags"),
    [
        ({}, []),
        ({"email": None}, ["MISSING_EMAIL"]),
        ({"email": ""}, ["MISSING_EMAIL"]),
        ({"email": "  "}, ["MISSING_EMAIL"]),
        ({"email": "helene.lefevre1example.com"}, ["BAD_EMAIL"]),
        ({"email": "helene.lefevre1@@example.com"}, ["BAD_EMAIL"]),
        ({"email": "helene.lefevre1@"}, ["BAD_EMAIL"]),
        ({"email": "helene lefevre1@example.com"}, ["BAD_EMAIL"]),
        ({"email": " HELENE.LEFEVRE1@EXAMPLE.COM "}, []),
        ({"region": None}, ["MISSING_REGION"]),
        ({"region": ""}, ["MISSING_REGION"]),
        ({"region": "  "}, ["MISSING_REGION"]),
        ({"signup_date": "27/02/2024"}, ["BAD_DATE"]),
        ({"signup_date": "2024-02-30"}, ["BAD_DATE"]),
        ({"signup_date": "2024/02/27"}, ["BAD_DATE"]),
        ({"signup_date": None}, ["BAD_DATE"]),
        ({"signup_date": "  "}, ["BAD_DATE"]),
        ({"signup_date": "2026-01-03"}, ["FUTURE_SIGNUP"]),
        ({"signup_date": "2026-01-02"}, []),
        ({"signup_date": "1899-12-31"}, ["ANCIENT_SIGNUP"]),
        ({"signup_date": "1500-01-01"}, ["ANCIENT_SIGNUP"]),
        ({"signup_date": "31/12/1899"}, ["BAD_DATE", "ANCIENT_SIGNUP"]),
        ({"signup_date": "1900-01-01"}, []),
        ({"updated_at": "1899-12-31T23:59:59"}, ["ANCIENT_UPDATED_AT"]),
        ({"updated_at": "1900-01-01T00:00:00"}, []),
    ],
)
def test_customer_flags_keep_the_record(spark, change, flags):
    (row,) = classified(spark, "customers", [{**CUSTOMER, **change}])

    assert (row.quality_flags, row.reject_reasons, row[DISPOSITION]) == (flags, [], "clean")


def test_customer_values_are_repaired_where_the_flag_says(spark):
    rows = classified(
        spark,
        "customers",
        [
            {**CUSTOMER, "region": None},
            {**CUSTOMER, "customer_id": "C2", "signup_date": "27/02/2024"},
            {**CUSTOMER, "customer_id": "C3", "signup_date": "31/02/2024"},
            {**CUSTOMER, "customer_id": "C4", "region": "  ", "signup_date": "2024-02-30"},
        ],
    )

    assert [(row.region, row.country) for row in rows] == [
        (None, "UNKNOWN"),
        ("FR-IDF", "FR"),
        ("FR-IDF", "FR"),
        (None, "UNKNOWN"),
    ]
    assert [row.signup_date for row in rows] == [date(2024, 2, 27), date(2024, 2, 27), None, None]


@pytest.mark.parametrize(
    "updated_at", ["N/A", "01/01/2026 08:00", "2026-01-01", "2026-02-30T08:00:00", "", "  ", None]
)
def test_a_customer_with_an_unreadable_updated_at_is_kept_and_flagged(spark, updated_at):
    (row,) = classified(spark, "customers", [{**CUSTOMER, "updated_at": updated_at}])

    assert (row.quality_flags, row[DISPOSITION]) == (["BAD_UPDATED_AT"], "clean")


def test_distinct_customer_records_without_updated_at_never_collapse(spark):
    records = [
        {**CUSTOMER, "updated_at": "N/A", "region": "DE-BY"},
        {**CUSTOMER, "updated_at": "01/01/2026 08:00", "region": "ES-MD"},
    ]

    current = [
        customers.table(
            classified_frame(spark, "customers", rows).where(F.col(DISPOSITION) == "clean")
        ).collect()
        for rows in (records, records[::-1])
    ]
    dispositions = [row[DISPOSITION] for row in classified(spark, "customers", records)]

    assert dispositions == ["clean", "clean"]
    assert len(current[0]) == 1
    assert current[0] == current[1]


def test_an_unreadable_updated_at_ranks_last_in_the_scd1_choice(spark):
    readable = {**CUSTOMER, "region": "FR-IDF", "ingestion_date": date(2026, 1, 1)}
    unreadable = {**CUSTOMER, "updated_at": "N/A", "region": "DE-BY"}

    (row,) = customers.table(
        classified_frame(spark, "customers", [readable, unreadable]).where(
            F.col(DISPOSITION) == "clean"
        )
    ).collect()

    assert row.region == "FR-IDF"


@pytest.mark.parametrize("customer_id", [None, "", "  "])
def test_a_customer_without_id_is_rejected(spark, customer_id):
    assert reasons(spark, "customers", CUSTOMER, {"customer_id": customer_id}) == ["MISSING_KEY"]


def test_a_restated_customer_is_a_key_duplicate(spark):
    rows = classified(
        spark, "customers", [CUSTOMER, {**CUSTOMER, "email": "HELENE.LEFEVRE1@EXAMPLE.COM"}]
    )

    assert sorted(row[DISPOSITION] for row in rows) == ["clean", "duplicated.key"]


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({}, []),
        ({"product_id": None}, ["MISSING_KEY"]),
        ({"product_id": "  "}, ["MISSING_KEY"]),
        ({"unit_price_eur": "abc"}, ["BAD_PRICE"]),
        ({"unit_price_eur": "-12.50"}, ["BAD_PRICE"]),
        ({"unit_price_eur": None}, ["BAD_PRICE"]),
        ({"category": None}, ["NULL_CATEGORY"]),
        ({"category": ""}, ["NULL_CATEGORY"]),
        ({"category": "  "}, ["NULL_CATEGORY"]),
        ({"updated_at": "2025-06-01"}, ["BAD_TIMESTAMP"]),
        ({"updated_at": "2025-02-30T00:00:00"}, ["BAD_TIMESTAMP"]),
        ({"updated_at": None}, ["BAD_TIMESTAMP"]),
        ({"updated_at": "  "}, ["BAD_TIMESTAMP"]),
        ({"updated_at": "1899-12-31T23:59:59"}, ["ANCIENT_TIMESTAMP"]),
        ({"updated_at": "1900-01-01T00:00:00"}, []),
    ],
)
def test_product_rules(spark, change, expected):
    assert reasons(spark, "products", PRODUCT, change) == expected


def test_a_conflicting_version_rejects_every_record_of_the_version(spark):
    conflict = {**PRODUCT, "unit_price_eur": "29.90"}
    other_day = {**PRODUCT, "unit_price_eur": "31.00"}

    rows = classified(spark, "products", [PRODUCT, PRODUCT, conflict])
    (later,) = classified(spark, "products", [other_day], day=date(2026, 1, 3))

    assert sorted(row[DISPOSITION] for row in rows) == ["duplicated.exact", "rejected", "rejected"]
    assert [row.reject_reasons for row in rows if row[DISPOSITION] == "rejected"] == [
        ["CONFLICTING_VERSION"]
    ] * 2
    assert later[DISPOSITION] == "clean"


@pytest.mark.slow
def test_generated_weblogs_parse_like_an_independent_parser(published):
    pattern = re.compile(
        r"^(\S+) - (\S+) \[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}) \+0000\] "
        r'"(GET|POST) (\S+) HTTP/1\.1" (\d{3}) (\d+) "-" "Mozilla/5\.0 \((\w+)\)"$'
    )
    kinds = {("GET", "/products/"): "view", ("GET", "/search"): "search"}
    kinds |= {("POST", "/cart/add"): "cart", ("POST", "/checkout"): "purchase"}
    expected = set()
    for day in DAYS:
        for line in set(published.landed_lines(day, "weblogs")):
            if not (found := pattern.match(line)):
                continue
            ip, user, stamp, method, path, status, size, device = found.groups()
            kind = next(
                (k for (m, p), k in kinds.items() if m == method and path.startswith(p)), "other"
            )
            product = re.search(r"(?:^/products/|[?&]product=)(P\d+)", path)
            expected.add(
                (
                    salted(line),
                    datetime.strptime(stamp, "%d/%b/%Y:%H:%M:%S"),
                    salted(ip),
                    ip.rsplit(".", 1)[0] + ".0/24",
                    None if user == "-" else user,
                    method,
                    path,
                    int(status),
                    int(size),
                    device,
                    kind,
                    product.group(1) if product else None,
                    day,
                )
            )
    columns = ["log_id", "event_ts", "ip_hash", "ip_prefix", "customer_id", "method", "path"]
    columns += ["http_status", "bytes_sent", "device", "event_type", "product_id"]
    rows = published.table("weblogs").select(*columns, F.col("ingestion_date").cast("string"))

    assert expected
    assert {tuple(row) for row in rows.collect()} == expected
    assert rows.count() == len(expected)


def test_a_combined_log_line_is_parsed(spark):
    (row,) = classified(spark, "weblogs", [{"value": LOG}])

    assert row[DISPOSITION] == "clean"
    assert (row.ip_prefix, row.customer_id, row.method, row.path, row.http_status) == (
        "198.18.12.0/24",
        "C000001",
        "GET",
        "/products/P00042",
        200,
    )
    assert (row.bytes_sent, row.referer, row.device, row.event_type, row.product_id) == (
        5123,
        None,
        "tablet",
        "view",
        "P00042",
    )
    assert str(row.event_ts) == "2026-01-01 20:21:48"


@pytest.mark.parametrize(
    ("request_line", "status", "event_type", "product_id"),
    [
        ("GET /search?q=cable HTTP/1.1", 200, "search", None),
        ("POST /cart/add?product=P00007 HTTP/1.1", 200, "cart", "P00007"),
        ("POST /checkout HTTP/1.1", 500, "purchase", None),
        ("GET /old-catalog HTTP/1.1", 404, "other", None),
    ],
)
def test_requests_map_to_event_types_and_errors_are_kept(
    spark, request_line, status, event_type, product_id
):
    line = LOG.replace("GET /products/P00042 HTTP/1.1", request_line).replace(
        " 200 ", f" {status} "
    )

    (row,) = classified(spark, "weblogs", [{"value": line}])

    assert (row[DISPOSITION], row.http_status, row.event_type, row.product_id) == (
        "clean",
        status,
        event_type,
        product_id,
    )


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        (
            ('"Mozilla/5.0 (tablet)"', f'"{ESCAPED_AGENT}"'),
            ("198.18.12.0/24", "GET", "/products/P00042", ESCAPED_AGENT),
        ),
        (
            ("198.18.12.34", "2001:DB8::1"),
            ("2001:db8:0::/48", "GET", "/products/P00042", "Mozilla/5.0 (tablet)"),
        ),
        (
            ("198.18.12.34", "2001:0db8:85a3:0000:0000:8a2e:0370:7334"),
            ("2001:db8:85a3::/48", "GET", "/products/P00042", "Mozilla/5.0 (tablet)"),
        ),
        (
            ('"GET /products/P00042 HTTP/1.1" 200 5123', '"-" 408 -'),
            ("198.18.12.0/24", None, None, "Mozilla/5.0 (tablet)"),
        ),
    ],
)
def test_valid_combined_log_variants_are_parsed(spark, change, expected):
    (row,) = classified(spark, "weblogs", [{"value": LOG.replace(*change)}])

    assert (row[DISPOSITION], row.reject_reasons) == ("clean", [])
    assert (row.ip_prefix, row.method, row.path, row.user_agent) == expected


def test_the_same_log_line_twice_in_a_day_is_one_request(spark):
    rows = classified(spark, "weblogs", [{"value": LOG}, {"value": LOG}])

    assert sorted(row[DISPOSITION] for row in rows) == ["clean", "duplicated.exact"]


def test_one_address_has_one_hash_whatever_its_case(spark):
    lower, upper = (LOG.replace("198.18.12.34", ip) for ip in ("2001:db8::1", "2001:DB8::1"))

    rows = classified(spark, "weblogs", [{"value": lower}, {"value": upper}])

    assert rows[0].ip_hash == rows[1].ip_hash


@pytest.mark.parametrize(
    "line",
    [
        "",
        "   ",
        LOG[:60],
        LOG.replace('"', ""),
        "01/Jan/2026:20:21:48 +0000 worker process exited on signal 9, client: 198.18.12.34",
        LOG.replace("01/Jan/2026", "31/Feb/2026"),
        LOG.replace("01/Jan/2026:20:21:48 +0000", "1/Jan/2026:20:21:48 +0000"),
        LOG.replace("01/Jan/2026:20:21:48 +0000", "01/Jan/2026:20:21:48 +0000 x"),
        LOG.replace("01/Jan/2026:20:21:48 +0000", "01/Jan/2026:20:21:48 GMT"),
        LOG.replace("01/Jan/2026:20:21:48 +0000", "01/Jan/2026:24:21:48 +0000"),
        LOG.replace("01/Jan/2026:20:21:48 +0000", "01/Jan/2026:20:21:48 +2500"),
    ],
)
def test_a_line_outside_the_combined_log_format_is_rejected(spark, line):
    assert reasons(spark, "weblogs", {"value": LOG}, {"value": line}) == ["MALFORMED_LINE"]


@pytest.mark.parametrize(
    ("time", "expected"),
    [
        ("31/Dec/1899:23:59:59 +0000", ["ANCIENT_TIMESTAMP"]),
        ("01/Jan/1900:00:59:59 +0100", ["ANCIENT_TIMESTAMP"]),
        ("01/Jan/1900:00:00:00 +0000", []),
        ("03/Jan/2026:00:00:00 +0000", []),
        ("03/Jan/2026:00:00:01 +0000", ["FUTURE_TIMESTAMP"]),
        ("02/Jan/2026:23:00:01 -0100", ["FUTURE_TIMESTAMP"]),
    ],
)
def test_a_log_time_before_1900_or_after_the_day_after_the_drop_is_rejected(spark, time, expected):
    line = LOG.replace("01/Jan/2026:20:21:48 +0000", time)

    assert reasons(spark, "weblogs", {"value": LOG}, {"value": line}) == expected


def test_a_log_time_offset_is_converted_to_utc(spark):
    line = LOG.replace("01/Jan/2026:20:21:48 +0000", "01/Jan/2026:00:21:48 +0200")

    (row,) = classified(spark, "weblogs", [{"value": line}])

    assert str(row.event_ts) == "2025-12-31 22:21:48"
