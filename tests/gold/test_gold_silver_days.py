"""KAN-58: gold reads what complete silver days published, the tables silver rebuilds whole
included.

``silver/customers`` and ``silver/products_scd2`` hold the rows rebuilt for the day silver last
wrote them for. Gold refuses to run while the rows it reads carry a day that is not complete,
or the table was rebuilt for one, whatever left it so: a day run source by source, a run stopped
after its write, a failed replay, a replay of an earlier day, a silver run during gold's.
"""

import contextlib
import hashlib
import threading
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from gtdp import io
from gtdp import publish as publish_module
from gtdp.gold.kpis import KPIS
from gtdp.gold.state import read_state
from gtdp.gold.tables import STAR, table_path
from gtdp.io import read_table
from gtdp.jobs import gold as gold_job
from gtdp.jobs import silver as silver_job
from gtdp.silver import days
from gtdp.silver.pipelines import PIPELINES
from gtdp.silver.pipelines import table_path as silver_path
from silver.lakes import DAYS, SALT, Lake, copy_of, local

pytestmark = pytest.mark.slow

J1, J2, J3 = DAYS


class Killed(BaseException):
    """What ``kill -9`` leaves behind: no ``except`` or ``finally`` of the job runs after it."""


@pytest.fixture
def lake(generated, tmp_path, monkeypatch) -> Lake:
    """Landing and bronze J1 to J3 of seed 42, no silver or gold yet."""
    monkeypatch.setenv("GTDP_PSEUDO_SALT", SALT)
    return copy_of(generated, tmp_path / "lake")


def gold_state(lake: Lake) -> dict[str, str]:
    """A checksum of the rows of every table of the gold job, in any order."""
    state = {}
    for name in [table.name for table in STAR] + list(KPIS):
        rows = read_table(lake.spark, table_path(lake.cfg.paths.gold, name)).collect()
        text = "\n".join(sorted(repr(tuple(row)) for row in rows))
        state[name] = hashlib.sha256(text.encode()).hexdigest()
    return state


def tear(path: Path, text: str) -> None:
    """Leave ``text`` at ``path`` with a checksum that matches it, as a writer that died after
    writing part of the file leaves it."""
    path.write_text(text)
    path.with_name(f".{path.name}.crc").unlink(missing_ok=True)


def complete(lake: Lake) -> list[str]:
    return [str(day) for day in days.complete_days(lake.spark, lake.cfg.paths.silver)]


def holds_rows_of(lake: Lake, source: str, day: str) -> bool:
    return lake.table(source).where(F.col("ingestion_date") == day).count() > 0


def in_another_run(job: Callable[[], object]) -> None:
    """Run ``job`` as another application would: in a thread of its own, to the end."""
    errors: list[BaseException] = []

    def target() -> None:
        try:
            job()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=target)
    thread.start()
    thread.join()
    if errors:
        raise errors[0]


def restate_a_version_as_the_one_before(lake: Lake) -> None:
    """Make the J2 drop of products one line: version 2 of a product, sent again with the
    attributes of version 1, as a catalog that takes back a price change does. Rebuilt with it,
    ``products_scd2`` loses version 2 and holds no row of J2. Silver J1 must have run."""
    products = lake.table("products")
    second = products.where(F.col("version") == 2).orderBy("product_id").first()
    first = products.where(
        (F.col("product_id") == second.product_id) & (F.col("version") == 1)
    ).first()
    record = [first.product_id, first.product_name, first.category, str(first.unit_price_eur)]
    line = ",".join([*record, f"{second.valid_from:%Y-%m-%dT%H:%M:%S}"])
    lake.change_landed(J2, "products", lambda lines: [lines[0], line])


def silver_runs_for_j2_when_gold_lists(lake: Lake, patch: pytest.MonkeyPatch, source: str) -> None:
    """Silver runs for J2 and ``source``, in another run, just before gold lists the files of
    the silver table of ``source``."""
    path, read = silver_path(lake.cfg.paths.silver, source), gold_job.read_table
    ran: list[bool] = []

    def silver_first(spark, table, *args, **kwargs):
        if table == path and not ran:
            ran.append(True)
            in_another_run(lambda: lake.run("silver", J2, source))
        return read(spark, table, *args, **kwargs)

    patch.setattr(gold_job, "read_table", silver_first)


def refused(lake: Lake, day: str, published: str, missing: str) -> pytest.ExceptionInfo:
    """Run gold for J1 and check it refuses, naming ``day``, its sources and the command that
    completes it, with nothing written and a ``FAILED`` audit row."""
    before = read_state(lake.spark, lake.cfg.paths.gold)
    command = rf"scripts/cluster/submit\.sh silver --date {day}\b"
    with pytest.raises(RuntimeError, match=command) as error:
        lake.run("gold", J1)

    message = str(error.value)
    assert f"published: {published}" in message, message
    assert f"missing: {missing}" in message, message
    assert read_state(lake.spark, lake.cfg.paths.gold) == before
    assert lake.audit()[-1].job_name == "gold"
    assert lake.audit()[-1].status == "FAILED"
    return error


def test_gold_waits_for_the_day_whose_dimensions_silver_rebuilt_then_equals_gold_in_one_go(
    lake, tmp_path
):
    lake.run("silver", J1)
    lake.run("gold", J1)
    published = gold_state(lake)
    lake.run("silver", J2, "customers")
    lake.run("silver", J2, "products")

    refused(lake, J2, published="customers, products", missing="orders, weblogs")

    assert gold_state(lake) == published
    lake.run("silver", J2, "orders")
    lake.run("silver", J2, "weblogs")
    lake.run("gold", J1)
    one_go = copy_of(lake, tmp_path / "one-go")
    one_go.run("silver", J1)
    one_go.run("silver", J2)
    one_go.run("gold", J2)
    assert complete(lake) == complete(one_go) == [J1, J2]
    assert gold_state(lake) == gold_state(one_go)


def test_gold_refuses_dimensions_a_silver_run_wrote_before_it_was_stopped(lake):
    lake.run("silver", J1)
    mark = days.mark_source

    def killed_before_the_customers_marker(spark, silver, day, source, run_id):
        if source == "customers":
            raise Killed
        mark(spark, silver, day, source, run_id)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(days, "mark_source", killed_before_the_customers_marker)
        with pytest.raises(Killed):
            lake.run("silver", J2, "customers")

    refused(lake, J2, published="none", missing="customers, orders, products, weblogs")


def test_gold_refuses_dimensions_left_by_an_earlier_run_of_a_day_a_replay_failed(lake):
    lake.run("silver", J1)
    lake.run("silver", J2, "customers")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(silver_job.SourceReport, "problems_before", lambda *_: ["injected"])
        with pytest.raises(silver_job.SilverCheckError, match="nothing published"):
            lake.run("silver", J2, "customers")

    refused(lake, J2, published="none", missing="customers, orders, products, weblogs")


def test_gold_refuses_dimensions_of_a_later_day_that_a_replay_of_an_earlier_day_unmarked(lake):
    lake.run("silver", J1)
    lake.run("silver", J2, "customers")
    lake.run("silver", J1, "weblogs")

    assert complete(lake) == [J1]
    refused(lake, J2, published="none", missing="customers, orders, products, weblogs")


def test_a_torn_marker_fails_with_its_path_until_a_silver_run_of_its_day_rewrites_it(lake):
    lake.run("silver", J1)
    lake.run("silver", J2, "customers")
    lake.run("silver", J2, "products")
    marker = local(lake.cfg.paths.silver) / "_days" / f"ingestion_date={J2}" / "customers.json"
    tear(marker, '{"source": "custo')
    unreadable = rf"{marker.name} is unreadable.*silver --date {J2}"

    with pytest.raises(RuntimeError, match=unreadable):
        lake.run("gold", J1)
    with pytest.raises(RuntimeError, match=unreadable):
        lake.run("silver", J2, "orders")
    lake.run("silver", J2)
    lake.run("gold", J1)

    assert complete(lake) == [J1, J2]
    assert days.published_sources(lake.spark, lake.cfg.paths.silver, date.fromisoformat(J2))


def test_a_damaged_built_for_file_stops_gold(lake):
    lake.run("silver", J1)
    record = local(silver_path(lake.cfg.paths.silver, "products")) / "_built_for.json"
    record.write_text('{"day": "2026-01-01", "run_id": "x"}')

    with pytest.raises(RuntimeError, match=rf"{record.name} is unreadable"):
        lake.run("gold", J1)


def test_gold_refuses_dimensions_of_an_incomplete_day_on_a_lake_without_their_built_for_files(
    lake,
):
    """The lake a silver older than ``_built_for.json`` wrote, as the cluster holds it: gold reads
    the rows of J2 in ``customers`` and ``products_scd2`` and refuses them (review A, F1)."""
    lake.run("silver", J1)
    lake.run("gold", J1)
    published = gold_state(lake)
    lake.run("silver", J2, "customers")
    lake.run("silver", J2, "products")
    for source, pipeline in PIPELINES.items():
        if not pipeline.partitioned:
            table = local(silver_path(lake.cfg.paths.silver, source))
            for name in ("_built_for.json", "._built_for.json.crc"):
                (table / name).unlink(missing_ok=True)

    refused(lake, J2, published="customers, products", missing="orders, weblogs")

    assert complete(lake) == [J1]
    assert gold_state(lake) == published


def test_gold_refuses_a_restated_version_that_left_no_row_of_its_incomplete_day(lake):
    lake.run("silver", J1)
    lake.run("gold", J1)
    published, products = gold_state(lake), lake.table("products").collect()
    restate_a_version_as_the_one_before(lake)
    lake.run("silver", J2, "products")

    assert not holds_rows_of(lake, "products", J2)
    assert sorted(lake.table("products").collect()) != sorted(products)
    refused(lake, J2, published="products", missing="customers, orders, weblogs")
    assert gold_state(lake) == published


def test_gold_refuses_a_restated_version_swapped_in_by_a_run_killed_before_its_marker(lake):
    lake.run("silver", J1)
    restate_a_version_as_the_one_before(lake)

    def killed_before_the_marker(*_args):
        raise Killed

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(days, "mark_source", killed_before_the_marker)
        with pytest.raises(Killed):
            lake.run("silver", J2, "products")

    assert not holds_rows_of(lake, "products", J2)
    refused(lake, J2, published="none", missing="customers, orders, products, weblogs")


@pytest.mark.parametrize("source", ["customers", "products"])
def test_gold_refuses_a_dimension_silver_swaps_for_j2_just_before_gold_lists_it(
    lake, monkeypatch, source
):
    """Review A, F3: a silver run lands between gold's check and its read. ``customers`` gets
    rows of J2; ``products_scd2`` a restated version and no row of J2."""
    lake.run("silver", J1)
    lake.run("gold", J1)
    published = gold_state(lake)
    if source == "products":
        restate_a_version_as_the_one_before(lake)
    silver_runs_for_j2_when_gold_lists(lake, monkeypatch, source)

    with pytest.raises(RuntimeError, match=rf"silver --date {J2}\b"):
        lake.run("gold", J1)

    assert complete(lake) == [J1]
    assert gold_state(lake) == published


def test_gold_fails_on_missing_files_when_silver_swaps_a_dimension_after_gold_checked_it(
    lake, monkeypatch
):
    """Gold lists the files of a silver table when it reads it, then checks those rows: a swap
    after that check leaves gold reading files that no longer exist, not other rows. Silver and
    gold run in separate applications; here they share one session, so the swap comes before
    gold caches anything that silver's ``refreshByPath`` would list again."""
    lake.run("silver", J1)
    lake.run("gold", J1)
    published = gold_state(lake)
    lock = gold_job.table_lock

    @contextlib.contextmanager
    def silver_swaps_customers_first(spark, path):
        in_another_run(lambda: lake.run("silver", J2, "customers"))
        with lock(spark, path):
            yield

    monkeypatch.setattr(gold_job, "table_lock", silver_swaps_customers_first)
    with pytest.raises(Exception, match="FileNotFound"):
        lake.run("gold", J1)

    assert holds_rows_of(lake, "customers", J2)
    assert gold_state(lake) == published


@pytest.mark.parametrize("failure", [OSError("write failed before the swap"), Killed()])
def test_a_dimension_write_that_stops_before_its_swap_leaves_gold_runnable(lake, failure):
    """Review A, F4: the J2 write of ``customers`` fails or is killed before the swap; the table
    still holds the rows of J1, a complete day."""
    lake.run("silver", J1)
    lake.run("gold", J1)
    published = gold_state(lake)
    customers = silver_path(lake.cfg.paths.silver, "customers")
    before = sorted(lake.table("customers").collect())
    replace = publish_module.replace_table

    def stops_before_the_swap(frame, path, *args, **kwargs):
        if path == customers:
            raise failure
        replace(frame, path, *args, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(publish_module, "replace_table", stops_before_the_swap)
        with pytest.raises(type(failure)):
            lake.run("silver", J2, "customers")
    assert sorted(lake.table("customers").collect()) == before

    lake.run("gold", J1)
    assert gold_state(lake) == published


def test_a_rolled_back_dimension_lets_gold_run_on_the_complete_days(lake):
    lake.run("silver", J1)
    lake.run("gold", J1)
    published, before = gold_state(lake), sorted(lake.table("customers").collect())

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(silver_job.SourceReport, "problems_after", lambda *_: ["injected"])
        with pytest.raises(silver_job.SilverCheckError, match="publication removed"):
            lake.run("silver", J2, "customers")

    assert sorted(lake.table("customers").collect()) == before
    lake.run("gold", J1)
    assert gold_state(lake) == published


def test_a_run_killed_inside_the_swap_leaves_the_j2_table_and_gold_refuses(lake):
    lake.run("silver", J1)
    rename = io._rename

    def killed_before_the_new_table_moves_in(fs, source, destination, **options):
        if source.getName() == "_customers.tmp":
            raise Killed
        rename(fs, source, destination, **options)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(io, "_rename", killed_before_the_new_table_moves_in)
        with pytest.raises(Killed):
            lake.run("silver", J2, "customers")

    assert holds_rows_of(lake, "customers", J2)
    refused(lake, J2, published="none", missing="customers, orders, products, weblogs")
