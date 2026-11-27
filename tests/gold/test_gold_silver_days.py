"""KAN-58: gold reads what complete silver days published, the tables silver rebuilds whole
included.

``silver/customers`` and ``silver/products_scd2`` hold the rows rebuilt for the day silver last
wrote them for. Gold refuses to run while that day is not complete, whatever left it so: a day
run source by source, a run stopped after its write, a failed replay, a replay of an earlier day.
"""

import hashlib
from datetime import date
from pathlib import Path

import pytest

from gtdp.gold.kpis import KPIS
from gtdp.gold.state import read_state
from gtdp.gold.tables import STAR, table_path
from gtdp.io import read_table
from gtdp.jobs import silver as silver_job
from gtdp.silver import days
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


def test_a_damaged_record_of_a_rebuilt_table_stops_gold(lake):
    lake.run("silver", J1)
    record = local(lake.cfg.paths.silver) / "_days" / "rebuilt" / "products.json"
    record.write_text('{"source": "products", "day": "2026-01-01", "run_id": "x"}')

    with pytest.raises(RuntimeError, match=rf"{record.name} is unreadable"):
        lake.run("gold", J1)
