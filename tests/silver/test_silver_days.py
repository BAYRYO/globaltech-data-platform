"""KAN-27 and KAN-28: a day is published whole or not at all, and the days run in order.

A source of a day gets its marker once published and verified; the day's ``_SUCCESS`` once
every source has one. A later day trusts complete days only and refuses to run after an
incomplete one; a failed check removes what the run published for the day.
"""

from datetime import date

import pytest

from gtdp import publish as publish_module
from gtdp.io import LaterPartitionError
from gtdp.jobs import silver as silver_job
from gtdp.silver import days
from gtdp.silver.pipelines import PIPELINES, rejects_path, table_path

from .lakes import DAYS, local
from .test_silver_incremental import silver_state

pytestmark = pytest.mark.slow

J1, J2, J3 = DAYS


class Killed(BaseException):
    """What ``kill -9`` leaves behind: no ``except`` or ``finally`` of the job runs after it."""


def complete(lake) -> list[str]:
    return [str(day) for day in days.complete_days(lake.spark, lake.cfg.paths.silver)]


def published_sources(lake, day: str) -> list[str]:
    marked = days.published_sources(lake.spark, lake.cfg.paths.silver, date.fromisoformat(day))
    return sorted(marked)


def partition_exists(lake, path: str, day: str) -> bool:
    return (local(path) / f"ingestion_date={day}").exists()


def test_a_day_is_complete_once_every_source_is_published(published):
    assert complete(published) == list(DAYS)
    assert all(published_sources(published, day) == sorted(PIPELINES) for day in DAYS)


def test_a_failed_check_after_the_write_removes_the_days_publication(
    fresh_lake, published, monkeypatch
):
    lake, silver = fresh_lake, fresh_lake.cfg.paths.silver
    lake.run("silver", J1)
    write = publish_module.write_partitions

    def losing_one_reject(frame, path, partitions):
        if path == rejects_path(silver, "orders"):
            frame = frame.limit(frame.count() - 1)
        write(frame, path, partitions)

    monkeypatch.setattr(publish_module, "write_partitions", losing_one_reject)
    with pytest.raises(silver_job.SilverCheckError, match="publication removed"):
        lake.run("silver", J2)
    monkeypatch.undo()

    assert not partition_exists(lake, rejects_path(silver, "orders"), J2)
    assert not partition_exists(lake, table_path(silver, "orders"), J2)
    assert "orders" not in published_sources(lake, J2)
    assert complete(lake) == [J1]
    with pytest.raises(silver_job.DayOrderError, match=f"{J2} is not complete"):
        lake.run("silver", J3)
    lake.run("silver", J2)
    lake.run("silver", J3)
    assert silver_state(lake) == silver_state(published)


def test_a_failed_dimension_check_puts_back_the_previous_days_table(fresh_lake, monkeypatch):
    lake = fresh_lake
    lake.run("silver", J1)
    before = lake.table("products").collect()
    replace, products, writes = (
        publish_module.replace_table,
        table_path(lake.cfg.paths.silver, "products"),
        [],
    )

    def losing_one_version(frame, path):
        writes.append(path)
        if path == products and writes.count(products) == 1:
            frame = frame.limit(frame.count() - 1)
        replace(frame, path)

    monkeypatch.setattr(publish_module, "replace_table", losing_one_version)
    with pytest.raises(silver_job.SilverCheckError, match="products"):
        lake.run("silver", J2)
    monkeypatch.undo()

    assert sorted(lake.table("products").collect()) == sorted(before)
    assert "products" not in published_sources(lake, J2)
    assert complete(lake) == [J1]


def test_a_killed_run_leaves_the_day_incomplete_until_it_is_replayed(
    fresh_lake, published, monkeypatch
):
    lake, silver = fresh_lake, fresh_lake.cfg.paths.silver
    lake.run("silver", J1)
    write = publish_module.write_partitions

    def killed_on_weblogs(frame, path, partitions):
        if path == table_path(silver, "weblogs"):
            raise Killed
        write(frame, path, partitions)

    monkeypatch.setattr(publish_module, "write_partitions", killed_on_weblogs)
    with pytest.raises(Killed):
        lake.run("silver", J2)
    monkeypatch.undo()

    assert published_sources(lake, J2) == ["customers", "orders", "products"]
    assert complete(lake) == [J1]
    with pytest.raises(silver_job.DayOrderError, match=f"{J2} is not complete"):
        lake.run("silver", J3)
    lake.run("silver", J2, "weblogs")
    lake.run("silver", J3)
    assert complete(lake) == list(DAYS)
    assert silver_state(lake) == silver_state(published)


def test_a_forced_earlier_day_makes_the_later_days_incomplete(fresh_lake, published):
    lake = fresh_lake
    for day in DAYS:
        lake.run("silver", day)

    with pytest.raises(LaterPartitionError, match=J3):
        lake.run("silver", J2)
    lake.run("silver", J2, force=True)

    assert complete(lake) == [J1, J2]
    lake.run("silver", J3)
    assert complete(lake) == list(DAYS)
    assert silver_state(lake) == silver_state(published)
