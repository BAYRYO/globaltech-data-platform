import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
GOLD = "/datalake/gold"


def allocate(time: str, block: str, nodes: tuple[int, ...], path: str) -> str:
    """A ``BLOCK* allocate`` line of the NameNode, replicas on 172.22.0.<node>."""
    replicas = ", ".join(f"172.22.0.{node}:9866" for node in nodes)
    return (
        f"2026-11-27 {time} INFO hdfs.StateChange: BLOCK* allocate {block}, "
        f"replicas={replicas} for {GOLD}/{path}"
    )


LOG = [
    allocate("07:44:17,185", "blk_1073794533_53796", (5, 4, 6), "_dim_date.lock.tmp"),
    allocate("07:44:18,361", "blk_1073794538_53802", (5, 6), "_dim_customer.tmp/part-0.parquet"),
    allocate("07:44:18,471", "blk_1073794539_53803", (6, 5, 4), "_dim_customer.lock.takeover.tmp"),
    allocate("07:45:23,517", "blk_1073794540_53804", (5, 6), "_dim_customer.lock.takeover.tmp"),
    allocate("07:45:25,828", "blk_1073794543_53807", (5, 4, 6), "_dim_product.tmp/part-0.parquet"),
    allocate("07:45:25,845", "blk_1073794544_53808", (5, 6), "_dim_product.tmp/part-0.parquet"),
    allocate("07:45:38,473", "blk_1073794623_53887", (4, 5, 6), "_kpi_price_deviation.lock.tmp"),
    allocate("07:45:50,001", "blk_1073794630_53894", (4, 5, 6), "_late.lock.tmp"),
]


@pytest.fixture(scope="module")
def allocations():
    spec = importlib.util.spec_from_file_location(
        "allocations", REPO / "scripts" / "resilience" / "allocations.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def retried(allocations, **window):
    found = allocations.parse(LOG)
    start, end = allocations.seconds("07:44:17.856"), allocations.seconds("07:45:49.352")
    return allocations.retries(found, "172.22.0.4", start, end, **window)


def test_each_file_first_allocated_to_the_stopped_node_in_the_window_gives_its_delay(allocations):
    found = retried(allocations)

    assert [(r.first.time, r.delay, r.first.replicas.index("172.22.0.4") + 1) for r in found] == [
        ("07:44:18.471", 65.046, 3),
        ("07:45:25.828", 0.017, 2),
        ("07:45:38.473", None, 1),
    ]
    assert found[0].again.replicas == ("172.22.0.5", "172.22.0.6")


def test_a_path_filter_keeps_the_files_that_hold_it(allocations):
    found = retried(allocations, path="_dim_product.tmp")

    assert [r.first.block for r in found] == ["blk_1073794543"]


def test_delays_fall_in_named_classes(allocations):
    classes = [allocations.delay_class(d) for d in (None, 0.017, 2.1, 3.1, 9.2, 60.1)]

    assert classes == [
        "not asked again",
        "< 1 s",
        "1 to 3 s",
        "3 to 4 s",
        "4 to 15 s",
        ">= 15 s",
    ]
