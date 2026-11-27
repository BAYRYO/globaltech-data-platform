"""The storage benchmark's reading of hdfs fsck and its overhead arithmetic."""

import pytest

from gtdp.perf.storage import Block, blocks, overhead

REPLICATED = """\
Connecting to namenode via http://namenode:9870/fsck?ugi=hadoop&files=1&blocks=1&locations=1
/benchmarks/rep3/orders.csv 150000000 bytes, replicated: replication=3, 2 block(s):  OK
0. BP-1:blk_1073741825_1001 len=134217728 Live_repl=3  [DatanodeInfoWithStorage[172.22.0.4:9866,DS-a,DISK], DatanodeInfoWithStorage[172.22.0.5:9866,DS-b,DISK], DatanodeInfoWithStorage[172.22.0.6:9866,DS-c,DISK]]
1. BP-1:blk_1073741826_1002 len=15782272 Live_repl=3  [DatanodeInfoWithStorage[172.22.0.5:9866,DS-b,DISK], DatanodeInfoWithStorage[172.22.0.6:9866,DS-c,DISK], DatanodeInfoWithStorage[172.22.0.4:9866,DS-a,DISK]]

Status: HEALTHY
"""  # noqa: E501
ERASURE_CODED = """\
/benchmarks/ec/orders.csv 150000000 bytes, erasure-coded: policy=XOR-2-1-1024k, 1 block(s):  OK
0. BP-1:blk_-9223372036854775792_1003 len=150000000 Live_repl=3  [blk_-9223372036854775792:DatanodeInfoWithStorage[172.22.0.6:9866,DS-c,DISK], blk_-9223372036854775791:DatanodeInfoWithStorage[172.22.0.4:9866,DS-a,DISK], blk_-9223372036854775790:DatanodeInfoWithStorage[172.22.0.5:9866,DS-b,DISK]]

Status: HEALTHY
"""  # noqa: E501


def test_each_replicated_block_lists_its_three_replicas():
    found = blocks(REPLICATED)
    assert [b.length for b in found] == [134217728, 15782272]
    assert all(b.live == 3 and len(set(b.nodes)) == 3 for b in found)


def test_a_block_group_lists_one_datanode_per_internal_block():
    assert blocks(ERASURE_CODED) == [
        Block(150000000, 3, ("172.22.0.6:9866", "172.22.0.4:9866", "172.22.0.5:9866"))
    ]


def test_overhead_is_the_share_of_bytes_beyond_the_data():
    assert overhead(100, 300) == 200.0
    assert overhead(100, 150) == 50.0
    assert overhead(82102, 164204) == pytest.approx(100.0)
