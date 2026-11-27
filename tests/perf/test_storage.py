"""The storage benchmark: its reading of hdfs fsck, its overhead arithmetic, and the HDFS commands
it sends, played against a fake ``hdfs`` client."""

import json
import posixpath
import subprocess

import pytest

from gtdp.perf import storage
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
KAN52 = ("/benchmarks/ec", "/benchmarks/rep3")
SOURCE, SMALL = "/scratch/orders.csv", "/scratch/products.csv"


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


class FakeHdfs:
    """An ``hdfs`` client over an in-memory namespace that answers the commands the tool sends.
    Each ``-cat`` advances a fake clock by the next scripted duration of its mode."""

    def __init__(self, existing=(), read_s=None, policy_answer=None):
        self.paths = set(existing) | {posixpath.dirname(p) for p in existing}
        self.policies: dict[str, str] = {}
        self.commands: list[list[str]] = []
        self.clock = 0.0
        self.read_s = {mode: list(times) for mode, times in (read_s or {}).items()}
        self.policy_answer = policy_answer

    def perf_counter(self):
        return self.clock

    def run(self, command, check=False, **_):
        self.commands.append(command)
        args, out, code = command[1:], "", 0
        if args[:2] == ["dfs", "-test"]:
            code = 0 if args[-1] in self.paths else 1
        elif args[:2] == ["dfs", "-mkdir"]:
            self.paths.update(a for a in args[2:] if a != "-p")
        elif args[:2] == ["dfs", "-cp"]:
            target = posixpath.join(args[-1], posixpath.basename(args[-2]))
            code = 1 if target in self.paths or "-f" in args else 0
            self.paths.add(target)
        elif args[:2] == ["dfs", "-rm"]:
            self.paths.discard(args[-1])
        elif args[:2] == ["ec", "-setPolicy"]:
            self.policies[args[args.index("-path") + 1]] = args[args.index("-policy") + 1]
        elif args[:2] == ["ec", "-getPolicy"]:
            path = args[-1]
            policy = self.policy_answer or self.policies.get(path)
            out = policy or f"The erasure coding policy of {path} is unspecified"
        elif args[:2] == ["dfs", "-cat"]:
            mode = posixpath.basename(posixpath.dirname(args[-1]))
            self.clock += self.read_s[mode].pop(0) if args[-1].endswith("orders.csv") else 0.5
        elif args[:2] == ["dfs", "-du"]:
            out = f"10  15  {args[-1]}\n"
        elif "-checksum" in args:
            out = f"{args[-1]}\tCOMPOSITE-CRC32C\tabcd\n"
        elif args[0] == "fsck":
            out = ERASURE_CODED
        if check and code:
            raise subprocess.CalledProcessError(code, command)
        return subprocess.CompletedProcess(command, code, out, "")

    def writes(self):
        mutating = ("-cp", "-mkdir", "-rm", "-setPolicy")
        return [c for c in self.commands if any(flag in c for flag in mutating)]


@pytest.fixture
def hdfs(monkeypatch):
    def install(**kwargs):
        fake = FakeHdfs(**kwargs)
        monkeypatch.setattr(storage.subprocess, "run", fake.run)
        monkeypatch.setattr(storage.time, "perf_counter", fake.perf_counter)
        return fake

    return install


def run_tool(tmp_path, *extra, runs=2):
    args = ["--source", SOURCE, "--small", SMALL, "--runs", str(runs), "--out", str(tmp_path)]
    return storage.main([*args, "--root", "/benchmarks/storage", *extra])


def test_rerunning_the_storage_benchmark_leaves_the_kan52_files_alone(hdfs, tmp_path):
    """docs/performance.md#reproduire runs storage.sh again and asks to keep /benchmarks/ec and
    /benchmarks/rep3 for KAN-52, whose block layout is committed in docs/perf-evidence/storage."""
    fake = hdfs(
        existing=[f"{folder}/orders_20260101.csv" for folder in KAN52],
        read_s={"rep3": [9, 1, 1], "ec": [9, 1, 1]},
    )
    assert run_tool(tmp_path, "--run", "r1") == 0
    rewrites = [c for c in fake.commands if "-cp" in c and c[-1].rstrip("/") in KAN52]
    assert rewrites == []
    assert all("/benchmarks/storage/r1/" in c[-1] + "/" for c in fake.writes()), fake.writes()


def test_a_run_area_that_exists_is_refused_before_any_write(hdfs, tmp_path):
    fake = hdfs(existing=["/benchmarks/storage/r1/ec/orders.csv"])
    assert run_tool(tmp_path, "--run", "r1") == 2
    assert fake.writes() == []


def test_the_erasure_coded_folder_gets_its_policy_checked_before_the_first_copy(hdfs, tmp_path):
    fake = hdfs(read_s={"rep3": [9, 1, 1], "ec": [9, 1, 1]})
    assert run_tool(tmp_path, "--run", "r1") == 0
    ec = "/benchmarks/storage/r1/ec"
    first_copy = next(i for i, c in enumerate(fake.commands) if "-cp" in c and c[-1] == ec)
    set_policy = fake.commands.index(
        ["hdfs", "ec", "-setPolicy", "-path", ec, "-policy", "XOR-2-1-1024k"]
    )
    get_policy = fake.commands.index(["hdfs", "ec", "-getPolicy", "-path", ec])
    assert set_policy < get_policy < first_copy
    result = json.loads((tmp_path / "result.json").read_text())
    assert {f["mode"]: f["policy"] for f in result["files"].values()} == {
        "rep3": "The erasure coding policy of /benchmarks/storage/r1/rep3 is unspecified",
        "ec": "XOR-2-1-1024k",
    }


def test_a_folder_with_another_policy_is_refused_before_any_copy(hdfs, tmp_path):
    fake = hdfs(policy_answer="RS-6-3-1024k")
    assert run_tool(tmp_path, "--run", "r1") == 2
    assert [c for c in fake.commands if "-cp" in c] == []


def test_each_timed_copy_writes_a_new_file_without_overwriting(hdfs, tmp_path):
    fake = hdfs(read_s={"rep3": [9, 1, 1, 1], "ec": [9, 1, 1, 1]})
    assert run_tool(tmp_path, "--run", "r1", runs=3) == 0
    copies = [c for c in fake.commands if "-cp" in c and SOURCE in c]
    assert len(copies) == 6
    assert not any("-f" in c for c in fake.commands if "-cp" in c)


def test_the_warm_up_read_is_left_out_and_the_modes_are_compared(hdfs, tmp_path):
    fake = hdfs(read_s={"rep3": [30, 4, 5, 6], "ec": [30, 1, 2, 3]})
    assert run_tool(tmp_path, "--run", "r1", runs=3) == 0
    result = json.loads((tmp_path / "result.json").read_text())
    assert result["read_s"]["rep3"]["runs"] == [4, 5, 6]
    assert result["read_s"]["ec"]["runs"] == [1, 2, 3]
    report = (tmp_path / "report.md").read_text()
    # 3 against 3, every rep3 read slower: the exact two-sided p is 2 splits of 20.
    assert "| p bilatéral (rep3 contre ec) |" in report
    assert "0.100" in report.split("| p bilatéral")[1].splitlines()[0]
    assert fake.paths >= {"/benchmarks/storage/r1/ec/orders.csv"}


def test_the_report_is_rebuilt_from_a_result_file(tmp_path):
    result = {
        "files": {},
        "copy_s": {"rep3": {"runs": [3, 4]}, "ec": {"runs": [1, 2]}},
        "read_s": {"rep3": {"runs": [3, 4]}, "ec": {"runs": [1, 2]}},
        "small_read_s": {"rep3": {"runs": [3, 4]}, "ec": {"runs": [1, 2]}},
    }
    (tmp_path / "result.json").write_text(json.dumps(result))
    assert storage.main(["--report", str(tmp_path)]) == 0
    report = (tmp_path / "report.md").read_text()
    assert "| p bilatéral (rep3 contre ec) | 0.333 | 0.333 | 0.333 |" in report
