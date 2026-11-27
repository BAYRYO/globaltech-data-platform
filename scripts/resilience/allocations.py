"""Block allocations of a NameNode log while a DataNode is stopped and not yet declared dead.

    allocations.py NAMENODE_LOG --address 172.22.0.4 --start 07:44:17.856 --end 07:45:49.352
                   [--path SUBSTRING]

For each file whose first allocation in [start, end) named the stopped DataNode, it prints when
its writer asked for a block again and the pipelines; then counts by delay and by position of
the stopped DataNode in the pipeline. A writer asks again once it fails to reach that DataNode,
so the delay is how long the failed connection took. The lines read are the ``BLOCK* allocate``
lines of the NameNode (``hdfs.StateChange``, INFO).
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

ALLOCATE = re.compile(
    r"(?P<time>\d\d:\d\d:\d\d[,.]\d{3}) INFO hdfs\.StateChange: BLOCK\* allocate "
    r"(?P<block>blk_\d+)_\d+, replicas=(?P<replicas>.*) for (?P<path>\S+)"
)
DELAYS = ((1.0, "< 1 s"), (3.0, "1 to 3 s"), (4.0, "3 to 4 s"), (15.0, "4 to 15 s"))


@dataclass(frozen=True)
class Allocation:
    seconds: float
    time: str
    block: str
    replicas: tuple[str, ...]
    path: str


@dataclass(frozen=True)
class Retry:
    first: Allocation
    again: Allocation | None

    @property
    def delay(self) -> float | None:
        return None if self.again is None else round(self.again.seconds - self.first.seconds, 3)


def seconds(clock: str) -> float:
    hours, minutes, rest = clock.replace(",", ".").split(":")
    return int(hours) * 3600 + int(minutes) * 60 + float(rest)


def parse(lines: Iterable[str]) -> list[Allocation]:
    found = []
    for line in lines:
        if match := ALLOCATE.search(line):
            replicas = tuple(r.split(":")[0] for r in match["replicas"].split(", "))
            time = match["time"].replace(",", ".")
            found.append(Allocation(seconds(time), time, match["block"], replicas, match["path"]))
    return found


def retries(
    allocations: Sequence[Allocation], address: str, start: float, end: float, path: str = ""
) -> list[Retry]:
    """One ``Retry`` per file whose first allocation in ``[start, end)`` names ``address``."""
    by_file: dict[str, list[Allocation]] = {}
    for allocation in allocations:
        if path in allocation.path:
            by_file.setdefault(allocation.path, []).append(allocation)
    return [
        Retry(first, rest[0] if rest else None)
        for first, *rest in by_file.values()
        if start <= first.seconds < end and address in first.replicas
    ]


def delay_class(delay: float | None) -> str:
    if delay is None:
        return "not asked again"
    return next((name for limit, name in DELAYS if delay < limit), ">= 15 s")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("log")
    parser.add_argument("--address", required=True, help="IP address of the stopped DataNode")
    parser.add_argument("--start", required=True, help="HH:MM:SS.mmm, the stop")
    parser.add_argument("--end", required=True, help="HH:MM:SS.mmm, the removeDeadDatanode")
    parser.add_argument("--path", default="", help="keep the files whose path holds this text")
    args = parser.parse_args(argv)
    with open(args.log, errors="replace") as log:
        found = retries(parse(log), args.address, seconds(args.start), seconds(args.end), args.path)
    for retry in found:
        again = retry.again
        print(
            f"{retry.first.time}  {','.join(retry.first.replicas):<38}"
            f" {again.time if again else '-':<12} {retry.delay if again else '-':>7}"
            f"  {','.join(again.replicas) if again else '-':<26} {retry.first.path}"
        )
    print(f"files: {len(found)}")
    print("delay:", dict(Counter(delay_class(r.delay) for r in found).most_common()))
    positions = Counter(r.first.replicas.index(args.address) + 1 for r in found)
    print("position of the stopped DataNode:", dict(sorted(positions.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
