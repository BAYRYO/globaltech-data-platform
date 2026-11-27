"""Replication asks of a NameNode log toward one DataNode, per 3 s pass of the NameNode, once the
lost DataNode is dead.

    asks.py NAMENODE_LOG TARGET SINCE      e.g. asks.py /tmp/logs/nn.log 127.0.0.1:19864 12:13:13

SINCE (HH:MM:SS) is when ``dfsadmin -report`` first lists the lost DataNode dead. The passes
start 3 s later, when the NameNode has removed it, and the rate counts the full passes after
that. BlockStateChange at DEBUG prints the ``BLOCK* ask`` lines.
"""

import collections
import re
import sys
from datetime import datetime, timedelta


def main(log: str, target: str, since: str) -> None:
    ask = re.compile(
        rf"(\S+ \S+),\d+ .*BLOCK\* ask \[([^\]]+)\] to replicate \S+ to datanode\(s\) {target}"
    )
    asks = []
    with open(log, errors="replace") as lines:
        for line in lines:
            if found := ask.match(line):
                when = datetime.strptime(found.group(1), "%Y-%m-%d %H:%M:%S")
                asks.append((when, found.group(2)))
    if not asks:
        sys.exit("no ask")
    clock = datetime.strptime(since, "%H:%M:%S").time()
    start = datetime.combine(asks[0][0].date(), clock) + timedelta(seconds=3)
    steady = [(when, source) for when, source in asks if when >= start]
    passes = collections.Counter(int((when - start).total_seconds()) // 3 for when, _ in steady)
    full = range(max(passes)) if passes else range(0)
    counts = [passes[i] for i in full]
    print("asks per 3 s pass after the death:", counts)
    rate = sum(counts) / (3 * len(counts)) if counts else 0.0
    print(f"{sum(counts)} asks over {3 * len(counts)} s = {rate:.2f} blocks/s")
    print("by source:", collections.Counter(source for _, source in steady).most_common())


if __name__ == "__main__":
    main(*sys.argv[1:4])
