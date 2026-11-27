"""Replication asks of a NameNode log toward one DataNode, per 3 s pass of the NameNode.

    asks.py NAMENODE_LOG TARGET      e.g. asks.py /tmp/logs/nn.log 127.0.0.1:19864

The count per pass is steady once the dead DataNode is removed: the rate in blocks per second is
measured over passes 2 to 21 (BlockStateChange at DEBUG prints the ``BLOCK* ask`` lines).
"""

import collections
import re
import sys
from datetime import datetime


def main(log: str, target: str) -> None:
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
    start = asks[0][0]
    passes = collections.Counter(int((when - start).total_seconds()) // 3 for when, _ in asks)
    print("asks per 3 s pass:", [passes[i] for i in range(max(passes) + 1)][:25])
    steady = [passes[i] for i in range(2, min(max(passes), 22))]
    rate = sum(steady) / (3 * len(steady))
    print(f"asks in passes 2 to 21: {sum(steady)} over {3 * len(steady)} s = {rate:.2f} blocks/s")
    print("by source:", collections.Counter(source for _, source in asks).most_common())


if __name__ == "__main__":
    main(*sys.argv[1:3])
