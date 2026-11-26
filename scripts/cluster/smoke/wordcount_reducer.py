"""Hadoop Streaming delivers reducer input sorted by key."""

import sys
from itertools import groupby

pairs = (line.rstrip("\n").split("\t", 1) for line in sys.stdin)
for word, group in groupby(pairs, key=lambda pair: pair[0]):
    print(f"{word}\t{sum(int(count) for _, count in group)}")
