#!/usr/bin/env bash
# Lot 7 (KAN-47): store the same file with replication 3 and with erasure coding, then compare
# their volumes, block layouts and read times (src/gtdp/perf/storage.py).
# Usage: scripts/perf/storage.sh --source HDFS_FILE [--small HDFS_FILE] [--runs N] [--out DIR]
#
#   --source  file copied into /benchmarks/rep3 and /benchmarks/ec, then measured
#   --small   a file smaller than one 1 MiB cell, for the small-file overhead
#   --runs    copies and reads per mode, alternated (default 5, plus one warm-up read)
#   --out     report directory, relative to the repository root (default build/perf/storage)
#
# The copies stay in place for the DataNode failure test (KAN-52).
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../cluster/lib.sh"

(($#)) || usage
in_image || run_in_client "$@"
exec python3 -m gtdp.perf.storage "$@"
