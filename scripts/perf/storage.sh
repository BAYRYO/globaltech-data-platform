#!/usr/bin/env bash
# Lot 7 (KAN-47): store the same file with replication 3 and with erasure coding, then compare
# their volumes, block layouts and read times (src/gtdp/perf/storage.py).
# Usage: scripts/perf/storage.sh --source HDFS_FILE [--small HDFS_FILE] [--runs N]
#                                [--root HDFS_DIR] [--run NAME] [--out DIR]
#        scripts/perf/storage.sh --report DIR
#
#   --source  file copied into <root>/<run>/rep3 and <root>/<run>/ec, then measured
#   --small   a file smaller than one 1 MiB cell, for the small-file overhead
#   --runs    copies and reads per mode, alternated (default 5, plus one warm-up read)
#   --root    HDFS root of the run areas (default: [perf] storage of conf/app.toml)
#   --run     name of this run's area, refused if it exists (default: the UTC time)
#   --out     report directory, relative to the repository root (default build/perf/storage)
#   --report  rebuild DIR/report.md from DIR/result.json, without HDFS
#
# The tool sets XOR-2-1-1024k on <run>/ec itself and never overwrites a file: the copies kept
# for the DataNode failure test (KAN-52) stay as they are.
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../cluster/lib.sh"

(($#)) || usage
in_image || run_in_client "$@"
exec python3 -m gtdp.perf.storage "$@"
