#!/usr/bin/env bash
# Lot 6: run the MapReduce and the Spark job N times each on the same files, check that their
# results are identical, and write the measures (report.md, runs.json) to the output directory.
# Usage: scripts/mapreduce/compare.sh --date YYYY-MM-DD [--landing URI] [--runs N] [--out DIR]
#                                     [--no-wait] [--max-wait SECONDS]
#
#   --landing   landing root to read (default: paths.landing of conf/env/cluster.toml)
#   --runs      runs per engine, alternated (default 3)
#   --out       report directory, relative to the repository root (default build/mr-vs-spark)
#   --no-wait   start each run at once instead of waiting until YARN runs no other application
#   GTDP_PSEUDO_SALT  required, as for every --env cluster job
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../cluster/lib.sh"

(($#)) || usage
in_image || run_in_client "$@"
exec python3 -m gtdp.perf.mr_compare "$@"
