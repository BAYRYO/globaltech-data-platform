#!/usr/bin/env bash
# Lot 6: run the MapReduce and the Spark job N times each on one or more volumes, check that their
# results are identical, and write the measures (report.md, runs.json) to the output directory.
# Usage: scripts/mapreduce/compare.sh --date YYYY-MM-DD [--volumes NAME=URI ...] [--runs N]
#                                     [--mr-conf KEY=VALUE ...] [--out DIR] [--no-wait]
#                                     [--max-wait SECONDS]
#        scripts/mapreduce/compare.sh --reread NAME=APP_ID,APP_ID... [NAME=...] [--out DIR]
#
#   --volumes   landing roots to measure, each named (default: landing=paths.landing of
#               conf/env/cluster.toml)
#   --runs      runs per variant and volume, alternated (default 5)
#   --mr-conf   adds the variant mapreduce-tuned, the MapReduce job with -D KEY=VALUE (repeatable)
#   --out       report directory, relative to the repository root (default build/mr-vs-spark)
#   --no-wait   start each run at once instead of waiting until YARN runs no other application
#   --reread    rebuild the measures of finished applications, grouped by volume, without running
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../cluster/lib.sh"

(($#)) || usage
in_image || run_in_client "$@"
exec python3 -m gtdp.perf.mr_compare "$@"
