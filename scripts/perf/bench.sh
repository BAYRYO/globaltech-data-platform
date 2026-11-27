#!/usr/bin/env bash
# Lot 7: run one benchmark of src/gtdp/perf/bench.py as a client-mode Spark application on YARN.
# Usage: scripts/perf/bench.sh <prepare|formats|skew|cache|partitions|explain|record-hash> [options]
#
#   --runs N        measured rounds after one warm-up round (default 5)
#   --root DIR      HDFS directory of the benchmark tables (default /benchmarks/perf/full)
#   --landing DIR   generator landing read by prepare (default /benchmarks/mr-vs-spark/full/landing)
#   --label NAME    report directory build/perf/<experiment>-<NAME> instead of build/perf/<experiment>
#   --salts, --skew-factor, --skew-threshold, --advisory-size   skew settings
#   --partitions N...                                         shuffle partitions of the sweep
#   GTDP_SPARK_SUBMIT_OPTS  extra spark-submit options, e.g. "--conf spark.executor.cores=4"
#
# Each run waits until YARN runs no other application. Reports: runs.json and report.md (timed
# experiments) or result.json, in the report directory. See docs/performance.md.
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../cluster/lib.sh"

(($#)) || usage
exec "$CLUSTER_DIR/submit.sh" src/gtdp/perf/bench.py "$@"
