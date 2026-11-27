#!/usr/bin/env bash
# Lot 7: run one benchmark of src/gtdp/perf/bench.py as a client-mode Spark application on YARN.
# Usage: scripts/perf/bench.sh <prepare|formats|skew|cache|partitions|explain|record-hash> [options]
#        scripts/perf/bench.sh <land|report|remeasure> [options]
#
#   --profile P     generator profile of the landing and the tables (default full)
#   --landing DIR   generator landing that land writes and prepare reads
#                   (default: [perf] landing of conf/app.toml, then /<profile>/landing)
#   --root DIR      HDFS directory of the benchmark tables (default: [perf] tables, then /<profile>)
#   --runs N        measured rounds (default 5)
#   --warmup N      warm-up rounds before them, left out of the statistics (default: [perf])
#   --out DIR       report directory (default build/perf/<experiment>[-<label>])
#   --label NAME    report directory build/perf/<experiment>-<NAME>
#   --salts, --salt-on, --skew-factor, --skew-threshold, --advisory-size   skew settings
#   --partitions N...                                                    shuffle partitions
#   GTDP_SPARK_SUBMIT_OPTS  extra spark-submit options, e.g. "--conf spark.executor.cores=4"
#
# land generates the profile (seed 42) and puts orders, customers, products and the manifests
# under --landing. report rebuilds report.md from the runs.json of --out; remeasure reads the
# stages of that runs.json again from the History Server. Neither starts a Spark application.
# Each timed run waits until YARN runs no other application. Reports: runs.json and report.md
# (timed experiments) or result.json, in the report directory. See docs/performance.md.
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../cluster/lib.sh"

(($#)) || usage
exec "$CLUSTER_DIR/submit.sh" src/gtdp/perf/bench.py "$@"
