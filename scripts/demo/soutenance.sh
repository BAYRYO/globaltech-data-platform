#!/usr/bin/env bash
# The live demonstration of the defence (docs/soutenance/demo.md). Reads /datalake, writes only
# under ROOT.
# Usage: scripts/demo/soutenance.sh prepare|live|clean
#
#   prepare  copy the first FILES files of the dev sensor day to data/generated/soutenance/sensors,
#            then run the stream on them without a crash, under ROOT/ref: the reference of `live`
#   live     the sequence shown to the jury: cluster health, gold state, silver reconciliation
#            from the audit, gold report, then the stream on the same files with its driver
#            killed at micro-batch CRASH_AFTER, under a new ROOT/crash-<time>, compared with
#            ROOT/ref; the report leaves out the history of ROOT/ref, earlier runs included
#   clean    delete ROOT
#
#   GTDP_DEMO_ROOT   ROOT, /user/etl/soutenance by default; refused under /datalake
#   GTDP_DEMO_FILES  FILES, 30 by default: 150 s of stream, the replay of each file takes about 3.6 s
#
# The stream runs with a 10 s trigger instead of the configured 30 s, so that it ends within the
# demonstration; everything else is the configured job. demo.sh stops it after 4 idle triggers:
# 40 s, longer than the replay takes to publish its first file. Logs go to a new directory under
# ${TMPDIR:-/tmp}, printed first.
set -euo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

root=${GTDP_DEMO_ROOT:-/user/etl/soutenance}
root=${root%/}
files=${GTDP_DEMO_FILES:-30}
crash_after=2
day=data/generated/dev/sensors/2026-01-01
subset=data/generated/soutenance/sensors
[[ $root == /* && $root != /datalake && $root != /datalake/* ]] ||
  { echo "error: GTDP_DEMO_ROOT must be an absolute HDFS path outside /datalake" >&2; exit 2; }
export GTDP_STREAMING__TRIGGER_INTERVAL_S=10
export GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1"

client() { scripts/cluster/client.sh "$@" 2> >(grep -v '^WARN\|^INFO' >&2); }
step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
show() { printf '$ %s\n' "$*"; }

prepare() {
  [[ -d $day ]] || { echo "error: $day not found: python -m gtdp.datagen generate --profile dev" >&2; exit 1; }
  rm -rf "$subset" && mkdir -p "$subset"
  find "$day" -name 'sensors_*.jsonl' | sort | head -n "$files" | xargs cp -t "$subset"
  echo "$(find "$subset" -name '*.jsonl' | wc -l) files in $subset"
  scripts/streaming/demo.sh --root "$root/ref" --fresh --interval 1 --src "$subset"
  scripts/cluster/submit.sh scripts/streaming/report.py "$root/ref" 2>/dev/null >"$subset.report" || true
  grep -E '^(aggregates|alerts|rejects) +rows' "$subset.report"
  grep -qE '^aggregates +rows +[1-9]' "$subset.report" ||
    { echo "error: the reference run wrote no aggregate, run prepare again" >&2; exit 1; }
}

silver_reconciliation() {
  GTDP_CLIENT_STDIN=1 scripts/cluster/client.sh pyspark --master 'local[2]' \
    --conf spark.ui.showConsoleProgress=false 2>/dev/null <<'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.read.parquet("hdfs://namenode:8020/datalake/ops/audit").createOrReplaceTempView("audit")
spark.sql("""
WITH last AS (
  SELECT *, row_number() OVER (PARTITION BY ingestion_date ORDER BY started_at DESC) AS n
  FROM audit WHERE job_name = 'silver' AND status = 'SUCCESS'
), sources AS (
  SELECT ingestion_date, s.source, s.counts.*
  FROM last LATERAL VIEW explode(from_json(metrics_json,
    'sources MAP<STRING, STRUCT<rows_in: BIGINT, clean: BIGINT, rejected: BIGINT, duplicated: BIGINT>>'
  ).sources) s AS source, counts
  WHERE n = 1
)
SELECT ingestion_date, sum(rows_in) AS bronze, sum(clean) AS clean, sum(rejected) AS rejected,
       sum(duplicated) AS duplicated,
       sum(rows_in) = sum(clean) + sum(rejected) + sum(duplicated) AS adds_up
FROM sources GROUP BY ingestion_date ORDER BY ingestion_date
""").show(truncate=False)
EOF
}

live() {
  [[ -d $subset ]] || { echo "error: $subset not found: run scripts/demo/soutenance.sh prepare first" >&2; exit 1; }
  logs=$(mktemp -d "${TMPDIR:-/tmp}/gtdp-soutenance.XXXXXX")
  crash=$root/crash-$(date +%H%M%S)
  echo "logs: $logs"

  step "1/6 Flux capteurs en arrière-plan : driver tué au micro-batch $crash_after, puis relancé"
  show scripts/streaming/demo.sh --root "$crash" --fresh --interval 1 --crash-after "$crash_after" --src "$subset" '&'
  scripts/streaming/demo.sh --root "$crash" --fresh --interval 1 --crash-after "$crash_after" \
    --src "$subset" >"$logs/stream.log" 2>&1 &
  stream_pid=$!

  step "2/6 Santé du cluster"
  show scripts/cluster/status.sh
  scripts/cluster/status.sh

  step "3/6 Gold publiée pour les jours silver complets J1 à J3"
  show scripts/cluster/client.sh hdfs dfs -cat /datalake/gold/_state.json
  client hdfs dfs -cat /datalake/gold/_state.json
  echo
  show "scripts/cluster/client.sh pyspark --master 'local[2]' < requête de l'audit (bronze = propres + rejets + doublons)"
  silver_reconciliation

  step "4/6 Rapport gold : gold contre silver recalculé, prix applicable de P00042"
  show scripts/cluster/submit.sh src/gtdp/gold/report.py
  scripts/cluster/submit.sh src/gtdp/gold/report.py >"$logs/gold-report.txt" 2>"$logs/gold-report.err" ||
    echo "gold report exit code $?"
  awk -v RS= '/\|status\|order_dates\|/ || /\|check +\|silver +\|/ || /\|product_sk +\|/ { print; print "" }' \
    "$logs/gold-report.txt" | cut -c1-136
  tail -n 1 "$logs/gold-report.txt"

  step "5/6 Flux capteurs : arrêt brutal et reprise sur checkpoint"
  show tail "$logs/stream.log"
  tail -n +1 -f --pid="$stream_pid" "$logs/stream.log"
  wait "$stream_pid"
  show scripts/cluster/submit.sh scripts/streaming/report.py "$root/ref" "$crash"
  scripts/cluster/submit.sh scripts/streaming/report.py "$root/ref" "$crash" \
    >"$logs/stream-report.txt" 2>"$logs/stream-report.err" || echo "stream report exit code $?"
  awk -v ref="$root/ref/" '/^== / { skip = index($0, ref) > 0 } !skip' "$logs/stream-report.txt"

  step "6/6 Applications YARN de la démonstration"
  show scripts/cluster/client.sh yarn application -list -appStates ALL
  client yarn application -list -appStates ALL 2>/dev/null |
    awk -F'\t' '$1 ~ /^application_/ { gsub(/ /, "", $0); print $1, $2, $6, $7 }' | sort | tail -n 6
}

clean() {
  client hdfs dfs -rm -r -f -skipTrash "$root"
}

case ${1:-} in
  prepare | live | clean) "$1" ;;
  *) sed -n '2,/^set /{/^set /d;s/^# \{0,1\}//;p}' "$0" >&2 && exit 2 ;;
esac
