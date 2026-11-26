#!/usr/bin/env bash
# Replay a sensor day through the streaming-sensors job on YARN, optionally crashing its driver.
# Usage: scripts/streaming/demo.sh [--root DIR] [--interval S] [--crash-after N] [--src DIR]
#                                  [--fresh] [--housekeeping]
#
#   --root DIR       HDFS lake root, /datalake by default. The job reads DIR/landing/sensors/incoming,
#                    writes DIR/gold/sensor_aggregates, DIR/gold/sensor_alerts, DIR/streaming/rejects,
#                    keeps its checkpoints in DIR/streaming/sensors/checkpoints and its audit rows
#                    in DIR/ops/audit (the configured paths when DIR is /datalake)
#   --interval S     seconds between two replayed files, 3 by default (the stream's pace is 5)
#   --crash-after N  docker kill the driver's client container as soon as the aggregates query has
#                    planned micro-batch N, then restart the job: it resumes from its checkpoint
#   --src DIR        sensor files to replay, data/generated/dev/sensors/2026-01-01 by default
#   --fresh          first delete what an earlier run left under DIR; refused on /datalake
#   --housekeeping   once the job has stopped, run streaming-housekeeping on DIR
#
# The job runs in client mode with one executor and stops by itself after 4 idle triggers (2 min
# at the configured 30 s) once the replay is over. Logs go to a new directory under
# ${TMPDIR:-/tmp}, printed first. GTDP_PSEUDO_SALT must be set (.env or export), as for any
# --env cluster job.
set -euo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

root=/datalake
interval=3
crash_after=
src=data/generated/dev/sensors/2026-01-01
fresh=
housekeeping=
while (($#)); do
  case $1 in
    --root) root=${2%/} && shift 2 ;;
    --interval) interval=$2 && shift 2 ;;
    --crash-after) crash_after=$2 && shift 2 ;;
    --src) src=$2 && shift 2 ;;
    --fresh) fresh=1 && shift ;;
    --housekeeping) housekeeping=1 && shift ;;
    *) sed -n '2,/^set /{/^set /d;s/^# \{0,1\}//;p}' "$0" >&2 && exit 2 ;;
  esac
done
[[ -d $src ]] || { echo "error: $src not found: generate it with python -m gtdp.datagen generate" >&2; exit 1; }
[[ -z $fresh || $root != /datalake ]] || { echo "error: --fresh deletes data, not on /datalake" >&2; exit 2; }

logs=$(mktemp -d "${TMPDIR:-/tmp}/gtdp-streaming-demo.XXXXXX")
checkpoints=$root/streaming/sensors/checkpoints
overrides=()
if [[ $root != /datalake ]]; then
  for area in landing gold streaming ops; do overrides+=(--set "paths.$area=$root/$area"); done
  overrides+=(--set "paths.checkpoints=$checkpoints")
fi
export GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1"
echo "logs: $logs"

client() { scripts/cluster/client.sh "$@" 2> >(grep -v '^WARN\|^INFO' >&2); }
webhdfs_status() {
  curl -s -o /dev/null -w '%{http_code}' "http://localhost:9870/webhdfs/v1$1?op=GETFILESTATUS&user.name=etl"
}
start_job() {
  scripts/cluster/submit.sh streaming-sensors --set streaming.stop_after_idle_triggers=4 \
    ${overrides[@]+"${overrides[@]}"} >"$logs/job-$1.log" 2>&1 &
  job_pid=$!
  until grep -q '"message": "query starts"' "$logs/job-$1.log" 2>/dev/null; do
    kill -0 "$job_pid" 2>/dev/null || { echo "error: job $1 ended early, see $logs/job-$1.log" >&2; exit 1; }
    sleep 1
  done
  echo "$(date +%T) job $1 started: $(grep -o 'application_[0-9_]*' "$logs/job-$1.log" | head -1)"
}

if [[ -n $fresh ]]; then
  client hdfs dfs -rm -r -f -skipTrash "$root/landing/sensors/incoming" \
    "$root/landing/sensors/incoming_staging" "$root/gold/sensor_aggregates" \
    "$root/gold/sensor_alerts" "$root/streaming/rejects" "$root/streaming/archive" \
    "$checkpoints" "$root/streaming/control" "$root/ops" >/dev/null
fi

start_job 1
client python3 -m gtdp.datagen replay-sensors --src "$src" \
  --dst "hdfs://namenode:8020$root/landing/sensors/incoming" --interval "$interval" \
  >"$logs/replay.log" 2>&1 &
replay_pid=$!
echo "$(date +%T) replay started: one file every $interval s from $src"

if [[ -n $crash_after ]]; then
  until [[ $(webhdfs_status "$checkpoints/aggregates/offsets/$crash_after") == 200 ]]; do
    sleep 0.05
  done
  container=$(docker ps -q --filter "name=gtdp-client-$job_pid-")
  docker kill "$container" >/dev/null
  wait "$job_pid" || true
  echo "$(date +%T) killed the driver container (docker kill $container) at aggregates batch $crash_after"
  for query in aggregates alerts rejects; do
    planned=$(client hdfs dfs -ls -C "$checkpoints/$query/offsets" | grep -o '[0-9]*$' | sort -n | tail -1)
    committed=$(client hdfs dfs -ls -C "$checkpoints/$query/commits" | grep -o '[0-9]*$' | sort -n | tail -1)
    echo "  $query: last planned batch $planned, last committed batch $committed"
  done
  sleep 20
  start_job 2
fi

wait "$replay_pid"
echo "$(date +%T) $(grep '^published' "$logs/replay.log")"
wait "$job_pid"
echo "$(date +%T) job stopped: $(grep -ho '"stop_reason": "[a-z ]*"' "$logs"/job-*.log | tail -1)"

if [[ -n $housekeeping ]]; then
  scripts/cluster/submit.sh streaming-housekeeping ${overrides[@]+"${overrides[@]}"} \
    >"$logs/housekeeping.log" 2>&1
  echo "$(date +%T) housekeeping: $(grep -o '"archived_files": [0-9]*' "$logs/housekeeping.log" | tail -1)"
fi
