#!/usr/bin/env bash
# Rebuild every gold table into an empty scratch area while a DataNode is stopped: the DataNode
# stops as soon as fact_sales has its first file and starts again once the job ends. Then compare
# the scratch gold with /datalake/gold, table by table and fact_sales partition by partition, and
# delete the scratch area.
# Usage: scripts/resilience/gold-write-under-failure.sh [--victim datanodeN] [--date YYYY-MM-DD]
#          [--scratch DIR] [--out DIR]
#
#   --victim   DataNode to stop (default datanode1)
#   --date     gold run date (default 2026-01-03)
#   --scratch  HDFS directory, absent at start, that receives the gold, its locks and its audit
#              row (default /user/etl/gold-write-under-failure)
#   --out      output folder relative to the repository root
#              (default output/resilience/write-<UTC time>)
#
# Needs the cluster nominal (3 live DataNodes, no low-redundancy block), the spare datanode4 not
# running, no YARN application and /datalake/gold PUBLISHED; it reads /datalake and never writes
# there. It stops and starts a DataNode container: run it only when nobody else uses the cluster.
# It writes to the output folder:
#   timeline.csv, events.csv     one HDFS sample every 5 s, the steps
#   fingerprint-datalake.json    fingerprint of /datalake/gold (gold_fingerprint.py)
#   fingerprint-scratch.json     fingerprint of the gold rebuilt during the failure
#   gold.log, gold-yarn.log      driver log and container logs of the run
#   fsck-fact-sales-outage.txt   blocks and replicas of the scratch fact_sales, DataNode stopped
#   dfsadmin-*.txt, fsck-*.txt   before the run and once every block has 3 replicas again
#   namenode.log                 NameNode lines about the run's blocks and the DataNode
# shellcheck source=scripts/resilience/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

victim=datanode1
day=2026-01-03
scratch=/user/etl/gold-write-under-failure
out=
while (($#)); do
  case $1 in
    --victim) victim=$2 ;;
    --date) day=$2 ;;
    --scratch) scratch=$2 ;;
    --out) out=$2 ;;
    *) usage ;;
  esac
  shift 2
done
[[ $victim =~ ^datanode[123]$ ]] || die "--victim must be datanode1, datanode2 or datanode3"
[[ $scratch == /* && $scratch != /datalake && $scratch != /datalake/* ]] ||
  die "--scratch must be an absolute HDFS path outside /datalake"
out=${out:-output/resilience/write-$(date -u +%Y%m%dT%H%M%SZ)}
[[ $out != /* ]] || die "--out must be relative to the repository root"
cd "$ROOT"
mkdir -p "$out"
started_at=$(now)
in_scratch=(GTDP_PATHS__GOLD="$scratch/gold" GTDP_PATHS__OPS="$scratch/ops")

victim_live() { is "$victim" live; }
replicated() { victim_live && nominal; }

echo "== preconditions"
for node in datanode1 datanode2 datanode3; do running "$node" || die "$node is not running"; done
! running datanode4 || die "the spare datanode4 runs: retire it first (docs/runbook/hdfs.md)"
apps=$(admin yarn application -list 2>/dev/null | grep -cE '^[[:space:]]*application_' || true)
((apps == 0)) || die "$apps YARN applications are running: the run needs the cluster alone"
state=$(admin hdfs dfs -cat /datalake/gold/_state.json)
[[ $state == *'"PUBLISHED"'* ]] || die "gold is not PUBLISHED: $state"
if admin hdfs dfs -test -e "$scratch"; then die "$scratch exists: delete it or choose another --scratch"; fi

trigger=
job=
cleanup() {
  local status=$?
  stop_client "$trigger"
  stop_client "$job"
  stop_poller
  if ! running "$victim"; then
    echo "starting $victim again" >&2
    docker start "$(container "$victim")" >/dev/null
  fi
  if ((status)); then
    echo "stopped early ($status): see $out; delete the scratch area with" \
      "scripts/cluster/client.sh hdfs dfs -rm -r -skipTrash $scratch" >&2
  fi
}
on_exit cleanup
event start
start_poller datanode1,datanode2,datanode3
nominal || die "the cluster is not nominal: see $out/timeline.csv"
snapshot before

echo "== fingerprint of /datalake/gold"
"$CLUSTER_DIR/submit.sh" scripts/resilience/gold_fingerprint.py \
  --out "$out/fingerprint-datalake.json" >"$out/fingerprint-datalake.log" 2>&1
event reference_fingerprinted

echo "== gold rebuilt into $scratch; $victim stops at the first file of fact_sales"
# shellcheck disable=SC2016
"$CLUSTER_DIR/client.sh" bash -c 'echo "$(date -u +%FT%TZ) watching $1"
  until hdfs dfs -count "$1" 2>/dev/null | awk "\$2 > 0 { f = 1 } END { exit !f }"; do sleep 0.5; done
  hdfs dfs -count "$1"; echo "$(date -u +%FT%TZ) first file"' \
  bash "$scratch/gold/_fact_sales.staging-*" >"$out/trigger.log" 2>&1 &
trigger=$!
until grep -q watching "$out/trigger.log"; do
  kill -0 "$trigger" 2>/dev/null || die "the trigger stopped: $out/trigger.log"
  sleep 1
done
env "${in_scratch[@]}" "$CLUSTER_DIR/submit.sh" gold --date "$day" >"$out/gold.log" 2>&1 &
job=$!
event job_submitted
wait "$trigger" || die "fact_sales never got a file: $out/trigger.log"
trigger=
event fact_sales_first_file
docker stop "$(container "$victim")" >/dev/null
event victim_stopped
code=0
wait "$job" || code=$?
job=
event "job_exit_$code"
app=$(grep -o 'application_[0-9]*_[0-9]*' "$out/gold.log" | head -n 1 || true)
echo "gold application: ${app:-unknown}, exit code $code"

echo "== replicas of fact_sales with $victim stopped, then $victim started again"
admin hdfs fsck "$scratch/gold/fact_sales" -files -blocks -locations \
  >"$out/fsck-fact-sales-outage.txt" 2>&1 || true
docker start "$(container "$victim")" >/dev/null
event victim_started
wait_for victim_live 600 victim_live
wait_for replicated 3600 replicated
snapshot after

echo "== fingerprint of the gold rebuilt during the failure"
env "${in_scratch[@]}" "$CLUSTER_DIR/submit.sh" scripts/resilience/gold_fingerprint.py \
  --out "$out/fingerprint-scratch.json" >"$out/fingerprint-scratch.log" 2>&1
if cmp -s "$out/fingerprint-datalake.json" "$out/fingerprint-scratch.json"; then
  event fingerprints_identical
else
  event fingerprints_differ
fi
if [[ -n $app ]]; then admin yarn logs -applicationId "$app" >"$out/gold-yarn.log" 2>&1 || true; fi
docker logs --since "$started_at" "$(container namenode)" 2>&1 |
  grep -E "BLOCK\* allocate|updatePipeline|removeDeadDatanode|Removing a node|registerDatanode|BLOCK\* ask" \
    >"$out/namenode.log" || true
"$CLUSTER_DIR/client.sh" hdfs dfs -rm -r -skipTrash "$scratch" >/dev/null
event scratch_deleted
stop_poller

((code == 0)) || die "the gold run failed with exit code $code: $out/gold.log"
cmp -s "$out/fingerprint-datalake.json" "$out/fingerprint-scratch.json" ||
  die "the gold written during the failure differs from /datalake/gold: $out/fingerprint-*.json"
echo "==> gold written during the failure of $victim is identical to /datalake/gold: $out"
