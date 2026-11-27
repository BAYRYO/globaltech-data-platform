#!/usr/bin/env bash
# Stop a DataNode during a gold replay, watch the NameNode declare it dead and re-replicate its
# blocks onto the spare DataNode, start the lost DataNode again, then retire the spare: the
# cluster ends with 3 live DataNodes, no low-redundancy block and no spare container.
# Usage: scripts/resilience/datanode-failure.sh [--victim datanodeN] [--date YYYY-MM-DD] [--out DIR]
#
#   --victim  DataNode to stop (default datanode1)
#   --date    gold run date, replayed during the failure (default 2026-01-03)
#   --out     output folder relative to the repository root
#             (default output/resilience/<UTC time>)
#
# Needs the cluster nominal (3 live DataNodes, no low-redundancy block), no YARN application, gold
# PUBLISHED, a NameNode started with dfs.hosts.exclude (docker/conf/hadoop/hdfs-site.xml) and its
# exclude file empty. It stops and starts DataNode containers and the spare datanode4 (compose
# profile spare): run it only when nobody else uses the cluster.
#
# An early stop (an error, Ctrl-C, SIGTERM) stops the replay and starts the victim again. A stopped
# spare is retired at once; a running one is set to decommission, and the next run first finishes
# its retirement (a spare left by an earlier run, or the spare alone in the exclude file).
#
# It writes to the output folder:
#   timeline.csv           one HDFS sample every 5 s (scripts/resilience/hdfs_watch.py poll)
#   events.csv             time,event of each step
#   fingerprint-*.json     gold fingerprints after the baseline run and after the replay
#   dfsadmin-*.txt fsck-*  snapshots before, during, after re-replication, during the
#                          decommission and at the end
#   ec-*.txt               erasure-coded read and checksums while the victim is dead
#   gold-*.log             driver logs; gold-replay-yarn.log the replay's container logs
#   namenode.log           NameNode lines about dead nodes, replication and decommission
#   summary.txt            the milestones (hdfs_watch.py summarize)
# shellcheck source=scripts/resilience/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

victim=datanode1
day=2026-01-03
out=
while (($#)); do
  case $1 in
    --victim) victim=$2 ;;
    --date) day=$2 ;;
    --out) out=$2 ;;
    *) usage ;;
  esac
  shift 2
done
[[ $victim =~ ^datanode[123]$ ]] || die "--victim must be datanode1, datanode2 or datanode3"
spare=datanode4
hosts=datanode1,datanode2,datanode3,$spare
out=${out:-output/resilience/$(date -u +%Y%m%dT%H%M%SZ)}
[[ $out != /* ]] || die "--out must be relative to the repository root"
cd "$ROOT"
mkdir -p "$out"
started_at=$(now)

victim_dead() { is "$victim" dead; }
victim_replicas_removed() { is "$victim" dead && is "${victim}_blocks" 0; }
spare_live() { is "$spare" live; }
rereplicated() { zero under_replicated missing ec_low_redundancy ec_missing; }
victim_live() { is "$victim" live; }
excess_deleted() {
  victim_live && [[ $(latest "${victim}_blocks") =~ ^[1-9] ]] && zero excess pending_deletion
}

# spare_in STATE...: whether the spare is in one of these states, read from the timeline while the
# poller runs, from one dfsadmin -report otherwise.
spare_in() {
  local state want
  if [[ -n $poller ]]; then
    state=$(latest "$spare")
  else
    state=$(admin python3 "$watch" state "$spare")
  fi
  for want; do [[ $state == "$want" ]] && return 0; done
  return 1
}
spare_exists() { docker inspect "$(container "$spare")" >/dev/null 2>&1; }

excluded() { docker exec "$(container namenode)" cat "$exclude_file"; }

# Replace the NameNode exclude file with the given hostnames, then make the NameNode reread it.
exclude() {
  printf '%s\n' "$@" | docker exec -i "$(container namenode)" \
    sh -c 'cat >"$1.new" && mv -f "$1.new" "$1"' sh "$exclude_file"
  admin hdfs dfsadmin -refreshNodes
}

block_log() { admin hadoop daemonlog -setlevel namenode:9870 BlockStateChange "$1" >/dev/null; }

# Retire the spare from wherever a step or an earlier run left it: decommissioned then stopped if
# it runs; once the NameNode counts it dead, out of the exclude file, its container and volume
# deleted. Its volume keeps every replica it held, and a later start would report them all.
retire_spare() {
  if running "$spare"; then
    if [[ $(excluded) != "$spare" ]]; then
      exclude "$spare"
      event decommission_started
    fi
    snapshot decommissioning
    wait_for spare_decommissioned 14400 spare_in live/decommissioned
    docker stop "$(container "$spare")" >/dev/null
    event spare_stopped
  fi
  wait_for spare_dead 300 spare_in dead/decommissioned dead absent
  # The NameNode holds the address it resolved for the spare until the next refresh, and Docker
  # gives a freed address to the next container: a DataNode started with it would be decommissioned.
  if [[ -n $(excluded) ]]; then
    exclude
    event exclude_cleared
  fi
  compose --profile spare rm --stop --force "$spare" >/dev/null 2>&1 || true
  docker volume rm "gtdp_$spare" >/dev/null 2>&1 || true
  event spare_removed
}

waiter=
job=
cleanup() {
  local status=$?
  set +e
  stop_client "$waiter"
  stop_client "$job"
  stop_poller
  block_log INFO
  if ! running "$victim"; then
    echo "starting $victim again" >&2
    docker start "$(container "$victim")" >/dev/null
  fi
  ((status)) || return 0
  if running "$spare"; then
    [[ $(excluded) == "$spare" ]] || exclude "$spare"
    echo "stopped early ($status): see $out; $spare runs and is being decommissioned: run" \
      "$0 again, it finishes the retirement first (docs/runbook/hdfs.md)" >&2
  elif spare_exists || [[ $(excluded) == "$spare" ]]; then
    echo "stopped early ($status): see $out; retiring the stopped $spare" >&2
    (retire_spare) || echo "$spare is not retired: follow docs/runbook/hdfs.md" >&2
  else
    echo "stopped early ($status): see $out" >&2
  fi
}

echo "== preconditions"
exclude_file=$(admin python3 -c '
import re, urllib.request
conf = urllib.request.urlopen("http://namenode:9870/conf").read().decode()
found = re.search(r"<name>dfs\.hosts\.exclude</name>\s*<value>([^<]*)</value>", conf)
print(found.group(1) if found else "")')
[[ -n $exclude_file ]] ||
  die "the NameNode runs without dfs.hosts.exclude: restart it with the hdfs-site.xml of this checkout (docs/runbook/hdfs.md)"
for node in datanode1 datanode2 datanode3; do running "$node" || die "$node is not running"; done
apps=$(admin yarn application -list 2>/dev/null | grep -cE '^[[:space:]]*application_' || true)
((apps == 0)) || die "$apps YARN applications are running: the scenario needs the cluster alone"
state=$(admin hdfs dfs -cat /datalake/gold/_state.json)
[[ $state == *'"PUBLISHED"'* ]] ||
  die "gold is not PUBLISHED: $state; publish it again with scripts/cluster/submit.sh gold --date $day"
listed=$(excluded)
[[ -z $listed || $listed == "$spare" ]] ||
  die "the exclude file names $(echo "$listed" | tr '\n' ' '): finish that decommission first (docs/runbook/hdfs.md)"

on_exit cleanup
if spare_exists || [[ -n $listed ]]; then
  echo "== $spare left by an earlier run: its retirement first"
  retire_spare
fi
event start
block_log DEBUG
start_poller "$hosts"
nominal || die "the cluster is not nominal: see $out/timeline.csv"
snapshot before

echo "== baseline gold run, then its fingerprint"
"$CLUSTER_DIR/submit.sh" gold --date "$day" >"$out/gold-baseline.log" 2>&1 ||
  die "the baseline gold run failed: $out/gold-baseline.log"
event baseline_finished
"$CLUSTER_DIR/submit.sh" scripts/resilience/gold_fingerprint.py \
  --out "$out/fingerprint-before.json" >"$out/fingerprint-before.log" 2>&1

echo "== gold replay; $victim stops when gold starts publishing"
admin python3 "$watch" wait-state /datalake/gold/_state.json PUBLISHING >"$out/wait-state.log" 2>&1 &
waiter=$!
until grep -q watching "$out/wait-state.log"; do
  kill -0 "$waiter" 2>/dev/null || die "the state watch stopped: $out/wait-state.log"
  sleep 1
done
"$CLUSTER_DIR/submit.sh" gold --date "$day" >"$out/gold-replay.log" 2>&1 &
job=$!
event replay_submitted
wait "$waiter" || die "gold never started publishing: $out/wait-state.log"
waiter=
event publishing_seen
docker stop "$(container "$victim")" >/dev/null
event victim_stopped
replay=0
wait "$job" || replay=$?
job=
event "replay_exit_$replay"
app=$(grep -o 'application_[0-9]*_[0-9]*' "$out/gold-replay.log" | head -n 1 || true)
echo "replay application: ${app:-unknown}, exit code $replay"

wait_for victim_dead_in_report 300 victim_dead
# The NameNode drops the dead node's replicas at its next heartbeat check, at most 35 s later.
reached victim_replicas_removed 120 victim_replicas_removed || true
snapshot during
admin hdfs fsck /datalake >"$out/fsck-datalake-during.txt" 2>&1 || true

echo "== erasure-coded read with $victim dead"
admin hdfs fsck /benchmarks/ec -files -blocks -locations >"$out/ec-fsck-during.txt" 2>&1 || true
admin hdfs dfs -Ddfs.checksum.combine.mode=COMPOSITE_CRC -checksum \
  /benchmarks/ec/orders_20260101.csv /benchmarks/rep3/orders_20260101.csv \
  /benchmarks/ec/products_20260101.csv /benchmarks/rep3/products_20260101.csv \
  >"$out/ec-checksum-during.txt" 2>&1
# shellcheck disable=SC2016
admin bash -c 'for f; do
  start=$EPOCHREALTIME
  sum=$(hdfs dfs -cat "$f" | md5sum)
  awk -v f="$f" -v s="${sum%% *}" -v a="$start" -v b="$EPOCHREALTIME" \
    "BEGIN { printf \"%s md5 %s read in %.1f s\n\", f, s, b - a }"
done' bash /benchmarks/ec/orders_20260101.csv /benchmarks/rep3/orders_20260101.csv \
  /benchmarks/ec/products_20260101.csv /benchmarks/rep3/products_20260101.csv \
  >"$out/ec-read-during.txt" 2>&1
event ec_read_done

echo "== fingerprint after the replay"
"$CLUSTER_DIR/submit.sh" scripts/resilience/gold_fingerprint.py \
  --out "$out/fingerprint-after.json" >"$out/fingerprint-after.log" 2>&1
if cmp -s "$out/fingerprint-before.json" "$out/fingerprint-after.json"; then
  event fingerprints_identical
else
  event fingerprints_differ
fi
if [[ -n $app ]]; then
  admin yarn logs -applicationId "$app" >"$out/gold-replay-yarn.log" 2>&1 || true
fi

echo "== spare $spare started: re-replication"
compose --profile spare up --detach --no-deps --no-build "$spare"
event spare_started
wait_for spare_live 300 spare_live
wait_for rereplicated 14400 rereplicated
snapshot rereplicated

echo "== $victim started again: excess replicas"
docker start "$(container "$victim")" >/dev/null
event victim_started
wait_for victim_live 600 victim_live
wait_for excess_deleted 3600 excess_deleted

echo "== $spare decommissioned, stopped and removed"
retire_spare
snapshot after
if nominal; then event nominal; else event not_nominal; fi

stop_poller
docker logs --since "$started_at" "$(container namenode)" 2>&1 |
  grep -E 'removeDeadDatanode|Removing a node|registerDatanode|BLOCK\* ask|addStoredBlock|[Dd]ecommission|refreshNodes|Excluding' \
    >"$out/namenode.log" || true
admin python3 "$watch" summarize "$out/timeline.csv" "$out/events.csv" --victim "$victim" \
  --spare "$spare" | tee "$out/summary.txt"
nominal || die "the cluster did not come back to nominal: $out/dfsadmin-after.txt"
((replay == 0)) || die "the replay failed with exit code $replay: $out/gold-replay.log"
cmp -s "$out/fingerprint-before.json" "$out/fingerprint-after.json" ||
  die "gold differs after the replay: $out/fingerprint-*.json"
echo "==> resilience scenario OK: $out"
