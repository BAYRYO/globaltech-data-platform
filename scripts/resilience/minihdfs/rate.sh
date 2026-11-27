#!/usr/bin/env bash
# Re-replication rate onto a 4th DataNode after one of 3 dies, for three settings of
# max-streams, work.multiplier.per.iteration and max-streams-hard-limit.
# Usage, in the cluster image (run.sh): rate.sh
# shellcheck source=scripts/resilience/minihdfs/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
mkdir -p /tmp/files
for i in $(seq 1 900); do echo "$i" >"/tmp/files/f$i"; done
for settings in "2 2 4" "8 4 4" "8 4 8"; do
  read -r streams multiplier hard <<<"$settings"
  echo "=== max-streams=$streams work.multiplier=$multiplier max-streams-hard-limit=$hard"
  write_conf false "$streams" "$multiplier" "$hard" 5000
  start_nn
  for i in 1 2 3; do start_dn "$i"; done
  wait_report "Live datanodes (3)" 90 || exit 1
  hdfs dfs -put /tmp/files /data 2>/dev/null
  echo "files: $(hdfs dfs -count /data | awk '{print $2}')"
  hadoop daemonlog -setlevel localhost:9870 BlockStateChange DEBUG >/dev/null 2>&1
  start_dn 4
  wait_report "Live datanodes (4)" 60
  kill -9 "${DN_PID[3]}"
  echo "killed 127.0.0.1:19863 at $(date -u +%H:%M:%S), spare 127.0.0.1:19864 already live"
  wait_report "Dead datanodes (1)" 120
  dead=$(date -u +%H:%M:%S)
  echo "dead at $dead"
  sleep 80
  python3 "$(dirname "${BASH_SOURCE[0]}")/asks.py" /tmp/logs/nn.log 127.0.0.1:19864 "$dead"
  report | grep -E "Under replicated|Live datanodes|Dead datanodes"
  stop_all
done
