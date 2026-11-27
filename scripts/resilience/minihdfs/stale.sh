#!/usr/bin/env bash
# Does dfs.namenode.avoid.write.stale.datanode keep a stale DataNode out of new write pipelines?
# Usage, in the cluster image (run.sh): stale.sh DATANODES   (3 or 4, replication 3)
# One DataNode is killed; 45 s later (stale after 30 s, dead after 630 s) 8 one-block files are
# written, the setting true, then false. Prints the first allocation of each file.
# shellcheck source=scripts/resilience/minihdfs/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
count=${1:?DATANODES}
echo x >/tmp/f
for avoid in true false; do
  echo "=== $count DataNodes, replication 3, avoid.write.stale.datanode=$avoid"
  write_conf "$avoid" 2 2 4 300000
  start_nn
  for i in $(seq 1 "$count"); do start_dn "$i"; done
  wait_report "Live datanodes ($count)" 90 || exit 1
  kill -9 "${DN_PID[3]}"
  killed=$(date -u +%H:%M:%S)
  echo "killed 127.0.0.1:19863 at $killed; files written 45 s later"
  sleep 45
  for i in 1 2 3 4 5 6 7 8; do hdfs dfs -put /tmp/f "/after$i" 2>/dev/null; done
  first=$(allocations "$killed" | awk '!($NF in seen) { seen[$NF] = 1; print }')
  echo "first allocations naming the stale node: $(grep -c 19863 <<<"$first") of 8 files"
  echo "$first"
  stop_all
done
