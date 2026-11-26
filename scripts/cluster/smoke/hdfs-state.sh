#!/usr/bin/env bash
# Print what init-hdfs.sh manages (modes, owners, ACLs, quotas, erasure coding) in a stable form,
# without timestamps, to prove that a second init-hdfs.sh run changes nothing.
# Usage: scripts/cluster/smoke/hdfs-state.sh
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

export HADOOP_USER_NAME=hadoop
in_image || run_in_client "$@"

hdfs dfs -ls -R / | awk '{print $1, $3, $4, $5, $8}'
hdfs dfs -getfacl -R /datalake
hdfs dfs -count -q '/datalake/*' /spark-logs /benchmarks
for path in /benchmarks/rep3 /benchmarks/ec; do
  echo "$path: $(hdfs ec -getPolicy -path "$path")"
done
hdfs ec -listPolicies | grep -i 'XOR-2-1-1024k'
