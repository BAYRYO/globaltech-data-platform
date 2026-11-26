#!/usr/bin/env bash
# Start the daemon for a compose role, in the foreground. Anything else runs as a plain command.
set -euo pipefail

case "${1:-}" in
  namenode)
    if [[ ! -f /hadoop/dfs/name/current/VERSION ]]; then
      hdfs namenode -format -nonInteractive -clusterId gtdp
    fi
    exec hdfs namenode
    ;;
  datanode) exec hdfs datanode ;;
  resourcemanager) exec yarn resourcemanager ;;
  nodemanager) exec yarn nodemanager ;;
  mr-history) exec mapred historyserver ;;
  spark-history)
    # The History Server exits if its log directory is missing, and it starts before
    # init-hdfs.sh. init-hdfs.sh then sets the owner, mode and quota.
    export HADOOP_USER_NAME=hadoop
    hdfs dfsadmin -safemode wait
    hdfs dfs -mkdir -p /spark-logs
    unset HADOOP_USER_NAME
    exec spark-class org.apache.spark.deploy.history.HistoryServer
    ;;
  *) exec "$@" ;;
esac
