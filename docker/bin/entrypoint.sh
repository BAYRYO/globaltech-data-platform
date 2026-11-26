#!/usr/bin/env bash
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
    # Spark's History Server exits at startup if its log directory does not exist.
    export HADOOP_USER_NAME=hadoop
    hdfs dfsadmin -safemode wait
    hdfs dfs -mkdir -p /spark-logs
    unset HADOOP_USER_NAME
    exec spark-class org.apache.spark.deploy.history.HistoryServer
    ;;
  *) exec "$@" ;;
esac
