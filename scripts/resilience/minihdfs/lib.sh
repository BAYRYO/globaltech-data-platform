# shellcheck shell=bash
# A throwaway HDFS inside one container of the cluster image: one NameNode and DataNodes on
# 127.0.0.1, configuration and data under /tmp. After the scripts of review A of PR #24 (KAN-52).
set -u
export HADOOP_CONF_DIR=/tmp/conf HADOOP_LOG_DIR=/tmp/logs
export HDFS_NAMENODE_OPTS="-Xmx320m" HDFS_DATANODE_OPTS="-Xmx160m" HADOOP_CLIENT_OPTS="-Xmx200m"
declare -A DN_PID
NN_PID=

# write_conf AVOID_STALE MAX_STREAMS MULTIPLIER HARD_LIMIT RECHECK_MS
write_conf() {
  rm -rf /tmp/conf /tmp/logs /tmp/nn /tmp/dn* /tmp/hd
  mkdir -p /tmp/conf /tmp/logs
  cp /opt/hadoop/etc/hadoop/log4j.properties /tmp/conf/ 2>/dev/null || true
  cat >/tmp/conf/core-site.xml <<XML
<configuration>
<property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
<property><name>hadoop.tmp.dir</name><value>/tmp/hd</value></property>
</configuration>
XML
  cat >/tmp/conf/hdfs-site.xml <<XML
<configuration>
<property><name>dfs.replication</name><value>3</value></property>
<property><name>dfs.namenode.name.dir</name><value>file:///tmp/nn</value></property>
<property><name>dfs.permissions.enabled</name><value>false</value></property>
<property><name>dfs.heartbeat.interval</name><value>3</value></property>
<property><name>dfs.namenode.heartbeat.recheck-interval</name><value>$5</value></property>
<property><name>dfs.namenode.safemode.extension</name><value>0</value></property>
<property><name>dfs.client.block.write.replace-datanode-on-failure.best-effort</name><value>true</value></property>
<property><name>dfs.namenode.datanode.registration.ip-hostname-check</name><value>false</value></property>
<property><name>dfs.namenode.stale.datanode.interval</name><value>30000</value></property>
<property><name>dfs.namenode.avoid.write.stale.datanode</name><value>$1</value></property>
<property><name>dfs.namenode.avoid.read.stale.datanode</name><value>$1</value></property>
<property><name>dfs.namenode.replication.max-streams</name><value>$2</value></property>
<property><name>dfs.namenode.replication.work.multiplier.per.iteration</name><value>$3</value></property>
<property><name>dfs.namenode.replication.max-streams-hard-limit</name><value>$4</value></property>
<property><name>dfs.datanode.hostname</name><value>localhost</value></property>
</configuration>
XML
}

start_nn() {
  hdfs namenode -format -force -nonInteractive >/tmp/logs/format.log 2>&1
  hdfs namenode >/tmp/logs/nn.log 2>&1 &
  NN_PID=$!
}

# start_dn INDEX: a DataNode on 127.0.0.1:1986INDEX
start_dn() {
  local i=$1
  HADOOP_IDENT_STRING=dn$i HADOOP_PID_DIR=/tmp/pid$i hdfs datanode \
    -Ddfs.datanode.data.dir="file:///tmp/dn$i" -Ddfs.datanode.address="127.0.0.1:1986$i" \
    -Ddfs.datanode.http.address="127.0.0.1:1987$i" -Ddfs.datanode.ipc.address="127.0.0.1:1988$i" \
    >"/tmp/logs/dn$i.log" 2>&1 &
  DN_PID[$i]=$!
}

report() { hdfs dfsadmin -report 2>/dev/null; }

# wait_report PATTERN TIMEOUT_S
wait_report() {
  local deadline=$((SECONDS + $2))
  until report | grep -q "$1"; do
    if ((SECONDS >= deadline)); then
      echo "TIMEOUT waiting for: $1"
      return 1
    fi
    sleep 2
  done
}

stop_all() {
  kill -9 "$NN_PID" "${DN_PID[@]}" 2>/dev/null
  wait 2>/dev/null
  DN_PID=()
}

# allocations SINCE: the NameNode's block allocations logged at or after SINCE (HH:MM:SS)
allocations() {
  awk -v t="$1" '$2 >= t' /tmp/logs/nn.log | grep 'BLOCK\* allocate' |
    sed -E 's/^[0-9-]+ //; s/INFO hdfs.StateChange: //'
}
