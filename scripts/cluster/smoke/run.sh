#!/usr/bin/env bash
# End-to-end check of a running, initialized cluster: Spark on YARN seen by the Spark History
# Server, MapReduce seen by the JobHistory server, Hadoop Streaming with Python on the NodeManagers.
# Usage: scripts/cluster/smoke/run.sh
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

in_image || run_in_client "$@"

SMOKE=scripts/cluster/smoke
WORK=/user/etl/smoke

wait_json() {
  python3 - "$@" <<'EOF'
import json
import sys
import time
import urllib.request

url, path, expected = sys.argv[1:4]
value = None
for _ in range(45):
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            value = json.load(resp)
        for key in path.split("."):
            value = value[int(key)] if isinstance(value, list) else value[key]
        if str(value) == expected:
            print(f"  {url} -> {path} = {value}")
            sys.exit(0)
    except (OSError, KeyError, IndexError, ValueError) as err:
        value = err
    time.sleep(2)
sys.exit(f"{url}: {path} is {value!r}, expected {expected}")
EOF
}

run_logged() {
  local log=$1
  shift
  "$@" >"$log" 2>&1 || {
    tail -n 60 "$log" >&2
    die "failed: $*"
  }
}

echo "==> Spark on YARN"
run_logged /tmp/spark.log bash scripts/cluster/submit.sh "$SMOKE/spark_smoke.py" "$WORK/spark"
grep '^GTDP_SMOKE' /tmp/spark.log
app_id="$(grep -o 'app_id=[^ ]*' /tmp/spark.log | cut -d= -f2)"
yarn application -status "$app_id" 2>/dev/null | grep -E '^\s*(State|Final-State) :'
yarn application -status "$app_id" 2>/dev/null | grep -q 'Final-State : SUCCEEDED' ||
  die "$app_id did not succeed on YARN"
wait_json "http://spark-history:18080/api/v1/applications/$app_id" attempts.0.completed True

echo "==> MapReduce pi"
run_logged /tmp/pi.log yarn jar "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar" pi 2 1000
grep 'Estimated value of Pi' /tmp/pi.log
job_id="$(grep -o 'job_[0-9]*_[0-9]*' /tmp/pi.log | head -n 1)"
wait_json "http://mr-history:19888/ws/v1/history/mapreduce/jobs/$job_id" job.state SUCCEEDED

echo "==> Hadoop Streaming with Python"
printf 'spark yarn hdfs\nyarn hdfs\nhdfs\n' >/tmp/words.txt
hdfs dfs -mkdir -p "$WORK/streaming"
hdfs dfs -put -f /tmp/words.txt "$WORK/streaming/input.txt"
hdfs dfs -rm -r -f -skipTrash "$WORK/streaming/output"
run_logged /tmp/streaming.log hadoop jar "$HADOOP_STREAMING_JAR" \
  -D mapreduce.job.name=gtdp-smoke-streaming \
  -files "$SMOKE/wordcount_mapper.py,$SMOKE/wordcount_reducer.py" \
  -mapper "python3 wordcount_mapper.py" -reducer "python3 wordcount_reducer.py" \
  -input "$WORK/streaming/input.txt" -output "$WORK/streaming/output"
result="$(hdfs dfs -cat "$WORK/streaming/output/part-*")"
echo "$result"
grep -qx $'hdfs\t3' <<<"$result" || die "streaming word count is wrong"
grep -q '^_python_3\.11\.' <<<"$result" || die "streaming mappers did not run Python 3.11"

echo "==> smoke OK"
