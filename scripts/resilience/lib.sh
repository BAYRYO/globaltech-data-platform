# shellcheck shell=bash disable=SC2154
# Helpers of the DataNode failure scripts. A script sets out (its output folder, relative to the
# repository root) before it calls them. The poller (hdfs_watch.py poll) writes $out/timeline.csv;
# event appends to $out/events.csv; the waits read the last timeline row sampled without error.
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../cluster/lib.sh"

watch=scripts/resilience/hdfs_watch.py
poller=

# on_exit FUNCTION: run FUNCTION when the script ends, after an error, Ctrl-C or SIGTERM too.
on_exit() {
  # shellcheck disable=SC2064
  trap "$1" EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
}

container() { echo "gtdp-$1-1"; }
running() { [[ $(docker inspect -f '{{.State.Running}}' "$(container "$1")" 2>/dev/null) == true ]]; }
admin() { HADOOP_USER_NAME=hadoop "$CLUSTER_DIR/client.sh" "$@"; }
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

event() {
  local when
  when=$(now)
  echo "$when,$1" >>"$out/events.csv"
  echo "[$when] $1"
}

snapshot() {
  admin hdfs dfsadmin -report >"$out/dfsadmin-$1.txt"
  admin hdfs fsck / >"$out/fsck-$1.txt" 2>&1 || true
}

# Value of a column in the last timeline row sampled without error.
latest() {
  awk -F, -v col="$1" 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == col) k = i; e = NF; next }
    $e == "" { v = $k } END { print v }' "$out/timeline.csv"
}
is() { [[ $(latest "$1") == "$2" ]]; }
zero() {
  local column
  for column; do [[ $(latest "$column") == 0 ]] || return 1; done
}
nominal() { is live 3 && zero under_replicated missing corrupt ec_low_redundancy ec_missing ec_corrupt; }

# start_poller HOSTS: sample HDFS every 5 s until stop_poller, return once the first row is in.
start_poller() {
  admin python3 "$watch" poll --out "$out/timeline.csv" --hosts "$1" --stop-file "$out/stop" \
    --max-seconds 43200 >"$out/poller.log" 2>&1 &
  poller=$!
  until [[ -n $(latest live 2>/dev/null) ]]; do
    kill -0 "$poller" 2>/dev/null || die "the poller stopped: $out/poller.log"
    sleep 2
  done
}

stop_poller() {
  touch "$out/stop"
  if [[ -n $poller ]]; then wait "$poller" || true; fi
  poller=
}

# reached EVENT TIMEOUT CONDITION...: record EVENT once CONDITION holds, false after TIMEOUT s.
reached() {
  local what=$1 timeout=$2 deadline
  shift 2
  deadline=$((SECONDS + timeout))
  until "$@"; do
    if ((SECONDS >= deadline)); then
      event "no_${what}_after_${timeout}s"
      return 1
    fi
    [[ -z $poller ]] || kill -0 "$poller" 2>/dev/null || die "the poller stopped: $out/poller.log"
    sleep 5
  done
  event "$what"
}
wait_for() { reached "$@" || die "no $1 after $2 s"; }

# Stop what "client.sh ... &" or "submit.sh ... &" started, given its $!: client.sh names its
# container after its own PID, which a background job keeps through its execs.
stop_client() {
  [[ -n $1 ]] || return 0
  local id
  for id in $(docker ps -q --filter "name=^gtdp-client-$1-" 2>/dev/null); do
    docker stop "$id" >/dev/null 2>&1 || true
  done
  kill "$1" 2>/dev/null || true
}
