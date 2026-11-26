#!/usr/bin/env bash
# Ask a running streaming-sensors job to stop between two micro-batches.
# Usage: scripts/streaming/stop.sh [ROOT]
#
# Creates the stop marker ROOT/streaming/control/stop-streaming-sensors (ROOT is /datalake by
# default, the configured streaming area). The job sees it within one trigger interval, stops its
# queries, writes a SUCCESS audit row with stop_reason "stop marker" and deletes the marker.
set -euo pipefail
export MSYS_NO_PATHCONV=1

root=${1:-/datalake}
control=${root%/}/streaming/control
exec "$(dirname "${BASH_SOURCE[0]}")/../cluster/client.sh" bash -c \
  "hdfs dfs -mkdir -p '$control' && hdfs dfs -touchz '$control/stop-streaming-sensors'"
