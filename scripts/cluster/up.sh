#!/usr/bin/env bash
# Start the cluster and wait until every service is healthy and HDFS accepts writes.
# Usage: scripts/cluster/up.sh [docker compose up options]     e.g. --build after a Dockerfile change
#
# The first run builds the image (about 1 GB of Apache archives to download).
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

compose up --detach --wait --wait-timeout 600 "$@" ||
  die "a service did not become healthy; see: docker compose ps; docker compose logs <service>"
compose exec -T namenode hdfs dfsadmin -safemode wait >/dev/null
"$CLUSTER_DIR/status.sh"
