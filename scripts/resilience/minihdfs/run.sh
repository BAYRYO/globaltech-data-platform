#!/usr/bin/env bash
# Run an experiment of this folder in a throwaway container of the cluster image: no network, no
# cluster container touched, one NameNode and its DataNodes on 127.0.0.1 inside the container. The
# container is named localhost: without a network, Hadoop cannot resolve a random hostname.
# About 1.2 GB of memory and 3 to 10 minutes each.
# Usage: scripts/resilience/minihdfs/run.sh stale 3|4
#        scripts/resilience/minihdfs/run.sh rate
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../../cluster/lib.sh"
[[ ${1:-} == stale || ${1:-} == rate ]] || usage
dir=$(host_path "$(dirname "${BASH_SOURCE[0]}")")
exec docker run --rm --network none --hostname localhost --name "minihdfs-$1-$$" \
  --entrypoint bash -v "$dir:/exp:ro" gtdp/hadoop-spark:3.3.6-3.5.3 "/exp/$1.sh" "${@:2}"
