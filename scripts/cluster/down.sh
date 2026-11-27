#!/usr/bin/env bash
# Stop and remove the cluster containers. HDFS data stays in the volumes for the next up.sh.
# Usage: scripts/cluster/down.sh
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

compose --profile spare down --remove-orphans
