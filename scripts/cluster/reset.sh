#!/usr/bin/env bash
# Remove the cluster containers and its volumes: every HDFS file is lost. Asks no confirmation.
# Usage: scripts/cluster/reset.sh            then scripts/cluster/up.sh and init-hdfs.sh
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

volumes="$(docker volume ls --quiet --filter label=com.docker.compose.project=gtdp | tr '\n' ' ')"
echo "Deleting the containers of compose project gtdp and its volumes: ${volumes:-none}"
compose down --volumes --remove-orphans
