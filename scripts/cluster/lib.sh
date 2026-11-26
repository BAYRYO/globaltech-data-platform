# shellcheck shell=bash
# Sourced by scripts/cluster/*.sh. A script that talks to HDFS or YARN runs its body inside the
# cluster image: from the host it re-runs itself in a client container (run_in_client).
set -euo pipefail
export MSYS_NO_PATHCONV=1

CLUSTER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$CLUSTER_DIR/../.." && pwd)"

die() {
  echo "error: $*" >&2
  exit 1
}

# Print the calling script's header comment (its usage) and exit.
usage() {
  awk 'NR == 1 { next } !/^#/ { exit } !/^# shellcheck/ { sub(/^# ?/, ""); print }' "$0" >&2
  exit 2
}

in_image() { [[ -n "${GTDP_IN_IMAGE:-}" ]]; }

# Docker Desktop on Windows wants C:/... paths; MSYS_NO_PATHCONV keeps Git Bash from rewriting them.
host_path() { (cd "$1" && (pwd -W 2>/dev/null || pwd)); }

compose() { (cd "$ROOT" && docker compose "$@"); }

# Re-run the calling script with the same arguments in a client container (never returns).
run_in_client() {
  local script
  script="$(cd "$(dirname "$0")" && pwd)/${0##*/}"
  exec "$CLUSTER_DIR/client.sh" bash "${script#"$ROOT"/}" "$@"
}
