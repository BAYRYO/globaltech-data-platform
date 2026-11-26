# shellcheck shell=bash
set -euo pipefail
export MSYS_NO_PATHCONV=1

CLUSTER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$CLUSTER_DIR/../.." && pwd)"

die() {
  echo "error: $*" >&2
  exit 1
}

usage() {
  awk 'NR == 1 { next } !/^#/ { exit } !/^# shellcheck/ { sub(/^# ?/, ""); print }' "$0" >&2
  exit 2
}

in_image() { [[ -n "${GTDP_IN_IMAGE:-}" ]]; }

# Docker Desktop on Windows wants C:/... paths; MSYS_NO_PATHCONV keeps Git Bash from rewriting them.
host_path() { (cd "$1" && (pwd -W 2>/dev/null || pwd)); }

compose() { (cd "$ROOT" && docker compose "$@"); }

run_in_client() {
  local script
  script="$(cd "$(dirname "$0")" && pwd)/${0##*/}"
  exec "$CLUSTER_DIR/client.sh" bash "${script#"$ROOT"/}" "$@"
}
