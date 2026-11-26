#!/usr/bin/env bash
# Run a command in a one-off client container on the cluster network (interactive bash if none).
# Usage: scripts/cluster/client.sh [command...]
#
# The checkout this script belongs to is mounted at /opt/project, the working directory.
#   .env              read if present (KEY=VALUE lines, no quotes), e.g. GTDP_PSEUDO_SALT=...
#   HADOOP_USER_NAME  HDFS and YARN user (default etl)
#   GTDP_*            exported variables are forwarded and win over .env, except
#                     GTDP_CONF_DIR, always /opt/project/conf
#   GTDP_GIT_SHA      defaults to the checkout's commit (git rev-parse --short HEAD)
#   GTDP_CLIENT_STDIN 1 forwards stdin to the command, e.g. to pipe a file into it. Otherwise only
#                     a terminal's stdin is, so a caller's `while read` loop keeps its input.
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

GTDP_IMAGE=gtdp/hadoop-spark:3.3.6-3.5.3
GTDP_NETWORK=gtdp

in_image && die "already inside the cluster image"
docker network inspect "$GTDP_NETWORK" >/dev/null 2>&1 ||
  die "network '$GTDP_NETWORK' not found: start the cluster with scripts/cluster/up.sh"

# A resolvable name: Spark executors connect back to a client-mode driver by its hostname.
name="gtdp-client-$$-$RANDOM"
args=(--rm --network "$GTDP_NETWORK" --name "$name" --hostname "$name"
  -v "$(host_path "$ROOT"):/opt/project" -w /opt/project
  -e "HADOOP_USER_NAME=${HADOOP_USER_NAME:-etl}" -e PYTHONPATH=/opt/project/src)
[[ -f $ROOT/.env ]] && args+=(--env-file "$(host_path "$ROOT")/.env")
GTDP_GIT_SHA="${GTDP_GIT_SHA:-$(cd "$ROOT" && git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
export GTDP_GIT_SHA
while IFS= read -r var; do
  [[ $var == GTDP_CONF_DIR ]] || args+=(-e "$var")
done < <(compgen -e | grep '^GTDP_' || true)
args+=(-e GTDP_CONF_DIR=/opt/project/conf)

if [[ -t 0 || ${GTDP_CLIENT_STDIN:-} == 1 ]]; then args+=(-i); fi
launcher=()
if [[ -t 0 && -t 1 ]]; then
  args+=(-t)
  # The default Git Bash window (mintty) is not a Windows console: docker needs winpty for a TTY.
  if [[ ${TERM_PROGRAM:-} == mintty ]] && command -v winpty >/dev/null; then launcher=(winpty); fi
fi

(($#)) || set -- bash
exec ${launcher[@]+"${launcher[@]}"} docker run "${args[@]}" "$GTDP_IMAGE" "$@"
