#!/usr/bin/env bash
# Run lint + tests in the reproducible test container.
# Usage: scripts/test.sh [pytest args...]      e.g. scripts/test.sh tests/test_config.py -k env
set -euo pipefail

export MSYS_NO_PATHCONV=1
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST_ROOT="$(cd "$ROOT" && (pwd -W 2>/dev/null || pwd))"
IMAGE="${GTDP_TEST_IMAGE:-gtdp/test-runner:latest}"

docker build --quiet -f "$HOST_ROOT/docker/test-runner.Dockerfile" -t "$IMAGE" "$HOST_ROOT" >/dev/null
docker run --rm -v "$HOST_ROOT:/workspace" -w /workspace "$IMAGE" \
  sh -c 'ruff check src tests && ruff format --check src tests && pytest -p no:cacheprovider "$@"' sh "$@"
