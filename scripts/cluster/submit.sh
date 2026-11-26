#!/usr/bin/env bash
# Package src/gtdp into dist/gtdp.zip and submit a PySpark application to YARN.
# Usage: scripts/cluster/submit.sh <job-name | path/to/script.py> [args...]
#
#   path/to/script.py   submitted as is (path relative to the repository root)
#   job-name            runs src/gtdp/jobs/run.py <job-name> --env cluster [args...]
#
#   GTDP_DEPLOY_MODE         client (default: driver in the client container) or cluster
#                            (driver in a YARN container: conf/ and GTDP_* go with it)
#   GTDP_SPARK_SUBMIT_OPTS   extra spark-submit options, split on spaces, e.g.
#                            GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=3"
#   GTDP_PSEUDO_SALT         pseudonymisation salt, from .env or the environment
#   HADOOP_USER_NAME         submitting user (default etl)
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

(($#)) || usage
in_image || run_in_client "$@"

target=$1
shift
mode=${GTDP_DEPLOY_MODE:-client}

python3 - "$mode" <<'EOF'
import os
import sys
import zipfile


def zip_dir(src, archive, root):
    with zipfile.ZipFile(archive + ".tmp", "w", zipfile.ZIP_DEFLATED) as zf:
        for folder, dirs, files in os.walk(src):
            dirs[:] = sorted(d for d in dirs if d != "__pycache__")
            for name in sorted(files):
                path = os.path.join(folder, name)
                zf.write(path, os.path.relpath(path, root))
    os.replace(archive + ".tmp", archive)


os.makedirs("dist", exist_ok=True)
zip_dir("src/gtdp", "dist/gtdp.zip", "src")
if sys.argv[1] == "cluster" and os.path.isdir("conf"):
    zip_dir("conf", "dist/conf.zip", "conf")
EOF
# The container runs as root: Hadoop's login fails for a host uid that has no passwd entry.
# Give dist/ back to the checkout's owner.
chown -R --reference=. dist 2>/dev/null || true

if [[ $target == *.py ]]; then
  app=("$target" "$@")
else
  [[ -n ${GTDP_PSEUDO_SALT:-} ]] ||
    echo "warning: GTDP_PSEUDO_SALT is not set (.env or export): --env cluster jobs exit with code 2" >&2
  app=(src/gtdp/jobs/run.py "$target" --env cluster "$@")
fi

opts=(--master yarn --deploy-mode "$mode" --py-files dist/gtdp.zip)
if [[ $mode == cluster ]]; then
  # GTDP_* (the salt among them) reach the driver as spark.yarn.appMasterEnv.* entries of a
  # private properties file, never on the command line, which ps shows to every user.
  # --properties-file replaces spark-defaults.conf, so the file starts with it.
  props="$(mktemp)"
  trap 'rm -f "$props"' EXIT
  {
    cat "$SPARK_CONF_DIR/spark-defaults.conf"
    echo
    if [[ -f dist/conf.zip ]]; then echo "spark.yarn.appMasterEnv.GTDP_CONF_DIR=conf"; fi
    while IFS= read -r var; do
      case $var in
        GTDP_CONF_DIR | GTDP_DEPLOY_MODE | GTDP_SPARK_SUBMIT_OPTS | GTDP_CLIENT_STDIN) ;;
        *)
          value=${!var}
          [[ $value != *$'\n'* ]] || die "$var contains a line break"
          printf 'spark.yarn.appMasterEnv.%s=%s\n' "$var" "${value//\\/\\\\}"
          ;;
      esac
    done < <(compgen -e | grep '^GTDP_' || true)
  } >"$props"
  opts+=(--properties-file "$props")
  if [[ -f dist/conf.zip ]]; then opts+=(--archives dist/conf.zip#conf); fi
fi
read -r -a extra <<<"${GTDP_SPARK_SUBMIT_OPTS:-}"

spark-submit "${opts[@]}" "${extra[@]}" "${app[@]}"
