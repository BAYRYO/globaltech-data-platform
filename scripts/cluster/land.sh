#!/usr/bin/env bash
# Put one generator drop into the HDFS landing zone, replacing what a previous run left there.
# Usage: scripts/cluster/land.sh <drop_dir> <ingestion_date>
#   e.g. scripts/cluster/land.sh data/generated/dev/landing/2026-01-01 2026-01-01
#
# <drop_dir> must be inside this checkout (it is mounted in the client container). Nothing is
# written unless <ingestion_date> is a real YYYY-MM-DD date and <drop_dir>/_MANIFEST.json has the
# same ingestion_date and lists exactly the files of the drop, each under a source folder (orders,
# customers, products, weblogs).
#
# The landing of a date is /datalake/landing/<name>/ingestion_date=<date>/ for each source and for
# _manifests, which holds _MANIFEST.json. The whole drop is staged first. Then the published
# landing of the date moves aside and the staged one moves into place, manifest last. A failure
# puts the previous landing back, and the next run does it for a run that was killed, so the date
# holds its previous landing or the new one, complete.
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

(($# == 2)) || usage
if ! in_image; then
  drop="$(cd "$1" && pwd)" || die "no such directory: $1"
  [[ $drop == "$ROOT"/* ]] || die "$1 is outside the checkout $ROOT"
  run_in_client "${drop#"$ROOT"/}" "$2"
fi

drop=$1 day=$2
SOURCES=(orders customers products weblogs)
NAMES=("${SOURCES[@]}" _manifests)
LANDING=/datalake/landing
staging=$LANDING/_staging/ingestion_date=$day
target() { echo "$LANDING/$1/ingestion_date=$day"; }
targets=()
for name in "${NAMES[@]}"; do targets+=("$(target "$name")"); done

on_exit() {
  rm -rf "$local_dir"
  if [[ -n ${dirty:-} ]]; then
    echo "==> failed: restoring the previous landing of $day" >&2
    rollback || die "restore incomplete, $staging still holds the previous landing: rerun land.sh"
    exit 1
  fi
}
local_dir="$(mktemp -d)"
trap on_exit EXIT
trap 'exit 1' INT TERM

# Check the drop against its manifest, then lay it out as the staging area holds it:
# <name>/ for each source (a symlink, since hdfs dfs -put cannot read a path with a space) and
# _manifests/_MANIFEST.json.
python3 - "$drop" "$day" "$local_dir" "${SOURCES[@]}" <<'EOF'
import datetime
import json
import pathlib
import shutil
import sys

drop, day, out = pathlib.Path(sys.argv[1]), sys.argv[2], pathlib.Path(sys.argv[3])
sources = sys.argv[4:]


def refuse(message):
    sys.exit(f"error: {message}")


try:
    real_date = datetime.date.fromisoformat(day).isoformat() == day
except ValueError:
    real_date = False
if not real_date:
    refuse(f"ingestion_date must be a real date written YYYY-MM-DD, got {day!r}")
manifest = drop / "_MANIFEST.json"
if not manifest.is_file():
    refuse(f"no _MANIFEST.json in {drop}")
try:
    content = json.loads(manifest.read_text(encoding="utf-8"))
    manifest_day = content["ingestion_date"]
    listed = {entry["path"] for entry in content["files"]}
    if not all(isinstance(path, str) for path in listed):
        raise TypeError("a path is not a string")
except (ValueError, KeyError, TypeError) as err:
    refuse(f"{manifest} is not a drop manifest: {err!r}")
if manifest_day != day:
    refuse(f"{manifest} describes the drop of {manifest_day}, not of {day}")

present = {p.relative_to(drop).as_posix() for p in drop.rglob("*") if p.is_file()}
present.discard("_MANIFEST.json")
problems = [f"listed but missing: {path}" for path in sorted(listed - present)]
problems += [f"present but not listed: {path}" for path in sorted(present - listed)]
problems += [f"not in a source folder: {path}" for path in sorted(listed) if path.split("/")[0] not in sources]
if not listed:
    problems.append("the manifest lists no file")
if problems:
    refuse(f"{drop} does not match its manifest\n  " + "\n  ".join(problems))

for source in sources:
    if any(path.split("/")[0] == source for path in listed):
        (out / source).symlink_to((drop / source).resolve())
(out / "_manifests").mkdir()
shutil.copy(manifest, out / "_manifests")
print(f"==> {drop}: {len(listed)} files, as listed by its manifest for {day}")
EOF
staged=()
for name in "${NAMES[@]}"; do [[ -e $local_dir/$name ]] && staged+=("$name"); done

# One listing: which partitions of the date are published (published[name]), and what a run that
# died left in the staging area (old[name] moved aside, new[name] not yet moved into place).
read_state() {
  local out path name listed=''
  published=() old=() new=() has_staging='' has_placing=''
  out="$(hdfs dfs -ls -d "$LANDING" "${targets[@]}" "$staging" "$staging/placing" \
    "$staging/old/*" "$staging/new/*" 2>/dev/null)" || true
  while read -r path; do
    case $path in
      "$LANDING") listed=1 ;;
      "$staging") has_staging=1 ;;
      "$staging/placing") has_placing=1 ;;
      "$staging"/old/*) old[${path##*/}]=1 ;;
      "$staging"/new/*) new[${path##*/}]=1 ;;
      "$LANDING"/*) name=${path#"$LANDING"/} && published[${name%%/*}]=1 ;;
    esac
  done < <(awk '{ print $NF }' <<<"$out")
  [[ -n $listed ]] || die "cannot list $LANDING"
}
declare -A published old new

# Put back the landing published before the run that left $staging. It starts from the state it
# reads, so a rollback cut short completes on the next run. Once placing exists, every published
# partition of the date was moved in by that run.
rollback() {
  local name
  read_state
  [[ -n $has_staging ]] || return 0
  if [[ -n $has_placing ]]; then
    for name in "${NAMES[@]}"; do
      [[ -z ${published[$name]:-} || -n ${new[$name]:-} ]] ||
        hdfs dfs -mv "$(target "$name")" "$staging/new/$name" || return 1
    done
  fi
  for name in "${NAMES[@]}"; do
    [[ -z ${old[$name]:-} ]] || hdfs dfs -mv "$staging/old/$name" "$(target "$name")" || return 1
  done
  hdfs dfs -rm -r -skipTrash "$staging" >/dev/null
}

read_state
if [[ -n $has_staging ]]; then
  echo "==> an earlier run for $day did not finish: restoring the landing it replaced"
  rollback || die "restore incomplete: rerun land.sh"
  read_state
fi

dirty=1
echo "==> staging ${staged[*]} in $staging"
hdfs dfs -mkdir -p "$staging/new" "$staging/old"
hdfs dfs -put "$local_dir"/* "$staging/new/"

echo "==> publishing"
for ((i = ${#NAMES[@]} - 1; i >= 0; i--)); do
  name=${NAMES[i]}
  [[ -z ${published[$name]:-} ]] || hdfs dfs -mv "$(target "$name")" "$staging/old/$name"
done
hdfs dfs -mkdir "$staging/placing"
for name in "${staged[@]}"; do
  hdfs dfs -mv "$staging/new/$name" "$(target "$name")"
done
hdfs dfs -rm -r -skipTrash "$staging" >/dev/null
dirty=

for name in "${staged[@]}"; do placed+=("$(target "$name")"); done
hdfs dfs -count -h "${placed[@]}"
