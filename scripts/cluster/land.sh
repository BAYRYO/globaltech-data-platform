#!/usr/bin/env bash
# Put one generator drop into the HDFS landing zone, replacing what a previous run left there.
# Usage: scripts/cluster/land.sh <drop_dir> <ingestion_date>
#   e.g. scripts/cluster/land.sh data/generated/dev/landing/2026-01-01 2026-01-01
#
# <drop_dir> must be inside this checkout (it is mounted in the client container). Each source
# folder (orders, customers, products, weblogs) replaces
# /datalake/landing/<source>/ingestion_date=<date>/ and _MANIFEST.json replaces
# /datalake/landing/_manifests/ingestion_date=<date>/. Files are staged under
# /datalake/landing/_staging and renamed into place, so a partition is either the old or the
# complete new one.
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

(($# == 2)) || usage
if ! in_image; then
  drop="$(cd "$1" && pwd)" || die "no such directory: $1"
  [[ $drop == "$ROOT"/* ]] || die "$1 is outside the checkout $ROOT"
  run_in_client "${drop#"$ROOT"/}" "$2"
fi

drop=$1 day=$2
[[ $day =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "ingestion_date must be YYYY-MM-DD, got $day"

staging=/datalake/landing/_staging/ingestion_date=$day
entries=() targets=() moves=()
for source in orders customers products weblogs; do
  [[ -d $drop/$source ]] || continue
  entries+=("$drop/$source")
  targets+=("/datalake/landing/$source/ingestion_date=$day")
  moves+=("$staging/$source /datalake/landing/$source/ingestion_date=$day")
done
((${#entries[@]})) || die "no source folder (orders, customers, products, weblogs) in $drop"
manifest_dir=/datalake/landing/_manifests/ingestion_date=$day
[[ -f $drop/_MANIFEST.json ]] || die "no _MANIFEST.json in $drop"
entries+=("$drop/_MANIFEST.json")
targets+=("$manifest_dir")
moves+=("$staging/_MANIFEST.json $manifest_dir/_MANIFEST.json")

echo "==> staging ${#entries[@]} entries of $drop in $staging"
hdfs dfs -rm -r -f -skipTrash "$staging"
hdfs dfs -mkdir -p "$staging"
hdfs dfs -put "${entries[@]}" "$staging/"

echo "==> replacing ${targets[*]}"
hdfs dfs -rm -r -f -skipTrash "${targets[@]}"
hdfs dfs -mkdir -p "$manifest_dir"
for move in "${moves[@]}"; do
  read -r from to <<<"$move"
  hdfs dfs -mv "$from" "$to"
done
hdfs dfs -rm -r -f -skipTrash "$staging"

hdfs dfs -count -h "${targets[@]}"
