#!/usr/bin/env bash
# Lot 6, Hadoop Streaming side: revenue per country and invalid-line counters of the orders
# landed up to a date. Replaces the output directory, prints the result, then job_id=<id>.
# Usage: scripts/mapreduce/run_mr.sh <landing-root> <date> <output-dir> [-D key=value ...]
#
#   landing-root  HDFS directory with orders/ and customers/ partitions ingestion_date=YYYY-MM-DD,
#                 for example /datalake/landing
#   date          YYYY-MM-DD: every partition up to this date is read; orders must have this one
#   output-dir    HDFS output directory of the job, deleted first
#   -D key=value  extra job settings (compare.sh --mr-conf), e.g. -D mapreduce.job.maps=8
#
# The customers files go to each mapper through the distributed cache (-files): a map-side join.
# The counters are in the JobHistory server (group Quality, invalid lines by reason).
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../cluster/lib.sh"

(($# >= 3)) || usage
in_image || run_in_client "$@"

landing=$1 date=$2 output=$3
shift 3
[[ $landing == *://* ]] || landing="$(hdfs getconf -confKey fs.defaultFS)$landing"

# The ingestion_date partitions of $1 up to $date, oldest first.
partitions() {
  local dir
  hdfs dfs -ls -C "$landing/$1" | while read -r dir; do
    if [[ $dir == */ingestion_date=* && ! ${dir##*=} > $date ]]; then echo "$dir"; fi
  done
}

mapfile -t order_dirs < <(partitions orders)
mapfile -t customer_dirs < <(partitions customers)
[[ ${#order_dirs[@]} -gt 0 && ${order_dirs[-1]##*=} == "$date" ]] ||
  die "no orders landed in $landing/orders/ingestion_date=$date"
((${#customer_dirs[@]})) || die "no customers landed in $landing/customers up to $date"
mapfile -t customer_files < <(hdfs dfs -ls -C "${customer_dirs[@]}" | grep -v '/[_.][^/]*$')

inputs=()
for dir in "${order_dirs[@]}"; do inputs+=(-input "$dir"); done
files=$(IFS=,; echo "$ROOT/mapreduce/mapper.py,$ROOT/mapreduce/reducer.py,${customer_files[*]}")
log=$(mktemp)
trap 'rm -f "$log"' EXIT

hdfs dfs -rm -r -f -skipTrash "$output" >/dev/null
hadoop jar "$HADOOP_STREAMING_JAR" \
  -D mapreduce.job.name=gtdp-revenue-by-country-mr \
  -D mapreduce.job.reduces=1 \
  "$@" \
  -files "$files" \
  -mapper "python3 mapper.py ${customer_files[*]##*/}" \
  -combiner "python3 reducer.py --combine" \
  -reducer "python3 reducer.py" \
  "${inputs[@]}" -output "$output" 2>&1 | tee "$log" >&2

hdfs dfs -cat "$output/part-*"
echo "job_id=$(grep -o -m 1 'job_[0-9]*_[0-9]*' "$log")"
