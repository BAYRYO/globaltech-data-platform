#!/usr/bin/env bash
# Create the HDFS tree with its owners, modes, ACLs, space quotas and erasure coding policies
# (docs/architecture.md, docs/platform.md), and upload the Spark jars archive.
# Usage: scripts/cluster/init-hdfs.sh
#
# Idempotent: every step sets a target state, so a second run changes nothing.
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

export HADOOP_USER_NAME=hadoop # HDFS superuser
in_image || run_in_client "$@"

# path                            owner          mode  quota  erasure-coding  acl
TREE="
/datalake                         etl:datalake   755   -      -               -
/datalake/landing                 etl:datalake   770   20g    -               -
/datalake/landing/orders          etl:datalake   770   -      -               -
/datalake/landing/customers       etl:datalake   770   -      -               -
/datalake/landing/products        etl:datalake   770   -      -               -
/datalake/landing/weblogs         etl:datalake   770   -      -               -
/datalake/landing/_manifests      etl:datalake   770   -      -               -
/datalake/landing/sensors         etl:datalake   770   -      -               -
/datalake/landing/sensors/incoming etl:datalake  770   -      -               -
/datalake/bronze                  etl:datalake   750   30g    -               -
/datalake/silver                  etl:datalake   750   30g    -               group:analysts:r-x,default:group:analysts:r-x
/datalake/silver/rejects          etl:datalake   750   -      -               group:analysts:---,default:group:analysts:---
/datalake/gold                    etl:datalake   755   10g    -               group:analysts:r-x,default:group:analysts:r-x
/datalake/streaming               etl:datalake   750   5g     -               -
/datalake/streaming/checkpoints   etl:datalake   750   -      -               -
/datalake/streaming/rejects       etl:datalake   750   -      -               -
/datalake/ops                     etl:datalake   750   2g     -               group:ops:r-x,default:group:ops:r-x
/spark-logs                       spark:hadoop   1777  5g     -               -
/app-logs                         hadoop:hadoop  1777  -      -               -
/mr-history                       hadoop:hadoop  755   -      -               -
/tmp                              hadoop:hadoop  1777  -      -               -
/user                             hadoop:hadoop  755   -      -               -
/user/etl                         etl:datalake   755   -      -               -
/benchmarks                       etl:datalake   755   20g    -               -
/benchmarks/rep3                  etl:datalake   755   -      replicate       -
/benchmarks/ec                    etl:datalake   755   -      XOR-2-1-1024k   -
/apps                             hadoop:hadoop  755   -      -               -
/apps/spark                       hadoop:hadoop  755   -      -               -
"
SPARK_ARCHIVE=/apps/spark/spark-jars-3.5.3.zip # spark.yarn.archive in spark-defaults.conf

paths=() acls=() ec=()
declare -A by_owner by_mode by_quota
while read -r path owner mode quota policy acl; do
  [[ -n $path ]] || continue
  paths+=("$path")
  by_owner[$owner]+=" $path"
  by_mode[$mode]+=" $path"
  [[ $quota == - ]] || by_quota[$quota]+=" $path"
  [[ $policy == - ]] || ec+=("$path $policy")
  [[ $acl == - ]] || acls+=("$path $acl")
done <<<"$TREE"

# Run "<command...> <paths>" once per group, e.g. one chown per owner.
for_groups() {
  local -n groups=$1
  shift
  local key targets
  for key in "${!groups[@]}"; do
    read -r -a targets <<<"${groups[$key]}"
    "$@" "$key" "${targets[@]}"
  done
}

hdfs dfsadmin -safemode wait >/dev/null

echo "==> directories"
hdfs dfs -mkdir -p "${paths[@]}"
for_groups by_owner hdfs dfs -chown
for_groups by_mode hdfs dfs -chmod

echo "==> ACLs"
for entry in "${acls[@]}"; do
  read -r path acl <<<"$entry"
  hdfs dfs -setfacl -m "$acl" "$path"
done

echo "==> space quotas"
for_groups by_quota hdfs dfsadmin -setSpaceQuota

echo "==> erasure coding"
hdfs ec -enablePolicy -policy XOR-2-1-1024k
for entry in "${ec[@]}"; do
  read -r path policy <<<"$entry"
  if [[ $policy == replicate ]]; then
    hdfs ec -setPolicy -path "$path" -replicate
  else
    hdfs ec -setPolicy -path "$path" -policy "$policy"
  fi
done

echo "==> Spark jars archive"
if hdfs dfs -test -e "$SPARK_ARCHIVE"; then
  echo "$SPARK_ARCHIVE already present"
else
  python3 - "$SPARK_HOME/jars" /tmp/spark-jars.zip <<'EOF'
import pathlib
import sys
import zipfile

jars, archive = pathlib.Path(sys.argv[1]), sys.argv[2]
with zipfile.ZipFile(archive, "w", zipfile.ZIP_STORED) as zf:
    for jar in sorted(jars.glob("*.jar")):
        zf.write(jar, jar.name)
EOF
  hdfs dfs -put /tmp/spark-jars.zip "$SPARK_ARCHIVE"
  echo "uploaded $SPARK_ARCHIVE"
fi

echo "==> done"
