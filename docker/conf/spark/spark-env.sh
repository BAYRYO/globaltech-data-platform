# shellcheck shell=bash
# Spark is the "without Hadoop" build: it runs on the cluster's own Hadoop jars.
SPARK_DIST_CLASSPATH="$(hadoop classpath)"
export SPARK_DIST_CLASSPATH
