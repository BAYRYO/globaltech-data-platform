"""Usage: scripts/cluster/submit.sh scripts/cluster/smoke/spark_smoke.py [output_path]
Prints one GTDP_SMOKE line with the YARN application id and the executor hosts.
"""

import platform
import socket
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROWS = 1_000_000

output = sys.argv[1] if len(sys.argv) > 1 else "/user/etl/smoke/spark"
spark = SparkSession.builder.appName("gtdp-smoke").getOrCreate()
sc = spark.sparkContext

counts = spark.range(ROWS).groupBy((F.col("id") % 10).alias("bucket")).count()
counts.write.mode("overwrite").parquet(output)
total = spark.read.parquet(output).agg(F.sum("count")).first()[0]
if total != ROWS:
    sys.exit(f"read back {total} rows from {output}, expected {ROWS}")

workers = sorted(
    set(
        sc.parallelize(range(32), 8)
        .map(lambda _: f"{socket.gethostname()}/python-{platform.python_version()}")
        .collect()
    )
)
print(f"GTDP_SMOKE app_id={sc.applicationId} master={sc.master} rows={total} workers={','.join(workers)}")
spark.stop()
