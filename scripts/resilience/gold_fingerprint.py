"""Fingerprint of the published gold: rows and the sum of ``xxhash64`` over every column of each
gold table, and the rows of each ``order_date`` partition of ``fact_sales``. The sum does not
depend on row order, so two runs that publish the same rows give the same JSON.

    scripts/cluster/submit.sh scripts/resilience/gold_fingerprint.py --out FILE [--env cluster]

FILE is written from the driver, relative to the repository root in client mode. The script also
prints ``gold/_state.json`` and the last gold audit rows.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gtdp.config import Env, load_config
from gtdp.gold.kpis import KPIS
from gtdp.gold.report import runs
from gtdp.gold.state import read_state
from gtdp.gold.tables import FACT_SALES, ORDER_DATE, STAR, table_path
from gtdp.io import read_table
from gtdp.spark import build_spark


def checksum(frame: DataFrame) -> dict[str, object]:
    total = frame.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.xxhash64(*frame.columns).cast("decimal(38,0)")).cast("string").alias("xxhash64"),
    ).first()
    return {"rows": total["rows"], "xxhash64_sum": total["xxhash64"]}


def fingerprint(spark: SparkSession, gold: str) -> dict[str, object]:
    names = [table.name for table in STAR] + list(KPIS)
    tables = {name: read_table(spark, table_path(gold, name)) for name in names}
    per_date = tables[FACT_SALES.name].groupBy(ORDER_DATE).count().collect()
    return {
        "tables": {name: checksum(frame) for name, frame in tables.items()},
        "fact_sales_rows_per_order_date": {str(r[ORDER_DATE]): r["count"] for r in per_date},
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--env", choices=[env.value for env in Env], default=Env.CLUSTER.value)
    args = parser.parse_args(argv)
    cfg = load_config(args.env)
    spark = build_spark(cfg, f"{cfg.app.name}-gold-fingerprint")
    try:
        print(f"gold/_state.json: {read_state(spark, cfg.paths.gold)}")
        result = fingerprint(spark, cfg.paths.gold)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
        for name, values in result["tables"].items():
            print(f"{name:<28} {values['rows']:>9} rows  xxhash64_sum {values['xxhash64_sum']}")
        print(f"fact_sales order_date partitions: {len(result['fact_sales_rows_per_order_date'])}")
        runs(spark, cfg).orderBy(F.col("started_at").desc()).limit(3).show(truncate=False)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
