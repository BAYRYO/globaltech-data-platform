"""Fingerprint of a gold: for each gold table, and for each ``order_date`` partition of
``fact_sales``, the rows and the sum of one hash per row. The hash reads every column through an
explicit encoding, ``to_json`` of the row with its nulls and microseconds, so a value that moves
to a null neighbour column changes it. The sum does not depend on row order: two golds that hold
the same rows give the same JSON.

    scripts/cluster/submit.sh scripts/resilience/gold_fingerprint.py --out FILE [--env cluster]

FILE is written from the driver, relative to the repository root in client mode. The script also
prints ``gold/_state.json`` and the last gold audit rows. ``GTDP_PATHS__GOLD`` and
``GTDP_PATHS__OPS`` point it at another gold and audit (``gtdp.config``).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from gtdp.config import Env, load_config
from gtdp.gold.kpis import KPIS
from gtdp.gold.report import runs
from gtdp.gold.state import read_state
from gtdp.gold.tables import FACT_SALES, ORDER_DATE, STAR, table_path
from gtdp.io import read_table
from gtdp.spark import build_spark

ENCODING = {"ignoreNullFields": "false", "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"}
"""``to_json`` options: a null column is written ``null``, a timestamp to the microsecond."""


def _totals(frame: DataFrame) -> list[Column]:
    row = F.to_json(F.struct(*(F.col(name) for name in frame.columns)), ENCODING)
    return [
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.xxhash64(row).cast("decimal(38,0)")).cast("string").alias("xxhash64_sum"),
    ]


def checksum(frame: DataFrame) -> dict[str, object]:
    total = frame.agg(*_totals(frame)).first()
    return {"rows": total["rows"], "xxhash64_sum": total["xxhash64_sum"]}


def fingerprint(spark: SparkSession, gold: str) -> dict[str, object]:
    names = [table.name for table in STAR] + list(KPIS)
    tables = {name: read_table(spark, table_path(gold, name)) for name in names}
    sales = tables[FACT_SALES.name]
    partitions = sales.groupBy(ORDER_DATE).agg(*_totals(sales)).collect()
    return {
        "tables": {name: checksum(frame) for name, frame in tables.items()},
        "fact_sales_partitions": {
            str(r[ORDER_DATE]): {"rows": r["rows"], "xxhash64_sum": r["xxhash64_sum"]}
            for r in partitions
        },
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
        print(f"fact_sales order_date partitions: {len(result['fact_sales_partitions'])}")
        runs(spark, cfg).orderBy(F.col("started_at").desc()).limit(3).show(truncate=False)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
