"""Run the whole lakehouse on your laptop, no AWS account needed.

    python scripts/run_local.py

Mirrors the AWS flow with local folders standing in for S3:
    data/sample/raw  --ingest-->  lake/raw  --Spark (same code as Glue)-->  lake/curated
                                                         \\--> lake/quarantine
then runs every Athena query in sql/athena/ against the Parquet with DuckDB
and saves the results to docs/results/.
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from pyspark.sql import SparkSession  # noqa: E402

from divvy_lakehouse.ingest import ingest_from_dir  # noqa: E402
from divvy_lakehouse.transform import read_raw, run_metrics, transform, write_parquet  # noqa: E402

LAKE = ROOT / "lake"


def main() -> None:
    """Run ingest, transform, and query stages locally against the committed sample data."""
    if LAKE.exists():
        shutil.rmtree(LAKE)

    print("1) Ingest: landing sample files in lake/raw")
    landed = ingest_from_dir(str(ROOT / "data/sample/raw"), str(LAKE / "raw"))
    print(f"   {len(landed)} monthly files landed")

    print("2) Transform: raw -> curated + quarantine (PySpark, same code as the Glue job)")
    spark = (SparkSession.builder.master("local[*]").appName("divvy-local")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    raw = read_raw(spark, str(LAKE / "raw/divvy_trips/*/*/*.csv"))
    curated, quarantine = transform(raw)
    curated.cache()
    quarantine.cache()
    write_parquet(curated, str(LAKE / "curated/divvy_trips"), partitioned=True)
    write_parquet(quarantine, str(LAKE / "quarantine/divvy_trips"), partitioned=False)

    metrics = run_metrics(raw, curated, quarantine)
    (ROOT / "docs/results").mkdir(parents=True, exist_ok=True)
    (ROOT / "docs/results/run_metrics.json").write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2))
    spark.stop()

    print("3) Query: running sql/athena/*.sql with DuckDB")
    from run_queries import run_all  # noqa: E402  (scripts/ is on sys.path when run directly)

    run_all()


if __name__ == "__main__":
    main()
