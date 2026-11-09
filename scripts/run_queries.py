"""Run the Athena SQL in sql/athena/ locally with DuckDB.

Athena is built on Trino; DuckDB speaks almost the same SQL. The only
translation needed for these queries is one function name:
    Athena  approx_percentile(x, 0.5)   ->   DuckDB  quantile_cont(x, 0.5)
(quantile_cont is exact, so local medians can differ from Athena's
approximate ones in the last decimal on large data.)

Files whose first line is `-- athena-only` (DDL) are skipped.
Results are written to docs/results/<query>.csv.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql/athena"
CURATED = ROOT / "lake/curated/divvy_trips"
RESULTS = ROOT / "docs/results"


def to_duckdb(sql: str) -> str:
    """Rewrite Athena-only SQL functions to their DuckDB equivalents."""
    return re.sub(r"\bapprox_percentile\s*\(", "quantile_cont(", sql, flags=re.IGNORECASE)


def connect() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with a divvy_lake.divvy_trips view over the curated Parquet."""
    con = duckdb.connect()
    con.execute("CREATE SCHEMA divvy_lake")
    con.execute(
        "CREATE VIEW divvy_lake.divvy_trips AS SELECT * FROM read_parquet("
        f"'{CURATED}/*/*/*.parquet', hive_partitioning = true)"
    )
    return con


def run_all() -> dict[str, "object"]:
    """Run every non-Athena-only query in sql/athena/, saving and returning each result."""
    RESULTS.mkdir(parents=True, exist_ok=True)
    con = connect()
    out = {}
    for f in sorted(SQL_DIR.glob("*.sql")):
        sql = f.read_text()
        if sql.lstrip().startswith("-- athena-only"):
            continue
        df = con.execute(to_duckdb(sql)).df()
        df.to_csv(RESULTS / f"{f.stem}.csv", index=False)
        out[f.stem] = df
        print(f"\n== {f.name} ({len(df)} rows)")
        print(df.head(12).to_string(index=False))
    return out


if __name__ == "__main__":
    run_all()
