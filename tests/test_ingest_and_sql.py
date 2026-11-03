from pathlib import Path

import pytest

from divvy_lakehouse.ingest import check_header, ingest_from_dir, month_range, raw_key
from run_queries import to_duckdb

ROOT = Path(__file__).resolve().parents[1]


def test_month_range_crosses_year():
    assert month_range("2025-11:2026-02") == ["202511", "202512", "202601", "202602"]
    assert month_range("2026-07") == ["202607"]


def test_raw_key_is_hive_partitioned():
    assert raw_key("202607") == "divvy_trips/ingest_year=2026/ingest_month=07/202607-divvy-tripdata.csv"


def test_header_check_rejects_schema_change():
    with pytest.raises(ValueError):
        check_header('"ride_id","bike","start"', "x.csv")


def test_ingest_from_dir_lands_every_month(tmp_path):
    landed = ingest_from_dir(str(ROOT / "data/sample/raw"), str(tmp_path))
    assert len(landed) == 12
    assert all("ingest_year=" in p and "ingest_month=" in p for p in landed)


def test_athena_sql_translation():
    assert to_duckdb("SELECT approx_percentile(x, 0.5)") == "SELECT quantile_cont(x, 0.5)"
