"""Unit tests for the raw -> curated transformation.

Run:  pytest -q
The edge-case fixture holds 1,268 real Divvy rows picked because they are
awkward: cross-file duplicates, daylight-saving negatives, test stations,
rides over 24 hours, rides outside Chicago, etc.
"""

from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from divvy_lakehouse.transform import (
    RAW_SCHEMA, add_derived, apply_quality_rules, deduplicate, read_raw,
    standardize, transform,
)

ROOT = Path(__file__).resolve().parents[1]
EDGE = ROOT / "tests/fixtures/edge_case_rides.csv"
SAMPLE = ROOT / "data/sample/raw"


def _row(**overrides):
    base = dict(
        ride_id="AAAA000000000001", rideable_type="classic_bike",
        started_at="2026-07-01 08:00:00.000", ended_at="2026-07-01 08:15:00.000",
        start_station_name="Clark St & Elm St", start_station_id="TA1",
        end_station_name="Wells St & Concord Ln", end_station_id="TA2",
        start_lat="41.9029", start_lng="-87.6313", end_lat="41.9121", end_lng="-87.6347",
        member_casual="member", source_file="202607-divvy-tripdata.csv",
    )
    base.update(overrides)
    return base


def _tag(spark, rows):
    schema = StructType(RAW_SCHEMA.fields + [StructField("source_file", StringType())])
    df = spark.createDataFrame([tuple(r[f.name] for f in schema.fields) for r in rows], schema)
    return apply_quality_rules(deduplicate(add_derived(standardize(df))))


@pytest.fixture(scope="module")
def edge(spark):
    schema = StructType(RAW_SCHEMA.fields + [StructField("source_file", StringType())])
    df = spark.read.option("header", True).schema(schema).csv(str(EDGE))
    return apply_quality_rules(deduplicate(add_derived(standardize(df)))).cache()


def test_clean_ride_passes_and_derives_columns(spark):
    r = _tag(spark, [_row()]).first()
    assert r.dq_reason is None
    assert r.duration_min == 15.0
    assert r.start_hour == 8
    assert r.start_dow == 3            # 1 July 2026 is a Wednesday (ISO 3)
    assert r.is_weekend is False
    assert r.year == 2026 and r.month == 7
    assert 1.0 < r.distance_km < 1.2   # ~1.05 km between the two docks


def test_iso_weekday_sunday_is_7(spark):
    r = _tag(spark, [_row(started_at="2026-07-05 10:00:00", ended_at="2026-07-05 10:30:00")]).first()
    assert r.start_dow == 7 and r.is_weekend is True


@pytest.mark.parametrize("override, reason", [
    (dict(ride_id=""), "missing_ride_id"),
    (dict(started_at="not a date"), "bad_timestamp"),
    (dict(rideable_type="unicycle"), "unknown_rideable_type"),
    (dict(member_casual="guest"), "unknown_rider_type"),
    (dict(ended_at="2026-07-01 07:59:00"), "non_positive_duration"),
    (dict(ended_at="2026-07-01 08:00:30"), "too_short_under_1_min"),
    (dict(ended_at="2026-07-02 09:00:00"), "too_long_over_24h"),
    (dict(end_lat="", end_lng=""), "missing_end_location"),
    (dict(end_lat="40.71", end_lng="-74.00"), "outside_service_area"),
    (dict(start_station_name="Hastings Test Station"), "non_customer_station"),
])
def test_each_rule_catches_its_case(spark, override, reason):
    assert _tag(spark, [_row(**override)]).first().dq_reason == reason


def test_first_rule_wins(spark):
    # Broken in two ways: unknown rider type AND negative duration.
    r = _tag(spark, [_row(member_casual="guest", ended_at="2026-07-01 07:00:00")]).first()
    assert r.dq_reason == "unknown_rider_type"


def test_duplicate_keeps_copy_from_latest_file(spark):
    rows = [_row(source_file="202606-divvy-tripdata.csv"), _row(source_file="202607-divvy-tripdata.csv")]
    tagged = _tag(spark, rows)
    kept = tagged.filter(F.col("dq_reason").isNull()).collect()
    assert len(kept) == 1 and kept[0].source_file == "202607-divvy-tripdata.csv"
    assert tagged.filter(F.col("dq_reason") == "duplicate_ride_id").count() == 1


def test_trailing_star_removed_from_station_names(spark):
    r = _tag(spark, [_row(start_station_name="Lincoln Ave & Roscoe St*")]).first()
    assert r.start_station_name == "Lincoln Ave & Roscoe St"


def test_edge_cases_from_real_data_are_quarantined(edge):
    reasons = {r.dq_reason for r in edge.select("dq_reason").distinct().collect()}
    for expected in ["duplicate_ride_id", "non_positive_duration", "too_long_over_24h",
                     "non_customer_station", "outside_service_area"]:
        assert expected in reasons, expected


def test_no_duplicates_survive(edge):
    valid = edge.filter(F.col("dq_reason").isNull())
    assert valid.count() == valid.select("ride_id").distinct().count()


def test_curated_rows_obey_every_rule(spark):
    raw = read_raw(spark, str(SAMPLE))
    valid, invalid = transform(raw)
    assert valid.count() + invalid.count() == raw.count()   # nothing silently dropped
    bad = valid.filter(
        (F.col("duration_min") < 1) | (F.col("duration_min") > 1440)
        | F.col("ride_id").isNull() | F.col("started_at").isNull()
        | ~F.col("member_casual").isin("member", "casual")
    )
    assert bad.count() == 0
