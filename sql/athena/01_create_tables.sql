-- athena-only
-- Registers the curated Parquet as an Athena table.
-- Partition projection lets Athena work out the year/month folders itself,
-- so we never have to run MSCK REPAIR TABLE or a crawler after each load.
-- (The CloudFormation template creates this same table; this file is the
--  hand-written version for learning and for running in the Athena console.)

CREATE EXTERNAL TABLE IF NOT EXISTS divvy_lake.divvy_trips (
  ride_id            string,
  rideable_type      string,
  member_casual      string,
  started_at         timestamp,
  ended_at           timestamp,
  ride_date          date,
  start_hour         int,
  start_dow          int,
  is_weekend         boolean,
  duration_min       double,
  distance_km        double,
  is_round_trip      boolean,
  start_station_id   string,
  start_station_name string,
  end_station_id     string,
  end_station_name   string,
  has_start_station  boolean,
  has_end_station    boolean,
  start_lat          double,
  start_lng          double,
  end_lat            double,
  end_lng            double,
  source_file        string
)
PARTITIONED BY (year int, month int)
STORED AS PARQUET
LOCATION 's3://<curated-bucket>/divvy_trips/'
TBLPROPERTIES (
  'parquet.compression'          = 'SNAPPY',
  'projection.enabled'           = 'true',
  'projection.year.type'         = 'integer',
  'projection.year.range'        = '2020,2030',
  'projection.month.type'        = 'integer',
  'projection.month.range'       = '1,12',
  'storage.location.template'    = 's3://<curated-bucket>/divvy_trips/year=${year}/month=${month}/'
);
