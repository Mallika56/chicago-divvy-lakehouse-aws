-- Where do casual riders start? These stations are where a
-- "convert casual riders to members" campaign would get the most eyeballs.
WITH by_station AS (
  SELECT
    start_station_name,
    COUNT(*)                                                         AS rides,
    CAST(SUM(CASE WHEN member_casual = 'casual' THEN 1 ELSE 0 END) AS BIGINT) AS casual_rides
  FROM divvy_lake.divvy_trips
  WHERE has_start_station
  GROUP BY start_station_name
)
SELECT
  start_station_name,
  casual_rides,
  rides,
  ROUND(100.0 * casual_rides / rides, 1) AS pct_casual
FROM by_station
ORDER BY casual_rides DESC, start_station_name
LIMIT 10;
