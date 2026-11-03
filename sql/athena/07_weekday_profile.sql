-- Rides by day of week (1 = Monday ... 7 = Sunday, ISO numbering set in the ETL).
SELECT
  start_dow,
  member_casual,
  COUNT(*) AS rides,
  ROUND(approx_percentile(duration_min, 0.5), 1) AS median_minutes
FROM divvy_lake.divvy_trips
GROUP BY 1, 2
ORDER BY 1, 2;
