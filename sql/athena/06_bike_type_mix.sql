-- Classic vs electric: who rides what, and for how long?
SELECT
  rideable_type,
  member_casual,
  COUNT(*)                                       AS rides,
  ROUND(approx_percentile(duration_min, 0.5), 1) AS median_minutes,
  ROUND(AVG(distance_km), 2)                     AS avg_km,
  ROUND(100.0 * SUM(CASE WHEN has_start_station THEN 0 ELSE 1 END) / COUNT(*), 1) AS pct_dockless_start
FROM divvy_lake.divvy_trips
GROUP BY 1, 2
ORDER BY 1, 2;
