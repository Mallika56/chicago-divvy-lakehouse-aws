-- How do members and casual riders differ?
-- One row per rider type: volume, share, typical ride length and habits.
SELECT
  member_casual,
  COUNT(*)                                                         AS rides,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)               AS pct_of_rides,
  ROUND(approx_percentile(duration_min, 0.5), 1)                   AS median_minutes,
  ROUND(AVG(distance_km), 2)                                       AS avg_km,
  ROUND(100.0 * SUM(CASE WHEN is_round_trip THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_round_trips,
  ROUND(100.0 * SUM(CASE WHEN is_weekend   THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_weekend
FROM divvy_lake.divvy_trips
GROUP BY member_casual
ORDER BY rides DESC;
