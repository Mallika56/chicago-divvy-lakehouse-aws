-- When do people ride? Rides by hour of day, weekday vs weekend, per rider type.
-- Commute peaks (8am / 5pm on weekdays) are the signature of members.
SELECT
  member_casual,
  CASE WHEN is_weekend THEN 'weekend' ELSE 'weekday' END AS day_type,
  start_hour,
  COUNT(*) AS rides
FROM divvy_lake.divvy_trips
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
