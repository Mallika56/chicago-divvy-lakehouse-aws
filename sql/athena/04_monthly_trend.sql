-- Seasonality: rides per month per rider type, plus the e-bike share.
-- Filtering on year/month (partition columns) lets Athena skip whole folders.
SELECT
  year,
  month,
  member_casual,
  COUNT(*) AS rides,
  ROUND(100.0 * SUM(CASE WHEN rideable_type = 'electric_bike' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_ebike
FROM divvy_lake.divvy_trips
WHERE (year = 2025 AND month >= 10) OR (year = 2026 AND month <= 9)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
