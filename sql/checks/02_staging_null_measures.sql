-- severity: warn
-- description: horas del día local con alguna medida nula en staging (la completitud la valida 04)
SELECT s.location_code, s.observed_at
FROM core.dim_location l
JOIN staging.weather_hourly s
  ON s.location_code = l.location_code
 AND s.observed_at >= (%(local_date)s::date)::timestamp AT TIME ZONE l.timezone
 AND s.observed_at <  (%(local_date)s::date + 1)::timestamp AT TIME ZONE l.timezone
WHERE l.is_active
  AND (   s.temperature_c IS NULL
       OR s.relative_humidity_pct IS NULL
       OR s.precipitation_mm IS NULL
       OR s.wind_speed_kmh IS NULL
       OR s.weather_code IS NULL)
ORDER BY s.location_code, s.observed_at
