-- severity: error
-- description: valores fuera de rangos físicos plausibles en staging (los NULL no se evalúan aquí, ver 02)
SELECT s.location_code, s.observed_at,
       s.temperature_c, s.relative_humidity_pct, s.precipitation_mm, s.wind_speed_kmh, s.weather_code
FROM core.dim_location l
JOIN staging.weather_hourly s
  ON s.location_code = l.location_code
 AND s.observed_at >= (%(local_date)s::date)::timestamp AT TIME ZONE l.timezone
 AND s.observed_at <  (%(local_date)s::date + 1)::timestamp AT TIME ZONE l.timezone
WHERE l.is_active
  AND (   s.temperature_c         NOT BETWEEN -60 AND 60
       OR s.relative_humidity_pct NOT BETWEEN 0 AND 100
       OR s.precipitation_mm      NOT BETWEEN 0 AND 150
       OR s.wind_speed_kmh        NOT BETWEEN 0 AND 250
       OR s.weather_code          NOT BETWEEN 0 AND 99)
ORDER BY s.location_code, s.observed_at
