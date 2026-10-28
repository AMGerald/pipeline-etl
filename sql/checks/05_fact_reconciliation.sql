-- severity: error
-- description: el hecho no coincide con lo que se recalcula desde staging (precipitación total u horas observadas)
SELECT l.location_code,
       f.precipitation_total_mm, s.precipitation_total_mm AS staging_precipitation_mm,
       f.hours_observed,         s.hours_observed         AS staging_hours
FROM core.fact_weather_daily f
JOIN core.dim_location l ON l.location_key = f.location_key
JOIN core.dim_date d     ON d.date_key = f.date_key AND d.full_date = %(local_date)s
CROSS JOIN LATERAL (
    SELECT sum(st.precipitation_mm)    AS precipitation_total_mm,
           count(st.temperature_c)     AS hours_observed
    FROM staging.weather_hourly st
    WHERE st.location_code = l.location_code
      AND st.observed_at >= d.full_date::timestamp AT TIME ZONE l.timezone
      AND st.observed_at <  (d.full_date + 1)::timestamp AT TIME ZONE l.timezone
) s
WHERE f.precipitation_total_mm IS DISTINCT FROM s.precipitation_total_mm
   OR f.hours_observed         IS DISTINCT FROM s.hours_observed
ORDER BY l.location_code
