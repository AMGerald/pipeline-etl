-- severity: error
-- description: ciudades activas sin fila en el hecho para el día local, o con menos de 24 horas observadas
SELECT l.location_code, f.hours_observed
FROM core.dim_location l
JOIN core.dim_date d ON d.full_date = %(local_date)s
LEFT JOIN core.fact_weather_daily f
       ON f.location_key = l.location_key AND f.date_key = d.date_key
WHERE l.is_active
  AND f.hours_observed IS DISTINCT FROM 24
ORDER BY l.location_code
