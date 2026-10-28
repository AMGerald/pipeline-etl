-- severity: warn
-- description: ciudades activas cuyo último día en el hecho es más viejo que LAG_DAYS + 2 (el pipeline se detuvo)
SELECT l.location_code, max(d.full_date) AS latest_day
FROM core.dim_location l
LEFT JOIN core.fact_weather_daily f ON f.location_key = l.location_key
LEFT JOIN core.dim_date d           ON d.date_key = f.date_key
WHERE l.is_active
GROUP BY l.location_code
HAVING max(d.full_date) IS NULL
    OR max(d.full_date) < (now() AT TIME ZONE 'UTC')::date - (%(lag_days)s + 2)
ORDER BY l.location_code
