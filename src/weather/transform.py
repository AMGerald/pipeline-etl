"""Transformación y carga: raw -> staging -> core.

Todo es idempotente: ejecutar dos veces lo mismo deja las tablas igual.
Sin dependencia de Airflow: el hook se recibe por parámetro.
"""

import logging
from datetime import date

log = logging.getLogger(__name__)

# Pivota los arrays paralelos del JSON a filas: la posición (ordinality) en cada
# array une `time` con el resto de variables. JSON null -> SQL NULL.
# El WHERE del DO UPDATE evita que reprocesar una respuesta antigua pise datos
# que ya vienen de una respuesta más nueva.
_LOAD_STAGING_SQL = """
INSERT INTO staging.weather_hourly
    (location_code, observed_at, temperature_c, relative_humidity_pct,
     precipitation_mm, wind_speed_kmh, weather_code, source_response_id)
SELECT
    r.location_code,
    (t.v::timestamp AT TIME ZONE 'UTC'),
    temp.v::numeric,
    hum.v::numeric,
    pre.v::numeric,
    wnd.v::numeric,
    wc.v::smallint,
    r.response_id
FROM raw.open_meteo_responses r
CROSS JOIN LATERAL jsonb_array_elements_text(r.payload #> '{hourly,time}')
        WITH ORDINALITY AS t(v, i)
LEFT JOIN LATERAL jsonb_array_elements_text(r.payload #> '{hourly,temperature_2m}')
        WITH ORDINALITY AS temp(v, i) ON temp.i = t.i
LEFT JOIN LATERAL jsonb_array_elements_text(r.payload #> '{hourly,relative_humidity_2m}')
        WITH ORDINALITY AS hum(v, i) ON hum.i = t.i
LEFT JOIN LATERAL jsonb_array_elements_text(r.payload #> '{hourly,precipitation}')
        WITH ORDINALITY AS pre(v, i) ON pre.i = t.i
LEFT JOIN LATERAL jsonb_array_elements_text(r.payload #> '{hourly,wind_speed_10m}')
        WITH ORDINALITY AS wnd(v, i) ON wnd.i = t.i
LEFT JOIN LATERAL jsonb_array_elements_text(r.payload #> '{hourly,weather_code}')
        WITH ORDINALITY AS wc(v, i) ON wc.i = t.i
WHERE r.response_id = %(response_id)s
ON CONFLICT (location_code, observed_at) DO UPDATE SET
    temperature_c         = EXCLUDED.temperature_c,
    relative_humidity_pct = EXCLUDED.relative_humidity_pct,
    precipitation_mm      = EXCLUDED.precipitation_mm,
    wind_speed_kmh        = EXCLUDED.wind_speed_kmh,
    weather_code          = EXCLUDED.weather_code,
    source_response_id    = EXCLUDED.source_response_id,
    loaded_at             = now()
WHERE staging.weather_hourly.source_response_id <= EXCLUDED.source_response_id
"""

# El día local de cada ciudad = [local_date 00:00, +1 día) en su timezone,
# convertido a UTC. Se filtra por rango (no por ::date) para poder usar la PK.
_BUILD_FACT_SQL = """
INSERT INTO core.fact_weather_daily
    (location_key, date_key, temp_min_c, temp_max_c, temp_avg_c,
     humidity_avg_pct, precipitation_total_mm, wind_speed_max_kmh,
     rainy_hours, hours_observed)
SELECT
    l.location_key,
    d.date_key,
    min(s.temperature_c),
    max(s.temperature_c),
    round(avg(s.temperature_c), 2),
    round(avg(s.relative_humidity_pct), 2),
    sum(s.precipitation_mm),
    max(s.wind_speed_kmh),
    count(*) FILTER (WHERE s.precipitation_mm > 0),
    count(*) FILTER (WHERE s.temperature_c IS NOT NULL)
FROM core.dim_location l
JOIN core.dim_date d ON d.full_date = %(local_date)s
JOIN staging.weather_hourly s
  ON s.location_code = l.location_code
 AND s.observed_at >= (d.full_date::timestamp AT TIME ZONE l.timezone)
 AND s.observed_at <  ((d.full_date + 1)::timestamp AT TIME ZONE l.timezone)
WHERE l.is_active
GROUP BY l.location_key, d.date_key
ON CONFLICT (location_key, date_key) DO UPDATE SET
    temp_min_c             = EXCLUDED.temp_min_c,
    temp_max_c             = EXCLUDED.temp_max_c,
    temp_avg_c             = EXCLUDED.temp_avg_c,
    humidity_avg_pct       = EXCLUDED.humidity_avg_pct,
    precipitation_total_mm = EXCLUDED.precipitation_total_mm,
    wind_speed_max_kmh     = EXCLUDED.wind_speed_max_kmh,
    rainy_hours            = EXCLUDED.rainy_hours,
    hours_observed         = EXCLUDED.hours_observed,
    loaded_at              = now()
"""


def _execute(hook, sql: str, params: dict) -> int:
    """Ejecuta una sentencia en su propia transacción y devuelve las filas afectadas."""
    conn = hook.get_conn()
    try:
        with conn, conn.cursor() as cur:  # `with conn` = commit/rollback
            cur.execute(sql, params)
            return cur.rowcount
    finally:
        conn.close()


def load_staging(hook, response_id: int) -> int:
    """raw -> staging para una respuesta. Devuelve filas insertadas/actualizadas."""
    rows = _execute(hook, _LOAD_STAGING_SQL, {"response_id": response_id})
    log.info("staging.weather_hourly: %s filas desde response_id=%s", rows, response_id)
    return rows


def build_fact_daily(hook, local_date: date) -> int:
    """staging -> core para un día local, todas las ciudades activas."""
    rows = _execute(hook, _BUILD_FACT_SQL, {"local_date": local_date})
    log.info("core.fact_weather_daily: %s filas para %s", rows, local_date)
    return rows
