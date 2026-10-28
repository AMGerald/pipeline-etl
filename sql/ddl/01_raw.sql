-- RAW: una fila por llamada a la API. Append-only: nunca se actualiza ni borra.
-- Si la misma ventana se extrae dos veces, quedan dos filas (historial completo).
CREATE TABLE IF NOT EXISTS raw.open_meteo_responses (
    response_id    bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_code  text        NOT NULL,
    window_start   date        NOT NULL,          -- rango pedido a la API (UTC)
    window_end     date        NOT NULL,
    request_url    text        NOT NULL,
    api_latitude   numeric(9,6),                  -- coordenadas que devolvió la API (grilla),
    api_longitude  numeric(9,6),                  -- distintas de las pedidas en dim_location
    api_elevation_m numeric(7,2),
    http_status    smallint    NOT NULL,
    payload        jsonb       NOT NULL,          -- respuesta tal cual llegó
    dag_run_id     text,                          -- trazabilidad hacia Airflow
    ingested_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_open_meteo_responses_loc_window
    ON raw.open_meteo_responses (location_code, window_start, window_end);
