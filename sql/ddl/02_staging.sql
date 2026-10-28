-- STAGING: grain = una fila por ciudad por hora UTC.
-- Tipos correctos, nombres claros con unidad, sin duplicados (PK).
CREATE TABLE IF NOT EXISTS staging.weather_hourly (
    location_code          text         NOT NULL,
    observed_at            timestamptz  NOT NULL,  -- inicio de la hora, en UTC
    temperature_c          numeric(5,2),
    relative_humidity_pct  numeric(5,2),
    precipitation_mm       numeric(6,2),
    wind_speed_kmh         numeric(6,2),
    weather_code           smallint,               -- código WMO
    source_response_id     bigint       NOT NULL,  -- linaje: de qué respuesta raw salió
    loaded_at              timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (location_code, observed_at)
);
