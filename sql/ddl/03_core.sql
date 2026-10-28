-- CORE: modelo dimensional (esquema estrella).

-- Dimensión de ubicación. location_key es llave subrogada; location_code es la natural.
CREATE TABLE IF NOT EXISTS core.dim_location (
    location_key   smallint     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_code  text         NOT NULL UNIQUE,
    city_name      text         NOT NULL,
    region         text         NOT NULL,
    country_code   char(2)      NOT NULL,
    latitude       numeric(8,5) NOT NULL,
    longitude      numeric(8,5) NOT NULL,
    timezone       text         NOT NULL,          -- IANA, p. ej. America/Lima
    is_active      boolean      NOT NULL DEFAULT true,
    updated_at     timestamptz  NOT NULL DEFAULT now()
);

-- Dimensión de fecha. date_key = YYYYMMDD (convención Kimball: legible y ordenable).
CREATE TABLE IF NOT EXISTS core.dim_date (
    date_key      integer  PRIMARY KEY,
    full_date     date     NOT NULL UNIQUE,
    year          smallint NOT NULL,
    quarter       smallint NOT NULL,
    month         smallint NOT NULL,
    month_name    text     NOT NULL,
    day_of_month  smallint NOT NULL,
    day_of_week   smallint NOT NULL,              -- ISO: 1 = lunes … 7 = domingo
    day_name      text     NOT NULL,
    is_weekend    boolean  NOT NULL
);

-- Hecho diario. Grain = una fila por ciudad por día LOCAL de la ciudad.
CREATE TABLE IF NOT EXISTS core.fact_weather_daily (
    location_key            smallint     NOT NULL REFERENCES core.dim_location (location_key),
    date_key                integer      NOT NULL REFERENCES core.dim_date (date_key),
    temp_min_c              numeric(5,2),
    temp_max_c              numeric(5,2),
    temp_avg_c              numeric(5,2),
    humidity_avg_pct        numeric(5,2),
    precipitation_total_mm  numeric(7,2),
    wind_speed_max_kmh      numeric(6,2),
    rainy_hours             smallint,              -- horas con precipitación > 0
    hours_observed          smallint     NOT NULL, -- completitud: debería ser 24
    loaded_at               timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (location_key, date_key)
);
