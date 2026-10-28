-- Se ejecuta automáticamente la PRIMERA vez que arranca warehouse-db
-- (solo si el volumen de datos está vacío).

CREATE SCHEMA IF NOT EXISTS raw;      -- respuestas de la API tal cual llegaron
CREATE SCHEMA IF NOT EXISTS staging;  -- datos aplanados, tipados y deduplicados
CREATE SCHEMA IF NOT EXISTS core;     -- modelo dimensional (dims + facts)
CREATE SCHEMA IF NOT EXISTS audit;    -- metadatos de ejecución del pipeline
