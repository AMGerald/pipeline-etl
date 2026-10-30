-- AUDIT: trazabilidad de cada ejecución del pipeline.

-- Grain = un run del DAG (run_id de Airflow). Se actualiza por upsert: limpiar y
-- reintentar tareas reutiliza la misma fila en vez de duplicarla.
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id          text         PRIMARY KEY,
    dag_id          text         NOT NULL,
    target_day      date,                          -- día procesado (data_interval_start - LAG_DAYS)
    state           text         NOT NULL CHECK (state IN ('running', 'success', 'failed')),
    started_at      timestamptz  NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    raw_responses   integer,                       -- respuestas de la API guardadas en raw
    staging_rows    integer,                       -- filas insertadas/actualizadas en staging
    fact_rows       integer,                       -- filas del hecho escritas
    failed_task     text,
    error           text
);

-- Grain = un check en un run. Se guarda aunque el run falle: es cuando más importa.
CREATE TABLE IF NOT EXISTS audit.check_results (
    run_id        text         NOT NULL REFERENCES audit.pipeline_runs (run_id) ON DELETE CASCADE,
    check_name    text         NOT NULL,
    severity      text         NOT NULL CHECK (severity IN ('error', 'warn')),
    description   text         NOT NULL,
    violations    integer      NOT NULL,
    sample        jsonb,                           -- primeras filas que violan la regla
    checked_at    timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, check_name)
);

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_target_day ON audit.pipeline_runs (target_day);
