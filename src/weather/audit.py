"""Auditoría: estado de cada run (audit.pipeline_runs) y resultado de sus checks.

Todas las escrituras son upserts por `run_id`: reintentar o limpiar tareas en
Airflow no duplica filas.
"""

import json
from datetime import date

from weather.db import execute

MAX_ERROR_CHARS = 2000

_START_SQL = """
INSERT INTO audit.pipeline_runs (run_id, dag_id, target_day, state)
VALUES (%(run_id)s, %(dag_id)s, %(target_day)s, 'running')
ON CONFLICT (run_id) DO UPDATE SET
    target_day = EXCLUDED.target_day, state = 'running', started_at = now(),
    finished_at = NULL, raw_responses = NULL, staging_rows = NULL, fact_rows = NULL,
    failed_task = NULL, error = NULL
"""

_SUCCESS_SQL = """
INSERT INTO audit.pipeline_runs
    (run_id, dag_id, target_day, state, finished_at, raw_responses, staging_rows, fact_rows)
VALUES (%(run_id)s, %(dag_id)s, %(target_day)s, 'success', now(),
        %(raw_responses)s, %(staging_rows)s, %(fact_rows)s)
ON CONFLICT (run_id) DO UPDATE SET
    state = 'success', finished_at = now(), raw_responses = EXCLUDED.raw_responses,
    staging_rows = EXCLUDED.staging_rows, fact_rows = EXCLUDED.fact_rows,
    failed_task = NULL, error = NULL
"""

# target_day puede ser desconocido al fallar (p. ej. si falló la tarea que lo calcula):
# no se pisa con NULL un valor ya registrado.
_FAILED_SQL = """
INSERT INTO audit.pipeline_runs
    (run_id, dag_id, target_day, state, finished_at, failed_task, error)
VALUES (%(run_id)s, %(dag_id)s, %(target_day)s, 'failed', now(), %(failed_task)s, %(error)s)
ON CONFLICT (run_id) DO UPDATE SET
    state = 'failed', finished_at = now(), failed_task = EXCLUDED.failed_task,
    error = EXCLUDED.error,
    target_day = COALESCE(EXCLUDED.target_day, audit.pipeline_runs.target_day)
"""

_CHECK_SQL = """
INSERT INTO audit.check_results
    (run_id, check_name, severity, description, violations, sample)
VALUES (%s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (run_id, check_name) DO UPDATE SET
    severity = EXCLUDED.severity, description = EXCLUDED.description,
    violations = EXCLUDED.violations, sample = EXCLUDED.sample, checked_at = now()
"""


def start_run(hook, run_id: str, dag_id: str, target_day: date) -> None:
    execute(hook, _START_SQL, {"run_id": run_id, "dag_id": dag_id, "target_day": target_day})


def finish_run_success(
    hook,
    run_id: str,
    dag_id: str,
    target_day: date,
    raw_responses: int,
    staging_rows: int,
    fact_rows: int,
) -> None:
    execute(
        hook,
        _SUCCESS_SQL,
        {
            "run_id": run_id,
            "dag_id": dag_id,
            "target_day": target_day,
            "raw_responses": raw_responses,
            "staging_rows": staging_rows,
            "fact_rows": fact_rows,
        },
    )


def finish_run_failed(
    hook,
    run_id: str,
    dag_id: str,
    failed_task: str,
    error: str,
    target_day: date | None = None,
) -> None:
    execute(
        hook,
        _FAILED_SQL,
        {
            "run_id": run_id,
            "dag_id": dag_id,
            "target_day": target_day,
            "failed_task": failed_task,
            "error": error[:MAX_ERROR_CHARS],
        },
    )


def record_check_results(hook, run_id: str, results) -> None:
    """Guarda todos los resultados en una transacción (`results`: lista de CheckResult)."""
    conn = hook.get_conn()
    try:
        with conn, conn.cursor() as cur:
            for r in results:
                cur.execute(
                    _CHECK_SQL,
                    (
                        run_id,
                        r.name,
                        r.severity,
                        r.description,
                        r.violations,
                        json.dumps(r.sample, default=str),
                    ),
                )
    finally:
        conn.close()
