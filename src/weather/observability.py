"""Alertas ante fallos del pipeline.

`on_task_failure` es el `on_failure_callback` de las tareas del DAG: deja el fallo
en audit.pipeline_runs, en el log y, si hay `ALERT_WEBHOOK_URL`, lo envía por
webhook (JSON `{"text": ...}`, compatible con Slack y Discord). Sin URL no se envía
nada fuera. Un callback nunca debe lanzar excepciones: una alerta rota no puede
enmascarar el error original.
"""

import logging
import os

import requests

from weather import audit

log = logging.getLogger(__name__)

WEBHOOK_ENV = "ALERT_WEBHOOK_URL"
WEBHOOK_TIMEOUT_S = 10
ALERT_ERROR_CHARS = 500


def format_failure(dag_id: str, task: str, run_id: str, target_day, error: str) -> str:
    return (
        f"🔴 Falló {dag_id}.{task}\n"
        f"run: {run_id}\n"
        f"día objetivo: {target_day or 'desconocido'}\n"
        f"error: {error[:ALERT_ERROR_CHARS]}"
    )


def send_alert(text: str, webhook_url: str | None = None) -> bool:
    """POST al webhook. Devuelve True si se entregó; nunca lanza."""
    url = webhook_url or os.environ.get(WEBHOOK_ENV)
    if not url:
        log.info("%s no configurada: la alerta queda solo en el log", WEBHOOK_ENV)
        return False
    try:
        resp = requests.post(url, json={"text": text}, timeout=WEBHOOK_TIMEOUT_S)
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        log.warning("No se pudo enviar la alerta: %s", exc)
        return False


def on_task_failure(context: dict, hook=None) -> None:
    """Callback de fallo definitivo de una tarea (ya agotó sus reintentos)."""
    try:
        ti = context.get("ti") or context.get("task_instance")
        dag_id = getattr(ti, "dag_id", "?")
        run_id = context.get("run_id") or getattr(ti, "run_id", "?")
        map_index = getattr(ti, "map_index", -1)
        task = getattr(ti, "task_id", "?") + (f"[{map_index}]" if map_index >= 0 else "")
        exc = context.get("exception")
        error = f"{type(exc).__name__}: {exc}" if exc else "error desconocido"
        target_day = _target_day(ti)

        log.error(
            "PIPELINE_FAILURE dag=%s task=%s run=%s target_day=%s error=%s",
            dag_id,
            task,
            run_id,
            target_day,
            error,
        )
        _record_failure(hook, run_id, dag_id, task, error, target_day)
        send_alert(format_failure(dag_id, task, run_id, target_day, error))
    except Exception:  # un callback no puede romper el run
        log.exception("on_task_failure falló; el error original de la tarea sigue vigente")


def _target_day(ti):
    """Día objetivo si la tarea `target_day` alcanzó a publicarlo; None si no."""
    try:
        from datetime import date

        value = ti.xcom_pull(task_ids="target_day")
        return date.fromisoformat(value) if value else None
    except Exception:
        return None


def _record_failure(hook, run_id, dag_id, task, error, target_day) -> None:
    try:
        if hook is None:
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            hook = PostgresHook(postgres_conn_id="warehouse_db")
        audit.finish_run_failed(hook, run_id, dag_id, task, error, target_day)
    except Exception:  # p. ej. la tabla aún no existe si falló apply_schema
        log.exception("No se pudo registrar el fallo en audit.pipeline_runs")
