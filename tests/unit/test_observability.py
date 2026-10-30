import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

import pytest

from tests.unit.fakes import BrokenHook, RecordingHook
from weather.observability import (
    WEBHOOK_ENV,
    format_failure,
    on_task_failure,
    send_alert,
)


@pytest.fixture
def webhook():
    """Servidor HTTP real en localhost: (url, mensajes recibidos, status a responder)."""
    received, behavior = [], {"status": 200}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            received.append(json.loads(body))
            self.send_response(behavior["status"])
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}/", received, behavior
    server.shutdown()
    server.server_close()


def make_context(exception=None, map_index=-1, xcom="2026-10-22"):
    ti = SimpleNamespace(
        dag_id="weather_daily",
        task_id="extract_city",
        run_id="manual__x",
        map_index=map_index,
        xcom_pull=lambda task_ids=None: xcom,
    )
    return {"ti": ti, "run_id": "manual__x", "exception": exception}


def test_format_failure_incluye_lo_necesario_y_trunca_el_error():
    text = format_failure("d", "t", "run-1", "2026-10-22", "x" * 5000)

    assert "d.t" in text and "run-1" in text and "2026-10-22" in text
    assert len(text) < 700


def test_send_alert_entrega_el_json_al_webhook(webhook):
    url, received, _ = webhook

    assert send_alert("hola", url) is True
    assert received == [{"text": "hola"}]


def test_send_alert_lee_la_url_del_entorno(webhook, monkeypatch):
    url, received, _ = webhook
    monkeypatch.setenv(WEBHOOK_ENV, url)

    assert send_alert("desde env") is True
    assert received == [{"text": "desde env"}]


def test_send_alert_sin_url_no_envia_nada(monkeypatch, caplog):
    monkeypatch.delenv(WEBHOOK_ENV, raising=False)

    with caplog.at_level(logging.INFO):
        assert send_alert("nadie escucha") is False

    assert "no configurada" in caplog.text


@pytest.mark.parametrize("status", [400, 500])
def test_send_alert_con_respuesta_de_error_devuelve_false_sin_lanzar(webhook, status):
    url, _, behavior = webhook
    behavior["status"] = status

    assert send_alert("x", url) is False


def test_send_alert_con_servidor_caido_devuelve_false_sin_lanzar():
    assert send_alert("x", "http://127.0.0.1:9/") is False  # puerto 9: nadie escucha


def test_on_task_failure_audita_y_alerta(webhook, monkeypatch):
    url, received, _ = webhook
    monkeypatch.setenv(WEBHOOK_ENV, url)
    hook = RecordingHook()

    on_task_failure(make_context(ValueError("API caída"), map_index=2), hook=hook)

    ((sql, params),) = hook.executed
    assert "audit.pipeline_runs" in sql and "'failed'" in sql
    assert params["run_id"] == "manual__x"
    assert params["failed_task"] == "extract_city[2]"
    assert params["error"] == "ValueError: API caída"
    assert str(params["target_day"]) == "2026-10-22"
    assert "extract_city[2]" in received[0]["text"] and "API caída" in received[0]["text"]


def test_on_task_failure_nunca_lanza_aunque_todo_falle(monkeypatch, caplog):
    monkeypatch.setenv(WEBHOOK_ENV, "http://127.0.0.1:9/")

    with caplog.at_level(logging.ERROR):
        on_task_failure(make_context(RuntimeError("x")), hook=BrokenHook())  # no lanza

    assert "No se pudo registrar el fallo" in caplog.text
    assert "PIPELINE_FAILURE" in caplog.text


def test_on_task_failure_tolera_contexto_incompleto(monkeypatch):
    monkeypatch.delenv(WEBHOOK_ENV, raising=False)

    on_task_failure({}, hook=RecordingHook())  # sin ti, sin excepción: no lanza


def test_on_task_failure_sin_target_day_publicado(monkeypatch):
    monkeypatch.delenv(WEBHOOK_ENV, raising=False)
    hook = RecordingHook()

    on_task_failure(make_context(RuntimeError("x"), xcom=None), hook=hook)

    assert hook.executed[0][1]["target_day"] is None
