from datetime import date

import pytest
import requests
from tenacity import wait_none

from weather import extract
from weather.extract import (
    MAX_ATTEMPTS,
    ExtractionError,
    Location,
    RetryableAPIError,
    extraction_window,
    fetch_archive,
)

TRU = Location("TRU", -8.1116, -79.0288)
START, END = date(2026, 10, 20), date(2026, 10, 21)


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"hourly": {"time": []}}
        self.text = text
        self.url = "https://archive-api.open-meteo.com/v1/archive?fake=1"

    def json(self):
        return self._payload


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch):
    """Los tests no deben dormir entre reintentos."""
    monkeypatch.setattr(fetch_archive.retry, "wait", wait_none())


def patch_get(monkeypatch, outcomes):
    """Reemplaza requests.get por una secuencia de respuestas/excepciones."""
    calls = []
    seq = iter(outcomes)

    def fake_get(url, params=None, timeout=None):
        calls.append({"url": url, "params": params, "timeout": timeout})
        outcome = next(seq)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(extract.requests, "get", fake_get)
    return calls


def test_extraction_window_incluye_el_dia_utc_siguiente():
    assert extraction_window(date(2026, 10, 20)) == (date(2026, 10, 20), date(2026, 10, 21))


def test_extraction_window_cruza_fin_de_mes():
    assert extraction_window(date(2026, 10, 31)) == (date(2026, 10, 31), date(2026, 11, 1))


def test_fetch_archive_construye_los_parametros(monkeypatch):
    calls = patch_get(monkeypatch, [FakeResponse(200, {"hourly": {"time": ["x"]}})])

    url, status, payload = fetch_archive(TRU, START, END)

    assert status == 200
    assert payload == {"hourly": {"time": ["x"]}}
    assert url.startswith("https://archive-api.open-meteo.com")
    params = calls[0]["params"]
    assert params["latitude"] == -8.1116
    assert params["longitude"] == -79.0288
    assert params["start_date"] == "2026-10-20"
    assert params["end_date"] == "2026-10-21"
    assert params["timezone"] == "GMT"
    assert params["hourly"].split(",") == list(extract.HOURLY_VARS)
    assert calls[0]["timeout"] == extract.REQUEST_TIMEOUT_S


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_reintenta_errores_transitorios_y_termina_bien(monkeypatch, status):
    calls = patch_get(monkeypatch, [FakeResponse(status), FakeResponse(status), FakeResponse(200)])

    _, code, _ = fetch_archive(TRU, START, END)

    assert code == 200
    assert len(calls) == 3


@pytest.mark.parametrize("exc", [requests.Timeout("lento"), requests.ConnectionError("caído")])
def test_reintenta_timeouts_y_errores_de_conexion(monkeypatch, exc):
    calls = patch_get(monkeypatch, [exc, FakeResponse(200)])

    fetch_archive(TRU, START, END)

    assert len(calls) == 2


def test_error_400_no_se_reintenta(monkeypatch):
    calls = patch_get(monkeypatch, [FakeResponse(400, text='{"reason":"Bad Request"}')])

    with pytest.raises(ExtractionError, match="HTTP 400 para TRU"):
        fetch_archive(TRU, START, END)

    assert len(calls) == 1


def test_agota_los_reintentos_y_relanza_el_error_original(monkeypatch):
    calls = patch_get(monkeypatch, [FakeResponse(503)] * MAX_ATTEMPTS)

    with pytest.raises(RetryableAPIError):  # reraise=True: no RetryError
        fetch_archive(TRU, START, END)

    assert len(calls) == MAX_ATTEMPTS
