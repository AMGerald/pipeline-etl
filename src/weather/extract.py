"""Extracción: Open-Meteo Archive API -> raw.open_meteo_responses.

Sin dependencia de Airflow en el módulo (el hook se recibe por parámetro),
para poder probarlo con mocks.
"""

import json
import logging
from dataclasses import dataclass
from datetime import date, timedelta

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

log = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
)
REQUEST_TIMEOUT_S = 30
MAX_ATTEMPTS = 5


@dataclass(frozen=True)
class Location:
    location_code: str
    latitude: float
    longitude: float


class RetryableAPIError(Exception):
    """Fallo transitorio (429 / 5xx): vale la pena reintentar."""


class ExtractionError(Exception):
    """Fallo permanente (4xx distinto de 429): reintentar daría lo mismo."""


def get_active_locations(hook) -> list[Location]:
    """Ciudades a extraer: dim_location es la fuente de verdad."""
    rows = hook.get_records(
        """
        SELECT location_code, latitude, longitude
        FROM core.dim_location
        WHERE is_active
        ORDER BY location_code
        """
    )
    return [Location(code, float(lat), float(lon)) for code, lat, lon in rows]


def extraction_window(utc_day: date) -> tuple[date, date]:
    """Ventana (inclusiva) a pedir para cerrar el día local que empieza en `utc_day`.

    Perú es UTC-5: el día local D va de D 05:00 UTC a D+1 05:00 UTC, así que
    hace falta también el día UTC siguiente.
    """
    return utc_day, utc_day + timedelta(days=1)


@retry(
    retry=retry_if_exception_type(
        (RetryableAPIError, requests.Timeout, requests.ConnectionError)
    ),
    stop=stop_after_attempt(MAX_ATTEMPTS),
    wait=wait_exponential_jitter(initial=2, max=60),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def fetch_archive(
    location: Location, start: date, end: date, session: requests.Session | None = None
) -> tuple[str, int, dict]:
    """Llama a la API para una ciudad y ventana. Devuelve (url, status, payload)."""
    params = {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "GMT",
    }
    resp = (session or requests).get(ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT_S)

    if resp.status_code == 429 or resp.status_code >= 500:
        raise RetryableAPIError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    if resp.status_code >= 400:
        raise ExtractionError(
            f"HTTP {resp.status_code} para {location.location_code} "
            f"{start}..{end}: {resp.text[:200]}"
        )
    return resp.url, resp.status_code, resp.json()


def save_raw(
    hook,
    location: Location,
    start: date,
    end: date,
    url: str,
    status: int,
    payload: dict,
    dag_run_id: str | None = None,
) -> int:
    """Inserta una fila en raw (append-only) y devuelve su response_id."""
    conn = hook.get_conn()
    try:
        with conn, conn.cursor() as cur:  # `with conn` = commit/rollback de la transacción
            cur.execute(
                """
                INSERT INTO raw.open_meteo_responses
                    (location_code, window_start, window_end, request_url,
                     api_latitude, api_longitude, api_elevation_m,
                     http_status, payload, dag_run_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                RETURNING response_id
                """,
                (
                    location.location_code,
                    start,
                    end,
                    url,
                    payload.get("latitude"),
                    payload.get("longitude"),
                    payload.get("elevation"),
                    status,
                    json.dumps(payload),
                    dag_run_id,
                ),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def extract_location(
    hook, location: Location, utc_day: date, dag_run_id: str | None = None
) -> int:
    """Extrae la ventana de una ciudad y la guarda en raw. Devuelve response_id."""
    start, end = extraction_window(utc_day)
    url, status, payload = fetch_archive(location, start, end)
    response_id = save_raw(hook, location, start, end, url, status, payload, dag_run_id)
    log.info(
        "raw.open_meteo_responses id=%s %s %s..%s (%s horas)",
        response_id,
        location.location_code,
        start,
        end,
        len(payload["hourly"]["time"]),
    )
    return response_id
