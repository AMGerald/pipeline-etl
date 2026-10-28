"""Utilidades para los tests de integración: hook mínimo y payloads sintéticos."""

from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

import psycopg2

from weather.extract import Location, save_raw
from weather.transform import load_staging

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "sql"
DAY = date(2026, 10, 20)  # día UTC/local de referencia en los tests


class PgHook:
    """Imita la parte de PostgresHook que usa src/weather (sin depender de Airflow)."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    def get_conn(self):
        return psycopg2.connect(self.dsn)

    def get_records(self, sql, parameters=None):
        conn = self.get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(sql, parameters)
                return cur.fetchall()
        finally:
            conn.close()

    def run(self, sql, parameters=None):
        conn = self.get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(sql, parameters)
        finally:
            conn.close()


def make_payload(
    start: date,
    days: int = 2,
    *,
    temp: Callable[[int], float | None] = float,
    precip: Callable[[int], float] = lambda i: 0.0,
) -> dict:
    """Respuesta tipo Open-Meteo. `i` = horas desde `start` 00:00 UTC (0 .. 24*days-1)."""
    hours = range(24 * days)
    stamps = [f"{start + timedelta(days=i // 24)}T{i % 24:02d}:00" for i in hours]
    return {
        "latitude": -8.119508,
        "longitude": -79.0676,
        "elevation": 38.0,
        "hourly": {
            "time": stamps,
            "temperature_2m": [temp(i) for i in hours],
            "relative_humidity_2m": [60.0 for _ in hours],
            "precipitation": [precip(i) for i in hours],
            "wind_speed_10m": [10.0 for _ in hours],
            "weather_code": [0 for _ in hours],
        },
    }


def ingest(
    hook: PgHook,
    location: Location,
    start: date = DAY,
    days: int = 2,
    **payload_kwargs,
) -> int:
    """Simula extract + load_staging para una ciudad. Devuelve el response_id."""
    payload = make_payload(start, days, **payload_kwargs)
    end = start + timedelta(days=days - 1)
    response_id = save_raw(hook, location, start, end, "http://fake", 200, payload, "test")
    load_staging(hook, response_id)
    return response_id
