"""Exploración de la API de Open-Meteo (Etapa 1).

Solo usa la librería estándar: ejecútalo con `python3 scripts/explore_open_meteo.py`.
Objetivo: entender la forma de la respuesta ANTES de diseñar tablas.
"""

import json
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, timedelta

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
]
# Trujillo
LATITUDE, LONGITUDE = -8.1116, -79.0288


def fetch(params: dict) -> tuple[str, int, dict]:
    url = f"{ARCHIVE_URL}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        return url, resp.status, json.load(resp)


def summarize(data: dict) -> None:
    print("\n1) Claves de primer nivel:", list(data))

    print("\n2) Coordenadas pedidas vs devueltas:")
    print(f"   pedidas  : {LATITUDE}, {LONGITUDE}")
    print(f"   devueltas: {data['latitude']}, {data['longitude']}  (elevación {data.get('elevation')} m)")
    print(f"   timezone : {data['timezone']}  offset: {data['utc_offset_seconds']} s")

    print("\n3) Unidades:", json.dumps(data["hourly_units"], ensure_ascii=False))

    hourly = data["hourly"]
    print("\n4) Longitud de cada array:", {k: len(v) for k, v in hourly.items()})

    print("\n5) Primeras 3 filas (arrays paralelos 'zipeados'):")
    for i in range(3):
        print("   ", {k: hourly[k][i] for k in hourly})

    print("\n6) Disponibilidad por día (horas / horas con algún nulo):")
    by_day = defaultdict(lambda: [0, 0])
    for i, ts in enumerate(hourly["time"]):
        day = ts[:10]
        by_day[day][0] += 1
        if any(hourly[v][i] is None for v in HOURLY_VARS):
            by_day[day][1] += 1
    for day, (hours, with_nulls) in sorted(by_day.items()):
        flag = "  <-- datos incompletos" if with_nulls else ""
        print(f"   {day}: {hours} h, {with_nulls} con nulos{flag}")


def show_error_format() -> None:
    print("\n7) Formato de un error (start_date > end_date):")
    try:
        fetch({"latitude": LATITUDE, "longitude": LONGITUDE,
               "start_date": "2026-01-10", "end_date": "2026-01-01",
               "hourly": "temperature_2m"})
    except urllib.error.HTTPError as e:
        print(f"   HTTP {e.code}: {e.read().decode()}")


def main() -> None:
    today = date.today()
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": (today - timedelta(days=14)).isoformat(),
        "end_date": (today - timedelta(days=1)).isoformat(),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "GMT",
    }
    url, status, data = fetch(params)
    print(f"GET {url}\nHTTP {status}")
    summarize(data)
    show_error_format()


if __name__ == "__main__":
    main()
