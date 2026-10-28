from datetime import date

import pytest

from tests.integration.helpers import DAY, ingest, make_payload
from weather.extract import Location, get_active_locations, save_raw
from weather.transform import build_fact_daily, load_staging

pytestmark = pytest.mark.integration

TRU = Location("TRU", -8.1116, -79.0288)
TRU_START = "2026-10-20 00:00+00"


def staging_rows(hook, code="TRU"):
    return hook.get_records(
        "SELECT observed_at, temperature_c, source_response_id FROM staging.weather_hourly "
        "WHERE location_code = %s ORDER BY observed_at",
        (code,),
    )


def fact_rows(hook, code="TRU", day=DAY):
    return hook.get_records(
        """
        SELECT f.temp_min_c, f.temp_max_c, f.temp_avg_c, f.precipitation_total_mm,
               f.rainy_hours, f.hours_observed
        FROM core.fact_weather_daily f
        JOIN core.dim_location l USING (location_key)
        JOIN core.dim_date d USING (date_key)
        WHERE l.location_code = %s AND d.full_date = %s
        """,
        (code, day),
    )


# --- extract -----------------------------------------------------------------


def test_get_active_locations_lee_de_dim_location_y_respeta_is_active(hook):
    assert [loc.location_code for loc in get_active_locations(hook)] == [
        "AQP",
        "CUZ",
        "LIM",
        "TRU",
    ]

    hook.run("UPDATE core.dim_location SET is_active = false WHERE location_code = 'LIM'")

    assert [loc.location_code for loc in get_active_locations(hook)] == ["AQP", "CUZ", "TRU"]


def test_save_raw_guarda_payload_completo_y_coordenadas_devueltas(hook):
    payload = make_payload(DAY)

    response_id = save_raw(hook, TRU, DAY, date(2026, 10, 21), "http://x", 200, payload, "run-1")

    (row,) = hook.get_records(
        "SELECT api_latitude, api_longitude, api_elevation_m, http_status, dag_run_id, "
        "jsonb_array_length(payload #> '{hourly,time}') "
        "FROM raw.open_meteo_responses WHERE response_id = %s",
        (response_id,),
    )
    assert [float(v) for v in row[:3]] == [-8.119508, -79.0676, 38.0]
    assert row[3:] == (200, "run-1", 48)


def test_raw_es_append_only_cada_extraccion_es_una_fila_nueva(hook):
    ingest(hook, TRU)
    ingest(hook, TRU)

    (count,) = hook.get_records("SELECT count(*) FROM raw.open_meteo_responses")[0]
    assert count == 2


# --- raw -> staging ----------------------------------------------------------


def test_load_staging_pivota_los_arrays_a_una_fila_por_hora(hook):
    response_id = ingest(hook, TRU)

    rows = staging_rows(hook)

    assert len(rows) == 48
    assert rows[0][0].isoformat() == "2026-10-20T00:00:00+00:00"
    assert rows[-1][0].isoformat() == "2026-10-21T23:00:00+00:00"
    assert [float(r[1]) for r in rows] == [float(i) for i in range(48)]
    assert {r[2] for r in rows} == {response_id}


def test_load_staging_conserva_los_nulos_de_la_fuente(hook):
    ingest(hook, TRU, temp=lambda i: None if i == 3 else 20.0)

    nulls = hook.get_records(
        "SELECT observed_at FROM staging.weather_hourly WHERE temperature_c IS NULL"
    )

    assert [r[0].hour for r in nulls] == [3]


def test_load_staging_es_idempotente(hook):
    response_id = ingest(hook, TRU)
    before = staging_rows(hook)

    load_staging(hook, response_id)
    load_staging(hook, response_id)

    assert staging_rows(hook) == before


def test_una_respuesta_nueva_actualiza_staging(hook):
    ingest(hook, TRU, temp=lambda i: 10.0)
    new_id = ingest(hook, TRU, temp=lambda i: 20.0)

    rows = staging_rows(hook)

    assert len(rows) == 48
    assert {float(r[1]) for r in rows} == {20.0}
    assert {r[2] for r in rows} == {new_id}


def test_reprocesar_una_respuesta_vieja_no_pisa_a_la_nueva(hook):
    old_id = ingest(hook, TRU, temp=lambda i: 10.0)
    new_id = ingest(hook, TRU, temp=lambda i: 20.0)

    load_staging(hook, old_id)  # reproceso fuera de orden

    rows = staging_rows(hook)
    assert {float(r[1]) for r in rows} == {20.0}
    assert {r[2] for r in rows} == {new_id}


def test_ventanas_solapadas_no_duplican_horas(hook):
    ingest(hook, TRU, start=date(2026, 10, 20))  # 20 y 21
    ingest(hook, TRU, start=date(2026, 10, 21))  # 21 y 22 (solapa el 21)

    assert len(staging_rows(hook)) == 72  # 3 días, no 4


# --- staging -> core ---------------------------------------------------------


def test_dia_local_de_lima_va_de_05_utc_a_05_utc_del_dia_siguiente(hook):
    ingest(hook, TRU, temp=float)  # temp = horas desde 2026-10-20 00:00 UTC

    build_fact_daily(hook, DAY)

    # Día local 20-oct = offsets 5 (05:00 UTC) .. 28 (04:00 UTC del 21)
    ((tmin, tmax, tavg, _, _, hours),) = fact_rows(hook)
    assert (float(tmin), float(tmax), float(tavg)) == (5.0, 28.0, 16.5)
    assert hours == 24


def test_dia_incompleto_se_refleja_en_hours_observed(hook):
    ingest(hook, TRU, days=1)  # solo el día UTC 20: faltan las 5 h del 21 (00-04 UTC)

    build_fact_daily(hook, DAY)

    ((_, _, _, _, _, hours),) = fact_rows(hook)
    assert hours == 19  # 05:00 .. 23:00 UTC


def test_hora_sin_temperatura_no_cuenta_como_observada(hook):
    ingest(hook, TRU, temp=lambda i: None if i == 10 else 20.0)

    build_fact_daily(hook, DAY)

    ((_, _, _, _, _, hours),) = fact_rows(hook)
    assert hours == 23


def test_lluvia_solo_cuenta_dentro_del_dia_local(hook):
    rain = {2: 9.0, 10: 0.5, 11: 0.5, 12: 0.5, 29: 7.0}  # 2 y 29 caen fuera del día local

    ingest(hook, TRU, precip=lambda i: rain.get(i, 0.0))
    build_fact_daily(hook, DAY)

    ((_, _, _, total, rainy_hours, _),) = fact_rows(hook)
    assert float(total) == 1.5
    assert rainy_hours == 3


def test_build_fact_daily_es_idempotente(hook):
    for loc in get_active_locations(hook):
        ingest(hook, loc)

    build_fact_daily(hook, DAY)
    first = hook.get_records("SELECT * FROM core.fact_weather_daily ORDER BY location_key")
    build_fact_daily(hook, DAY)
    second = hook.get_records("SELECT * FROM core.fact_weather_daily ORDER BY location_key")

    assert len(first) == len(second) == 4
    assert [r[:10] for r in first] == [r[:10] for r in second]  # todo menos loaded_at


def test_sin_datos_en_staging_no_se_crean_filas(hook):
    assert build_fact_daily(hook, DAY) == 0


def test_el_hecho_solo_incluye_ciudades_activas(hook):
    for loc in get_active_locations(hook):
        ingest(hook, loc)
    hook.run("UPDATE core.dim_location SET is_active = false WHERE location_code = 'LIM'")

    assert build_fact_daily(hook, DAY) == 3
