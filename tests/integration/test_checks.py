import pytest

from tests.integration.helpers import DAY, SQL_DIR, ingest
from weather.checks import DataQualityError, run_checks
from weather.extract import get_active_locations
from weather.transform import build_fact_daily

pytestmark = pytest.mark.integration

CHECKS_DIR = SQL_DIR / "checks"
NO_STALE = 100_000  # lag enorme: el check de freshness no molesta en estos tests


@pytest.fixture
def loaded(hook):
    """Las 4 ciudades cargadas de punta a punta para el día DAY, datos sanos."""
    for loc in get_active_locations(hook):
        ingest(hook, loc)
    build_fact_daily(hook, DAY)
    return hook


def run(hook, lag_days=NO_STALE):
    return run_checks(hook, DAY, lag_days, checks_dir=CHECKS_DIR)


def test_datos_sanos_pasan_todos_los_checks(loaded):
    results = run(loaded)

    assert len(results) == 6
    assert [r.name for r in results if r.failed] == []


# Cada corrupción debe hacer fallar exactamente su check (y ninguno más).
ERROR_CASES = [
    pytest.param(
        "UPDATE raw.open_meteo_responses SET payload = jsonb_set(payload, "
        "'{hourly,precipitation}', (payload #> '{hourly,precipitation}') - 0) "
        "WHERE response_id = 1",
        "01_raw_array_lengths",
        id="array-mas-corto",
    ),
    pytest.param(
        "UPDATE raw.open_meteo_responses SET payload = payload - 'hourly' WHERE response_id = 1",
        "01_raw_array_lengths",
        id="sin-hourly",
    ),
    pytest.param(
        "UPDATE staging.weather_hourly SET temperature_c = 99.9 "
        "WHERE location_code = 'CUZ' AND observed_at = '2026-10-20 12:00+00'",
        "03_staging_ranges",
        id="temperatura-imposible",
    ),
    pytest.param(
        "UPDATE staging.weather_hourly SET relative_humidity_pct = 130 "
        "WHERE location_code = 'CUZ' AND observed_at = '2026-10-20 12:00+00'",
        "03_staging_ranges",
        id="humedad-mayor-a-100",
    ),
    pytest.param(
        "DELETE FROM core.fact_weather_daily WHERE location_key = "
        "(SELECT location_key FROM core.dim_location WHERE location_code = 'AQP')",
        "04_fact_completeness",
        id="ciudad-sin-fila-en-el-hecho",
    ),
    pytest.param(
        "UPDATE core.fact_weather_daily SET precipitation_total_mm = 55 WHERE location_key = "
        "(SELECT location_key FROM core.dim_location WHERE location_code = 'TRU')",
        "05_fact_reconciliation",
        id="hecho-no-cuadra-con-staging",
    ),
]


@pytest.mark.parametrize(("corruption", "expected_check"), ERROR_CASES)
def test_cada_check_de_error_detecta_su_corrupcion(loaded, corruption, expected_check):
    loaded.run(corruption)

    with pytest.raises(DataQualityError) as exc:
        run(loaded)

    failing = str(exc.value).split(": ", 1)[1]
    assert failing == expected_check


def test_nulos_en_staging_son_warn_y_no_bloquean(loaded):
    loaded.run(
        "UPDATE staging.weather_hourly SET wind_speed_kmh = NULL "
        "WHERE location_code = 'LIM' AND observed_at = '2026-10-20 12:00+00'"
    )

    results = {r.name: r for r in run(loaded)}  # no lanza

    assert results["02_staging_null_measures"].violations == 1
    assert results["02_staging_null_measures"].severity == "warn"


def test_freshness_avisa_cuando_el_pipeline_esta_detenido(loaded):
    results = {r.name: r for r in run(loaded, lag_days=-NO_STALE)}  # no lanza

    assert results["06_fact_freshness"].violations == 4
    assert results["06_fact_freshness"].severity == "warn"


def test_dia_con_horas_faltantes_en_staging_falla_completitud(loaded):
    loaded.run(
        "DELETE FROM staging.weather_hourly WHERE location_code = 'LIM' "
        "AND observed_at BETWEEN '2026-10-20 12:00+00' AND '2026-10-20 16:00+00'"
    )
    build_fact_daily(loaded, DAY)  # el hecho se recalcula y queda consistente: 19 h

    with pytest.raises(DataQualityError) as exc:
        run(loaded)

    assert str(exc.value).split(": ", 1)[1] == "04_fact_completeness"
    ((_, hours),) = loaded.get_records(
        "SELECT l.location_code, f.hours_observed FROM core.fact_weather_daily f "
        "JOIN core.dim_location l USING (location_key) WHERE l.location_code = 'LIM'"
    )
    assert hours == 19


def test_freshness_marca_ciudades_que_nunca_tuvieron_datos(hook):
    sql = (CHECKS_DIR / "06_fact_freshness.sql").read_text()

    rows = hook.get_records(sql, {"lag_days": 5})

    assert [(code, latest) for code, latest in rows] == [
        ("AQP", None),
        ("CUZ", None),
        ("LIM", None),
        ("TRU", None),
    ]
