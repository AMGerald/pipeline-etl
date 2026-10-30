import pytest

from tests.integration.helpers import DAY, ingest
from weather import audit
from weather.checks import CheckResult, evaluate_checks
from weather.extract import get_active_locations
from weather.transform import build_fact_daily

pytestmark = pytest.mark.integration

RUN, DAG = "manual__2026-10-27", "weather_daily"


@pytest.fixture(autouse=True)
def clean_audit(hook):
    hook.run("TRUNCATE audit.pipeline_runs CASCADE")


def run_row(hook, run_id=RUN):
    (row,) = hook.get_records(
        "SELECT state, target_day, finished_at IS NOT NULL, raw_responses, staging_rows, "
        "fact_rows, failed_task, error FROM audit.pipeline_runs WHERE run_id = %s",
        (run_id,),
    )
    return row


def test_start_run_abre_el_run_en_estado_running(hook):
    audit.start_run(hook, RUN, DAG, DAY)

    assert run_row(hook) == ("running", DAY, False, None, None, None, None, None)


def test_run_exitoso_guarda_los_conteos(hook):
    audit.start_run(hook, RUN, DAG, DAY)

    audit.finish_run_success(hook, RUN, DAG, DAY, raw_responses=4, staging_rows=192, fact_rows=4)

    assert run_row(hook) == ("success", DAY, True, 4, 192, 4, None, None)


def test_run_fallido_guarda_tarea_y_error_truncado(hook):
    audit.start_run(hook, RUN, DAG, DAY)

    audit.finish_run_failed(hook, RUN, DAG, "extract_city[1]", "boom " * 1000, DAY)

    state, day, finished, *_, failed_task, error = run_row(hook)
    assert (state, day, finished, failed_task) == ("failed", DAY, True, "extract_city[1]")
    assert len(error) == audit.MAX_ERROR_CHARS


def test_fallo_sin_run_previo_crea_la_fila(hook):
    # p. ej. falla la primera tarea, antes de start_run
    audit.finish_run_failed(hook, RUN, DAG, "apply_schema", "sin permisos")

    assert run_row(hook)[:2] == ("failed", None)


def test_fallo_sin_target_day_no_pisa_el_registrado(hook):
    audit.start_run(hook, RUN, DAG, DAY)

    audit.finish_run_failed(hook, RUN, DAG, "x", "e", target_day=None)

    assert run_row(hook)[1] == DAY


def test_reintentar_un_run_fallido_hasta_exito_reutiliza_la_fila(hook):
    audit.start_run(hook, RUN, DAG, DAY)
    audit.finish_run_failed(hook, RUN, DAG, "extract_city[0]", "timeout", DAY)

    audit.finish_run_success(hook, RUN, DAG, DAY, 4, 192, 4)  # se limpió la tarea y pasó

    assert run_row(hook) == ("success", DAY, True, 4, 192, 4, None, None)
    (count,) = hook.get_records("SELECT count(*) FROM audit.pipeline_runs")[0]
    assert count == 1


def test_start_run_de_un_run_existente_lo_reinicia(hook):
    audit.start_run(hook, RUN, DAG, DAY)
    audit.finish_run_failed(hook, RUN, DAG, "x", "e", DAY)

    audit.start_run(hook, RUN, DAG, DAY)

    assert run_row(hook) == ("running", DAY, False, None, None, None, None, None)


def test_estado_invalido_lo_rechaza_la_base(hook):
    with pytest.raises(Exception, match="check"):
        hook.run(
            "INSERT INTO audit.pipeline_runs (run_id, dag_id, state) VALUES ('x', 'd', 'raro')"
        )


def test_guarda_los_resultados_de_los_checks_incluso_con_violaciones(hook):
    for loc in get_active_locations(hook):
        ingest(hook, loc)
    build_fact_daily(hook, DAY)
    hook.run(
        "UPDATE staging.weather_hourly SET temperature_c = 99.9 "
        "WHERE location_code = 'CUZ' AND observed_at = '2026-10-20 12:00+00'"
    )
    audit.start_run(hook, RUN, DAG, DAY)

    from tests.integration.helpers import SQL_DIR

    results = evaluate_checks(hook, DAY, 100_000, checks_dir=SQL_DIR / "checks")
    audit.record_check_results(hook, RUN, results)

    rows = dict(
        (name, (sev, n))
        for name, sev, n in hook.get_records(
            "SELECT check_name, severity, violations FROM audit.check_results WHERE run_id = %s",
            (RUN,),
        )
    )
    assert len(rows) == 6
    assert rows["03_staging_ranges"] == ("error", 1)
    assert rows["01_raw_array_lengths"] == ("error", 0)
    (sample,) = hook.get_records(
        "SELECT sample->0->>'location_code' FROM audit.check_results "
        "WHERE run_id = %s AND check_name = '03_staging_ranges'",
        (RUN,),
    )[0]
    assert sample == "CUZ"


def test_record_check_results_es_idempotente_por_run_y_check(hook):
    audit.start_run(hook, RUN, DAG, DAY)
    first = [CheckResult("c1", "error", "d", 3, [{"a": 1}])]
    second = [CheckResult("c1", "error", "d", 0, [])]

    audit.record_check_results(hook, RUN, first)
    audit.record_check_results(hook, RUN, second)

    assert hook.get_records("SELECT check_name, violations FROM audit.check_results") == [("c1", 0)]


def test_check_results_requiere_el_run_y_se_borra_en_cascada(hook):
    with pytest.raises(Exception, match="foreign key"):
        audit.record_check_results(hook, "no-existe", [CheckResult("c", "warn", "d", 0)])

    audit.start_run(hook, RUN, DAG, DAY)
    audit.record_check_results(hook, RUN, [CheckResult("c", "warn", "d", 0)])
    hook.run("DELETE FROM audit.pipeline_runs WHERE run_id = %s", (RUN,))
    assert hook.get_records("SELECT count(*) FROM audit.check_results")[0][0] == 0
