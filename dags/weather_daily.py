"""DAG diario: Open-Meteo -> raw -> staging -> core (Etapas 4, 5 y 7).

Cada run procesa el día `data_interval_start - LAG_DAYS` (UTC): el archivo de
Open-Meteo reemplaza datos provisionales por definitivos unos días después.
La lógica vive en src/weather; aquí solo se orquesta.

Observabilidad: `start_run` abre la fila en audit.pipeline_runs, `record_success`
la cierra y, si una tarea falla definitivamente, `on_failure_callback` la marca
como fallida y envía la alerta.
"""

from datetime import UTC, date, datetime, timedelta

from airflow.sdk import dag, get_current_context, task

DEFAULT_LAG_DAYS = 5  # se sobrescribe con la Variable de Airflow `weather_lag_days`


def _lag_days() -> int:
    from airflow.sdk import Variable  # dentro de la función: nada pesado al parsear

    return int(Variable.get("weather_lag_days", default=DEFAULT_LAG_DAYS))


def _hook():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id="warehouse_db")


def _notify_failure(context):
    from weather.observability import on_task_failure

    on_task_failure(context)


@dag(
    schedule="@daily",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,  # evita que dos runs escriban a la vez ventanas solapadas
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": _notify_failure,
    },
    tags=["weather", "etl"],
)
def weather_daily():
    @task
    def apply_schema() -> list[str]:
        from weather.schema import apply_schema as apply

        return apply(_hook())

    @task
    def target_day() -> str:
        """Día a procesar. En runs manuales data_interval_start puede venir vacío."""
        ctx = get_current_context()
        base = ctx.get("data_interval_start") or ctx.get("logical_date") or ctx["dag_run"].run_after
        return (base.date() - timedelta(days=_lag_days())).isoformat()

    @task
    def start_run(_schema_applied: list[str], day: str) -> str:
        """Abre la fila del run en audit.pipeline_runs (requiere el DDL aplicado)."""
        from weather.audit import start_run as start

        run = get_current_context()["dag_run"]
        start(_hook(), run.run_id, run.dag_id, date.fromisoformat(day))
        return run.run_id

    @task
    def get_locations(_run_started: str) -> list[dict]:
        from weather.extract import get_active_locations

        locations = get_active_locations(_hook())
        if not locations:
            raise ValueError(
                "dim_location no tiene ciudades activas; ejecuta `make ddl` para sembrarla"
            )
        return [vars(loc) for loc in locations]  # dicts: serializables por XCom

    @task
    def extract_city(location: dict, day: str) -> int:
        from weather.extract import Location, extract_location

        return extract_location(
            _hook(),
            Location(**location),
            date.fromisoformat(day),
            dag_run_id=get_current_context()["run_id"],
        )

    @task
    def load_staging(response_id: int) -> int:
        from weather.transform import load_staging as load

        return load(_hook(), response_id)

    @task
    def build_fact(day: str, _staged: list[int]) -> int:
        """Se ejecuta una vez, cuando todas las ciudades están en staging."""
        from weather.transform import build_fact_daily

        return build_fact_daily(_hook(), date.fromisoformat(day))

    @task
    def quality_checks(day: str, _built: int) -> dict:
        """Guarda los resultados en audit y falla el run si algún check `error` falla."""
        from weather.audit import record_check_results
        from weather.checks import evaluate_checks, raise_if_failed

        local_date = date.fromisoformat(day)
        results = evaluate_checks(_hook(), local_date, _lag_days())
        record_check_results(_hook(), get_current_context()["run_id"], results)
        raise_if_failed(results, local_date)  # después de guardar: queda registro aunque falle
        return {r.name: r.violations for r in results}

    @task
    def record_success(day: str, staged: list[int], fact_rows: int, _checks: dict) -> None:
        """Cierra la fila del run con los conteos por capa."""
        from weather.audit import finish_run_success

        run = get_current_context()["dag_run"]
        staged = list(staged)
        finish_run_success(
            _hook(),
            run.run_id,
            run.dag_id,
            date.fromisoformat(day),
            raw_responses=len(staged),
            staging_rows=sum(staged),
            fact_rows=fact_rows,
        )

    schema = apply_schema()
    day = target_day()
    started = start_run(schema, day)
    locations = get_locations(started)
    responses = extract_city.partial(day=day).expand(location=locations)
    staged = load_staging.expand(response_id=responses)
    built = build_fact(day, staged)
    checks = quality_checks(day, built)
    record_success(day, staged, built, checks)


weather_daily()
