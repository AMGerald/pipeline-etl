"""DAG diario: Open-Meteo -> raw -> staging -> core (Etapa 4).

Cada run procesa el día `data_interval_start - LAG_DAYS` (UTC): el archivo de
Open-Meteo reemplaza datos provisionales por definitivos unos días después.
La lógica vive en src/weather; aquí solo se orquesta.
"""

from datetime import date, datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task

DEFAULT_LAG_DAYS = 5  # se sobrescribe con la Variable de Airflow `weather_lag_days`


def _lag_days() -> int:
    from airflow.sdk import Variable  # dentro de la función: nada pesado al parsear

    return int(Variable.get("weather_lag_days", default=DEFAULT_LAG_DAYS))


@dag(
    schedule="@daily",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,  # evita que dos runs escriban a la vez ventanas solapadas
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["weather", "etl"],
)
def weather_daily():
    @task
    def apply_schema() -> list[str]:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from weather.schema import apply_schema as apply

        return apply(PostgresHook(postgres_conn_id="warehouse_db"))

    @task
    def target_day() -> str:
        """Día a procesar. En runs manuales data_interval_start puede venir vacío."""
        ctx = get_current_context()
        base = (
            ctx.get("data_interval_start")
            or ctx.get("logical_date")
            or ctx["dag_run"].run_after
        )
        return (base.date() - timedelta(days=_lag_days())).isoformat()

    @task
    def get_locations(_schema_applied: list[str]) -> list[dict]:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from weather.extract import get_active_locations

        locations = get_active_locations(PostgresHook(postgres_conn_id="warehouse_db"))
        if not locations:
            raise ValueError(
                "dim_location no tiene ciudades activas; ejecuta `make ddl` para sembrarla"
            )
        return [vars(loc) for loc in locations]  # dicts: serializables por XCom

    @task
    def extract_city(location: dict, day: str) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from weather.extract import Location, extract_location

        return extract_location(
            PostgresHook(postgres_conn_id="warehouse_db"),
            Location(**location),
            date.fromisoformat(day),
            dag_run_id=get_current_context()["run_id"],
        )

    @task
    def load_staging(response_id: int) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from weather.transform import load_staging as load

        return load(PostgresHook(postgres_conn_id="warehouse_db"), response_id)

    @task
    def build_fact(day: str, _staged: list[int]) -> int:
        """Se ejecuta una vez, cuando todas las ciudades están en staging."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from weather.transform import build_fact_daily

        return build_fact_daily(
            PostgresHook(postgres_conn_id="warehouse_db"), date.fromisoformat(day)
        )

    @task
    def quality_checks(day: str, _built: int) -> dict:
        """Falla el run si algún check `error` encuentra violaciones (ver sql/checks/)."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from weather.checks import run_checks

        results = run_checks(
            PostgresHook(postgres_conn_id="warehouse_db"),
            date.fromisoformat(day),
            _lag_days(),
        )
        return {r.name: r.violations for r in results}

    schema = apply_schema()
    day = target_day()
    locations = get_locations(schema)
    responses = extract_city.partial(day=day).expand(location=locations)
    staged = load_staging.expand(response_id=responses)
    quality_checks(day, build_fact(day, staged))


weather_daily()
