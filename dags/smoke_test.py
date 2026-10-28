"""DAG de verificación del entorno (Etapa 0).

Comprueba que:
  1. Airflow puede conectarse al warehouse con la connection `warehouse_db`.
  2. Existen los schemas raw, staging, core y audit.
  3. El paquete `weather` (src/) es importable desde las tareas.
"""

from datetime import UTC, datetime

from airflow.sdk import dag, task

EXPECTED_SCHEMAS = ["audit", "core", "raw", "staging"]


@dag(
    schedule=None,  # solo se ejecuta manualmente
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["infra"],
)
def smoke_test():
    @task
    def check_warehouse() -> list[str]:
        # Import dentro de la tarea: el DAG processor no lo carga al parsear
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="warehouse_db")
        rows = hook.get_records(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = ANY(%s)
            ORDER BY schema_name
            """,
            parameters=(EXPECTED_SCHEMAS,),
        )
        found = [r[0] for r in rows]
        if found != EXPECTED_SCHEMAS:
            raise ValueError(f"Schemas esperados {EXPECTED_SCHEMAS}, encontrados {found}")
        return found

    @task
    def check_src_import() -> str:
        import weather

        return weather.__version__

    check_warehouse()
    check_src_import()


smoke_test()
