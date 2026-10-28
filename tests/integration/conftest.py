import psycopg2
import pytest
from testcontainers.community.postgres import PostgresContainer

from tests.integration.helpers import SQL_DIR, PgHook


@pytest.fixture(scope="session")
def _postgres():
    """Un Postgres 16 por sesión, con el DDL y los seeds reales del proyecto."""
    with PostgresContainer("postgres:16") as pg:
        dsn = (
            f"host={pg.get_container_host_ip()} port={pg.get_exposed_port(5432)} "
            f"user={pg.username} password={pg.password} dbname={pg.dbname}"
        )
        conn = psycopg2.connect(dsn)
        try:
            with conn, conn.cursor() as cur:
                for folder in ("init", "ddl", "seed"):
                    for path in sorted((SQL_DIR / folder).glob("*.sql")):
                        cur.execute(path.read_text())
        finally:
            conn.close()
        yield dsn


@pytest.fixture
def hook(_postgres):
    """Hook sobre una base limpia: raw, staging y hecho vacíos; ciudades activas."""
    hook = PgHook(_postgres)
    hook.run(
        """
        TRUNCATE raw.open_meteo_responses RESTART IDENTITY;
        TRUNCATE staging.weather_hourly;
        TRUNCATE core.fact_weather_daily;
        UPDATE core.dim_location SET is_active = true;
        """
    )
    return hook
