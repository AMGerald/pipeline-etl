"""Aplicación del DDL del warehouse (idempotente: IF NOT EXISTS / ON CONFLICT)."""

import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

SQL_DIR = Path(os.environ.get("WAREHOUSE_SQL_DIR", "/opt/airflow/sql"))

# dim_location NO se siembra aquí: la tabla es la fuente de verdad y el seed
# pisaría correcciones hechas a mano. Se siembra una vez con `make ddl`.
SEED_FILES = ("seed/01_dim_date.sql",)


def apply_schema(hook, sql_dir: Path = SQL_DIR) -> list[str]:
    """Ejecuta ddl/*.sql (en orden) y los seeds seguros. Devuelve los archivos aplicados."""
    files = sorted((sql_dir / "ddl").glob("*.sql")) + [sql_dir / f for f in SEED_FILES]
    conn = hook.get_conn()
    try:
        with conn, conn.cursor() as cur:  # una sola transacción: todo o nada
            for f in files:
                cur.execute(f.read_text())
                log.info("aplicado %s", f.name)
    finally:
        conn.close()
    return [f.name for f in files]
