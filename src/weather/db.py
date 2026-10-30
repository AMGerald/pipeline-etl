"""Acceso mínimo a la base a través de un hook (psycopg2 por debajo)."""


def execute(hook, sql: str, params: dict | None = None) -> int:
    """Ejecuta una sentencia en su propia transacción y devuelve las filas afectadas."""
    conn = hook.get_conn()
    try:
        with conn, conn.cursor() as cur:  # `with conn` = commit/rollback
            cur.execute(sql, params)
            return cur.rowcount
    finally:
        conn.close()
