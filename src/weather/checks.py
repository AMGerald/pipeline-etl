"""Checks de calidad de datos: cada archivo de sql/checks/ devuelve las filas que
violan una regla (cero filas = el check pasa).

Cabecera obligatoria en cada archivo:
    -- severity: error | warn
    -- description: texto corto
`error` hace fallar el run; `warn` solo se registra en el log.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from weather.schema import SQL_DIR

log = logging.getLogger(__name__)

CHECKS_DIR = SQL_DIR / "checks"
SAMPLE_ROWS = 5
_HEADER = re.compile(r"^--\s*(severity|description):\s*(.+)$", re.MULTILINE)


class DataQualityError(Exception):
    """Algún check de severidad `error` encontró violaciones."""


@dataclass
class CheckResult:
    name: str
    severity: str
    description: str
    violations: int
    sample: list[dict] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return self.violations > 0


def _parse(path: Path) -> tuple[str, str, str]:
    sql = path.read_text()
    meta = dict(_HEADER.findall(sql))
    severity = meta.get("severity", "").strip()
    if severity not in ("error", "warn") or "description" not in meta:
        raise ValueError(f"{path.name}: falta cabecera `severity: error|warn` o `description:`")
    return severity, meta["description"].strip(), sql


def run_checks(
    hook, local_date: date, lag_days: int, checks_dir: Path = CHECKS_DIR
) -> list[CheckResult]:
    """Ejecuta todos los checks para un día local y devuelve los resultados.

    Lanza DataQualityError si algún check `error` falla (después de correr todos,
    para reportar el panorama completo y no solo el primer problema).
    """
    params = {"local_date": local_date, "lag_days": lag_days}
    results: list[CheckResult] = []

    conn = hook.get_conn()
    try:
        with conn, conn.cursor() as cur:
            for path in sorted(checks_dir.glob("*.sql")):
                severity, description, sql = _parse(path)
                cur.execute(sql, params)
                columns = [c.name for c in cur.description]
                rows = cur.fetchall()
                results.append(
                    CheckResult(
                        name=path.stem,
                        severity=severity,
                        description=description,
                        violations=len(rows),
                        sample=[dict(zip(columns, r)) for r in rows[:SAMPLE_ROWS]],
                    )
                )
    finally:
        conn.close()

    for r in results:
        if not r.failed:
            log.info("OK   %s", r.name)
        else:
            level = logging.ERROR if r.severity == "error" else logging.WARNING
            log.log(level, "FAIL %s [%s] %s: %s filas. Muestra: %s",
                    r.name, r.severity, r.description, r.violations, r.sample)

    failed = [r.name for r in results if r.failed and r.severity == "error"]
    if failed:
        raise DataQualityError(f"Checks con error para {local_date}: {', '.join(failed)}")
    return results
