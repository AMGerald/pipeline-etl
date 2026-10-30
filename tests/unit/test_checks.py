from datetime import date

import pytest

from weather.checks import DataQualityError, _parse, run_checks

PASSING = "-- severity: {sev}\n-- description: pasa\nSELECT 1 WHERE false"
FAILING = "-- severity: {sev}\n-- description: falla\nSELECT 1 AS x, 'FAILING' AS motivo"


class FakeCursor:
    """Devuelve una fila si el SQL contiene 'FAILING', ninguna si no."""

    def __init__(self):
        self.description = None
        self._rows = []

    def execute(self, sql, params=None):
        if "FAILING" in sql:
            self.description = [type("C", (), {"name": "x"}), type("C", (), {"name": "motivo"})]
            self._rows = [(1, "FAILING")]
        else:
            self.description = [type("C", (), {"name": "x"})]
            self._rows = []

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return FakeCursor()

    def close(self):
        pass


class FakeHook:
    def get_conn(self):
        return FakeConn()


def write_checks(tmp_path, **files):
    for name, content in files.items():
        (tmp_path / f"{name}.sql").write_text(content)
    return tmp_path


def run(tmp_path):
    return run_checks(FakeHook(), date(2026, 10, 20), 5, checks_dir=tmp_path)


def test_parse_lee_severidad_y_descripcion(tmp_path):
    path = tmp_path / "01_x.sql"
    path.write_text(PASSING.format(sev="warn"))

    severity, description, sql = _parse(path)

    assert (severity, description) == ("warn", "pasa")
    assert "SELECT" in sql


@pytest.mark.parametrize(
    "content",
    [
        "SELECT 1",  # sin cabecera
        "-- severity: error\nSELECT 1",  # sin description
        "-- severity: critico\n-- description: x\nSELECT 1",  # severidad inválida
    ],
)
def test_parse_rechaza_cabeceras_invalidas(tmp_path, content):
    path = tmp_path / "01_x.sql"
    path.write_text(content)

    with pytest.raises(ValueError, match="cabecera"):
        _parse(path)


def test_todos_pasan(tmp_path):
    write_checks(tmp_path, a=PASSING.format(sev="error"), b=PASSING.format(sev="warn"))

    results = run(tmp_path)

    assert [(r.name, r.violations) for r in results] == [("a", 0), ("b", 0)]


def test_un_error_lanza_data_quality_error(tmp_path):
    write_checks(tmp_path, a=PASSING.format(sev="error"), b=FAILING.format(sev="error"))

    with pytest.raises(DataQualityError, match="b"):
        run(tmp_path)


def test_un_warn_no_lanza_pero_queda_registrado(tmp_path):
    write_checks(tmp_path, a=FAILING.format(sev="warn"))

    results = run(tmp_path)

    assert results[0].failed
    assert results[0].severity == "warn"
    assert results[0].sample == [{"x": 1, "motivo": "FAILING"}]


def test_corre_todos_los_checks_antes_de_fallar(tmp_path):
    write_checks(tmp_path, a=FAILING.format(sev="error"), b=FAILING.format(sev="error"))

    with pytest.raises(DataQualityError) as exc:
        run(tmp_path)

    assert str(exc.value).endswith(": a, b")  # reporta el panorama completo


def test_evaluate_checks_no_lanza_pero_raise_if_failed_si(tmp_path):
    from weather.checks import evaluate_checks, raise_if_failed

    write_checks(tmp_path, a=FAILING.format(sev="error"))

    results = evaluate_checks(FakeHook(), date(2026, 10, 20), 5, checks_dir=tmp_path)  # no lanza

    assert results[0].failed
    with pytest.raises(DataQualityError, match=": a$"):
        raise_if_failed(results, date(2026, 10, 20))
