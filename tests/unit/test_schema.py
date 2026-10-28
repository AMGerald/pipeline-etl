from weather.schema import SEED_FILES, apply_schema


class RecordingCursor:
    def __init__(self, executed):
        self.executed = executed

    def execute(self, sql):
        self.executed.append(sql)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class RecordingConn:
    def __init__(self):
        self.executed = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return RecordingCursor(self.executed)

    def close(self):
        self.closed = True


class FakeHook:
    def __init__(self):
        self.conn = RecordingConn()

    def get_conn(self):
        return self.conn


def test_aplica_ddl_en_orden_y_solo_los_seeds_seguros(tmp_path):
    (tmp_path / "ddl").mkdir()
    (tmp_path / "seed").mkdir()
    (tmp_path / "ddl" / "02_b.sql").write_text("DDL-B")
    (tmp_path / "ddl" / "01_a.sql").write_text("DDL-A")
    (tmp_path / "seed" / "01_dim_date.sql").write_text("SEED-DATE")
    (tmp_path / "seed" / "02_dim_location.sql").write_text("SEED-LOCATION")
    hook = FakeHook()

    applied = apply_schema(hook, tmp_path)

    assert hook.conn.executed == ["DDL-A", "DDL-B", "SEED-DATE"]  # dim_location NO se siembra
    assert applied == ["01_a.sql", "02_b.sql", "01_dim_date.sql"]
    assert hook.conn.closed


def test_dim_location_no_esta_entre_los_seeds_del_dag():
    assert not any("dim_location" in f for f in SEED_FILES)
