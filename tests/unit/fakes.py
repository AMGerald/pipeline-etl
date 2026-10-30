"""Dobles de prueba compartidos: un hook que solo registra lo que se ejecuta."""


class RecordingCursor:
    def __init__(self, executed):
        self.executed = executed
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class RecordingConn:
    def __init__(self, executed):
        self.executed = executed

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return RecordingCursor(self.executed)

    def close(self):
        pass


class RecordingHook:
    def __init__(self):
        self.executed = []

    def get_conn(self):
        return RecordingConn(self.executed)


class BrokenHook:
    def get_conn(self):
        raise RuntimeError("base de datos caída")
