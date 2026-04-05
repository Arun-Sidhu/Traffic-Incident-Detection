# conftest.py
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if "psycopg2" not in sys.modules:
    psycopg2_stub = types.ModuleType("psycopg2")
    def _connect(*args, **kwargs):
        raise RuntimeError("psycopg2.connect should be monkeypatched in tests")
    psycopg2_stub.connect = _connect
    sys.modules["psycopg2"] = psycopg2_stub

if "prometheus_fastapi_instrumentator" not in sys.modules:
    instrumentator_mod = types.ModuleType("prometheus_fastapi_instrumentator")
    class Instrumentator:
        def instrument(self, app):
            return self
        def expose(self, app, endpoint="/metrics", include_in_schema=False):
            return self
    instrumentator_mod.Instrumentator = Instrumentator
    sys.modules["prometheus_fastapi_instrumentator"] = instrumentator_mod
