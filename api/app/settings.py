# settings.py
import os
from pathlib import Path

API_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = API_DIR / "static"

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "trafficdb")
DB_USER = os.getenv("DB_USER", "traffic")
DB_PASS = os.getenv("DB_PASS", "traffic")

# Conversion factor from meters per second to miles per hour.
MPS_TO_MPH = 2.2369362920544


def _split_csv_env(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _get_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


DEFAULT_CORS_ORIGINS = ",".join(
    [
        "http://localhost",
        "http://127.0.0.1",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]
)

CORS_ALLOW_ORIGINS = _split_csv_env("CORS_ALLOW_ORIGINS", DEFAULT_CORS_ORIGINS)
CORS_ALLOW_CREDENTIALS = _get_bool_env("CORS_ALLOW_CREDENTIALS", False)