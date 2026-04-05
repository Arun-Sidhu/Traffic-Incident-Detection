# db.py
from typing import Any

import psycopg2

from .settings import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER


def get_conn() -> Any:
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )