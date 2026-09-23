from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

from .config import settings

log = logging.getLogger("sentinel.db")

_conninfo = settings.database_url
if _conninfo.startswith("postgresql://") and "+psycopg" not in _conninfo:
    pass


def wait_for_db(retries: int = 60, delay: float = 2.0) -> None:
    last = None
    for i in range(retries):
        try:
            with psycopg.connect(_conninfo, connect_timeout=5) as conn:
                conn.execute("SELECT 1")
            log.info("Database is reachable")
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            log.warning("Database not ready (%s/%s): %s", i + 1, retries, exc)
            time.sleep(delay)
    raise RuntimeError(f"Database never became ready: {last}")


@contextmanager
def connection() -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(_conninfo, row_factory=dict_row, autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetch_all(sql: str, params: tuple = ()) -> list[dict]:
    with connection() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def fetch_one(sql: str, params: tuple = ()) -> dict | None:
    with connection() as conn:
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None


def execute(sql: str, params: tuple = ()) -> None:
    with connection() as conn:
        conn.execute(sql, params)


def execute_many(sql: str, seq: list[tuple]) -> None:
    with connection() as conn:
        conn.executemany(sql, seq)


def upsert_json(sql: str, params: tuple) -> None:
    """Helper so callers can wrap dicts as Json."""
    with connection() as conn:
        conn.execute(sql, params)


Jsonb = Json
