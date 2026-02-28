"""
TimescaleDB connection pool and upsert helpers.

Environment variables (or .env):
  BDSP_DB_HOST, BDSP_DB_PORT, BDSP_DB_NAME, BDSP_DB_USER, BDSP_DB_PASSWORD
"""

import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2 import pool

# ─── Connection Pool ──────────────────────────────────────────────────────────

_pool: pool.ThreadedConnectionPool | None = None


def _get_dsn() -> str:
    return (
        f"host={os.environ.get('BDSP_DB_HOST', 'localhost')} "
        f"port={os.environ.get('BDSP_DB_PORT', '5433')} "
        f"dbname={os.environ.get('BDSP_DB_NAME', 'bdsp')} "
        f"user={os.environ.get('BDSP_DB_USER', 'bdsp')} "
        f"password={os.environ.get('BDSP_DB_PASSWORD', 'password')}"
    )


def get_pool() -> pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        _pool = pool.ThreadedConnectionPool(minconn=1, maxconn=10, dsn=_get_dsn())
    return _pool


@contextmanager
def get_conn() -> Generator[psycopg2.extensions.connection, None, None]:
    """Context manager: yields a connection, auto-commits or rolls back."""
    conn = get_pool().getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        get_pool().putconn(conn)


# ─── Upsert Helpers ───────────────────────────────────────────────────────────

def upsert_entsoe(records: list[dict]) -> int:
    """
    Insert ENTSO-E day-ahead price records.

    Expected keys: time (UTC-aware datetime), domain, price_eur_mwh, currency
    Returns: number of rows inserted (duplicates silently skipped).
    """
    sql = """
        INSERT INTO entsoe_day_ahead_prices (time, domain, price_eur_mwh, currency)
        VALUES (%(time)s, %(domain)s, %(price_eur_mwh)s, %(currency)s)
        ON CONFLICT (time, domain) DO NOTHING
    """
    return _bulk_execute(sql, records)


def upsert_weather(records: list[dict]) -> int:
    """
    Insert open-meteo weather records.

    Expected keys: time, latitude, longitude, temperature_2m, wind_speed_10m,
                   shortwave_radiation, cloud_cover
    """
    sql = """
        INSERT INTO weather_hourly
            (time, latitude, longitude, temperature_2m, wind_speed_10m,
             shortwave_radiation, cloud_cover)
        VALUES
            (%(time)s, %(latitude)s, %(longitude)s, %(temperature_2m)s,
             %(wind_speed_10m)s, %(shortwave_radiation)s, %(cloud_cover)s)
        ON CONFLICT (time, latitude, longitude) DO NOTHING
    """
    return _bulk_execute(sql, records)


def upsert_ekz(records: list[dict]) -> int:
    """
    Insert EKZ tariff records (15-min raw).

    Expected keys: time, tariff_type, price_chf_kwh
    """
    sql = """
        INSERT INTO ekz_tariffs_raw (time, tariff_type, price_chf_kwh)
        VALUES (%(time)s, %(tariff_type)s, %(price_chf_kwh)s)
        ON CONFLICT (time, tariff_type) DO NOTHING
    """
    return _bulk_execute(sql, records)


def upsert_bafu(records: list[dict]) -> int:
    """
    Insert BAFU hydro records.

    Expected keys: time, station_id, discharge_m3s, level_masl
    """
    sql = """
        INSERT INTO bafu_hydro (time, station_id, discharge_m3s, level_masl)
        VALUES (%(time)s, %(station_id)s, %(discharge_m3s)s, %(level_masl)s)
        ON CONFLICT (time, station_id) DO NOTHING
    """
    return _bulk_execute(sql, records)


# ─── Internal ─────────────────────────────────────────────────────────────────

def _bulk_execute(sql: str, records: list[dict]) -> int:
    if not records:
        return 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, records)
            return cur.rowcount
