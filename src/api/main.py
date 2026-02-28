"""
Minimal FastAPI backend for the BigDataSmallPrice admin dashboard.

Serves:
  GET /                → admin_dash.html
  GET /api/db-status   → live row counts + time ranges from TimescaleDB
"""

import os
from pathlib import Path

import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="BDSP Admin API")

_STATIC = Path(__file__).parent / "static"

_DB = {
    "host": os.getenv("BDSP_DB_HOST", "timescaledb"),
    "port": int(os.getenv("BDSP_DB_PORT", 5432)),
    "dbname": os.getenv("BDSP_DB_NAME", "bdsp"),
    "user": os.getenv("BDSP_DB_USER", "bdsp"),
    "password": os.getenv("BDSP_DB_PASSWORD", "password"),
}

_TABLES = {
    "entsoe": "entsoe_day_ahead_prices",
    "weather": "weather_hourly",
    "ekz": "ekz_tariffs_raw",
    "bafu": "bafu_hydro",
}


@app.get("/api/db-status")
def db_status():
    result = {}
    try:
        conn = psycopg2.connect(**_DB)
        cur = conn.cursor()
        for key, table in _TABLES.items():
            cur.execute(f"SELECT count(*), min(time), max(time) FROM {table}")  # noqa: S608
            count, oldest, newest = cur.fetchone()
            result[key] = {
                "count": count,
                "oldest": oldest.isoformat() if oldest else None,
                "newest": newest.isoformat() if newest else None,
            }
        cur.close()
        conn.close()
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    return result


@app.get("/", response_class=HTMLResponse)
def index():
    return (_STATIC / "admin_dash.html").read_text()
