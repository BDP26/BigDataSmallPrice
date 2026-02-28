"""
Minimal FastAPI backend for the BigDataSmallPrice admin dashboard.

Serves:
  GET /                              → admin_dash.html
  GET /api/db-status                 → live row counts + time ranges
  GET /api/feature-status            → training_features view stats
  GET /api/airflow/dags              → proxy to Airflow REST API
  GET /api/db-explorer/schema        → column names + types for all tables
  GET /api/db-explorer/rows/{table}  → paginated rows (newest first)
"""

import os
from decimal import Decimal
from pathlib import Path

import httpx
import psycopg2
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="BDSP Admin API")

_STATIC = Path(__file__).parent / "static"

_DB = {
    "host": os.getenv("BDSP_DB_HOST", "timescaledb"),
    "port": int(os.getenv("BDSP_DB_PORT", 5432)),
    "dbname": os.getenv("BDSP_DB_NAME", "bdsp"),
    "user": os.getenv("BDSP_DB_USER", "bdsp"),
    "password": os.environ["BDSP_DB_PASSWORD"],
}

_TABLES = {
    "entsoe":   "entsoe_day_ahead_prices",
    "weather":  "weather_hourly",
    "ekz":      "ekz_tariffs_raw",
    "bafu":     "bafu_hydro",
    "features": "training_features",
}

_AIRFLOW_URL  = os.getenv("AIRFLOW_API_URL",      "http://airflow-webserver:8080")
_AIRFLOW_USER = os.getenv("AIRFLOW_API_USER",      "admin")
_AIRFLOW_PASS = os.getenv("AIRFLOW_API_PASSWORD",  "")

# Whitelist – prevents SQL injection via table name parameter
_ALLOWED_TABLES = set(_TABLES.values())


def _serialize(v):
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def _connect():
    return psycopg2.connect(**_DB)


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/api/db-status")
def db_status():
    result = {}
    try:
        conn = _connect()
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


@app.get("/api/db-explorer/schema")
def db_schema():
    """Return column names, types and nullability for all 4 tables."""
    sql = """
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = ANY(%s)
        ORDER BY table_name, ordinal_position
    """
    result: dict = {}
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(sql, (list(_ALLOWED_TABLES),))
        for table_name, col_name, data_type, nullable in cur.fetchall():
            result.setdefault(table_name, []).append({
                "column": col_name,
                "type": data_type,
                "nullable": nullable == "YES",
            })
        cur.close()
        conn.close()
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    return result


@app.get("/api/db-explorer/rows/{table}")
def db_rows(
    table: str,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    """Return paginated rows for a whitelisted table, newest first."""
    if table not in _ALLOWED_TABLES:
        return JSONResponse({"error": f"Unknown table: {table!r}"}, status_code=400)
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(  # noqa: S608
            f"SELECT * FROM {table} ORDER BY time DESC LIMIT %s OFFSET %s",
            (limit, offset),
        )
        cols = [desc[0] for desc in cur.description]
        rows = [
            {cols[i]: _serialize(v) for i, v in enumerate(row)}
            for row in cur.fetchall()
        ]
        cur.close()
        conn.close()
        return {"columns": cols, "rows": rows, "offset": offset, "limit": limit}
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/feature-status")
def feature_status():
    sql = """
        SELECT
            COUNT(*)                                        AS row_count,
            MIN(time)                                       AS oldest,
            MAX(time)                                       AS newest,
            COUNT(*) FILTER (WHERE lag_24h IS NOT NULL)    AS rows_with_lags
        FROM training_features
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(sql)
        row_count, oldest, newest, rows_with_lags = cur.fetchone()
        cur.close()
        conn.close()
        return {
            "row_count":      int(row_count) if row_count else 0,
            "oldest":         oldest.isoformat() if oldest else None,
            "newest":         newest.isoformat() if newest else None,
            "rows_with_lags": int(rows_with_lags) if rows_with_lags else 0,
        }
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/airflow/dags")
def airflow_dags():
    auth = (_AIRFLOW_USER, _AIRFLOW_PASS)
    base = _AIRFLOW_URL.rstrip("/") + "/api/v1"
    try:
        with httpx.Client(timeout=5) as client:
            dags_resp = client.get(f"{base}/dags", auth=auth)
            dags_resp.raise_for_status()
            dags = dags_resp.json()["dags"]

            result = []
            for dag in dags:
                dag_id = dag["dag_id"]
                runs_resp = client.get(
                    f"{base}/dags/{dag_id}/dagRuns",
                    params={"order_by": "-execution_date", "limit": 1},
                    auth=auth,
                )
                last_run = None
                if runs_resp.is_success:
                    items = runs_resp.json().get("dag_runs", [])
                    if items:
                        r = items[0]
                        last_run = {
                            "state":          r.get("state"),
                            "execution_date": r.get("execution_date"),
                            "start_date":     r.get("start_date"),
                            "end_date":       r.get("end_date"),
                        }
                schedule = dag.get("schedule_interval") or {}
                result.append({
                    "dag_id":    dag_id,
                    "is_paused": dag.get("is_paused", False),
                    "schedule":  schedule.get("value") if isinstance(schedule, dict) else str(schedule),
                    "next_run":  dag.get("next_dagrun"),
                    "last_run":  last_run,
                })
        return result
    except httpx.ConnectError:
        return JSONResponse({"error": "Airflow not reachable"}, status_code=503)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/", response_class=HTMLResponse)
def index():
    return (_STATIC / "admin_dash.html").read_text()
