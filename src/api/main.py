"""
FastAPI backend for the BigDataSmallPrice platform.

Serves:
  GET  /                              → admin dashboard (admin_dash.html)
  GET  /dashboard                     → user dashboard  (user_dash.html)
  GET  /api/db-status                 → live row counts + time ranges
  GET  /api/feature-status            → training_features view stats
  GET  /api/airflow/dags              → proxy to Airflow REST API
  GET  /api/db-explorer/schema        → column names + types for all tables
  GET  /api/db-explorer/rows/{table}  → paginated rows (newest first)
  GET  /api/forecast                  → current price prediction (public)
  GET  /api/price-history             → last 24 h ENTSO-E prices (public)
  POST /auth/register                 → create a user account
  POST /auth/login                    → exchange credentials for JWT
  POST /api/predict                   → model inference (JWT required)
  POST /api/backfill/trigger          → trigger bdsp_backfill Airflow DAG
  GET  /api/backfill/status/{run_id}  → poll a specific backfill dag_run by run-id
"""

from __future__ import annotations

import hashlib
import os
import time
from decimal import Decimal
from pathlib import Path

import httpx
import jwt
import pandas as pd
import psycopg2
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

app = FastAPI(title="BDSP API", docs_url="/docs")

# In Docker the static directory is volume-mounted; locally it lives at src/frontend/static
_STATIC = Path(os.getenv("BDSP_STATIC_DIR", str(Path(__file__).parent.parent / "frontend" / "static")))

_DB = {
    "host": os.getenv("BDSP_DB_HOST", "timescaledb"),
    "port": int(os.getenv("BDSP_DB_PORT", 5432)),
    "dbname": os.getenv("BDSP_DB_NAME", "bdsp"),
    "user": os.getenv("BDSP_DB_USER", "bdsp"),
    "password": os.getenv("BDSP_DB_PASSWORD", ""),
}

_TABLES = {
    "entsoe":          "entsoe_day_ahead_prices",
    "entsoe_load":     "entsoe_actual_load",
    "entsoe_gen":      "entsoe_generation",
    "entsoe_flows":    "entsoe_crossborder_flows",
    "entsoe_forecast": "entsoe_load_forecast",
    "weather":         "weather_hourly",
    "ckw":             "ckw_tariffs_raw",
    "groupe_e":        "groupe_e_tariffs_raw",
    "ekz":             "ekz_tariffs_raw",
    "bafu":            "bafu_hydro",
    "winterthur_load": "winterthur_load",
    "winterthur_pv":   "winterthur_pv",
    "features":        "training_features",
}

_AIRFLOW_URL  = os.getenv("AIRFLOW_API_URL",     "http://airflow-webserver:8080")
_AIRFLOW_USER = os.getenv("AIRFLOW_API_USER",     "admin")
_AIRFLOW_PASS = os.getenv("AIRFLOW_API_PASSWORD", "")

# Whitelist – prevents SQL injection via table name parameter
_ALLOWED_TABLES = set(_TABLES.values())

# ── JWT config ────────────────────────────────────────────────────────────────

_JWT_SECRET    = os.getenv("BDSP_JWT_SECRET", "change-me-in-production")
_JWT_ALGORITHM = "HS256"
_JWT_EXPIRE_H  = 24

# ── In-memory stores (module-level for easy test access) ──────────────────────

_USERS: dict[str, str] = {}   # username → SHA-256 hashed password
_model_cache: dict = {}        # prefix → loaded model object

_bearer = HTTPBearer()

# ── Known API rate limits ─────────────────────────────────────────────────────

_KNOWN_LIMITS: dict[str, dict] = {
    "entsoe":    {"daily": 400,   "source": "documented"},
    "openmeteo": {"daily": 10000, "source": "documented"},
    "ekz":       {"daily": None,  "source": "unknown"},
    "ckw":       {"daily": None,  "source": "unknown"},
    "groupe_e":  {"daily": None,  "source": "unknown"},
    "bafu":      {"daily": None,  "source": "unknown"},
}

# ── Pydantic schemas ──────────────────────────────────────────────────────────


class UserIn(BaseModel):
    username: str
    password: str


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class PredictRequest(BaseModel):
    features: dict[str, float]


class PredictResponse(BaseModel):
    prediction_eur_mwh: float
    model: str


# ── Internal helpers ──────────────────────────────────────────────────────────


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


def _ensure_api_call_log_table(conn) -> None:
    """
    Ensure api_call_log exists so rate-limit endpoints don't fail on fresh DBs.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS api_call_log (
                id BIGSERIAL,
                called_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                source TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                was_rate_limited BOOLEAN NOT NULL DEFAULT FALSE,
                response_ms INTEGER,
                date_fetched DATE,
                CONSTRAINT api_call_log_pkey PRIMARY KEY (id, called_at)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS api_call_log_source_idx
            ON api_call_log (source, called_at DESC)
            """
        )
    conn.commit()


def _hash_pw(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def _make_token(username: str) -> str:
    payload = {"sub": username, "exp": time.time() + _JWT_EXPIRE_H * 3600}
    return jwt.encode(payload, _JWT_SECRET, algorithm=_JWT_ALGORITHM)


def _current_user(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
) -> str:
    """FastAPI dependency that validates a Bearer JWT and returns the username."""
    try:
        payload = jwt.decode(
            creds.credentials, _JWT_SECRET, algorithms=[_JWT_ALGORITHM]
        )
        return payload["sub"]
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired"
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )


def _get_model(prefix: str = "xgb"):
    """Load model from disk on first call; return cached instance afterwards."""
    if prefix not in _model_cache:
        from modelling.predict import find_latest_model, load_model  # noqa: PLC0415
        models_dir = os.getenv("BDSP_MODELS_DIR", "models/")
        path = find_latest_model(models_dir, prefix=prefix)
        _model_cache[prefix] = load_model(path)
    return _model_cache[prefix]


# ── Auth endpoints ────────────────────────────────────────────────────────────


@app.post("/auth/register", status_code=status.HTTP_201_CREATED)
def register(user: UserIn):
    """Register a new user account."""
    if user.username in _USERS:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="Username already exists"
        )
    _USERS[user.username] = _hash_pw(user.password)
    return {"message": "User created"}


@app.post("/auth/login", response_model=Token)
def login(user: UserIn):
    """Authenticate and return a JWT access token."""
    pw_hash = _USERS.get(user.username)
    if pw_hash is None or pw_hash != _hash_pw(user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials"
        )
    return {"access_token": _make_token(user.username)}


# ── Model inference ───────────────────────────────────────────────────────────


@app.post("/api/predict", response_model=PredictResponse)
def predict_price(
    request: PredictRequest,
    _username: str = Depends(_current_user),
):
    """
    Run the XGBoost model on an explicit feature dict (JWT required).

    Body: ``{"features": {"lag_1h": 75.0, "temperature_2m": 8.5, ...}}``
    """
    from modelling.predict import predict_from_dict  # noqa: PLC0415
    try:
        model = _get_model("xgb")
        price = predict_from_dict(model, request.features)
    except FileNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="No trained model available. Run the training pipeline first.",
        )
    return {"prediction_eur_mwh": price, "model": "xgb"}


# ── Public forecast endpoints ─────────────────────────────────────────────────


@app.get("/api/forecast")
def forecast():
    """
    Return price prediction with full tariff breakdown in Rp./kWh.

    Combines:
    - Model B (XGBoost EPEX): predicts day-ahead price in EUR/MWh
    - Model A (load model): predicts net grid load in kWh
    - Tariff formulas: converts both outputs to Rp./kWh tariff components

    Falls back to energy-price-only estimate if Model A is not yet available.
    """
    from modelling.predict import predict_from_dict  # noqa: PLC0415
    from processing.export_pipeline import FEATURE_COLS  # noqa: PLC0415
    from processing.tariff_formulas import compute_tariff  # noqa: PLC0415
    try:
        conn = _connect()
        df_epex = pd.read_sql(
            "SELECT * FROM training_features ORDER BY time DESC LIMIT 1",
            conn,
            parse_dates=["time"],
        )
        # Try to fetch latest load feature row as well
        try:
            df_load = pd.read_sql(
                "SELECT * FROM winterthur_net_load_features ORDER BY time DESC LIMIT 1",
                conn,
                parse_dates=["time"],
            )
        except Exception:
            df_load = pd.DataFrame()
        conn.close()

        if df_epex.empty:
            return JSONResponse({"error": "No feature data available"}, status_code=503)

        row = df_epex.iloc[0]
        feature_dict = {
            col: float(row[col]) for col in FEATURE_COLS if col in df_epex.columns
        }

        # Model B: EPEX price prediction
        model_epex = _get_model("xgb")
        epex_price = predict_from_dict(model_epex, feature_dict)

        # Model A: net load prediction (optional)
        net_load: float | None = None
        if not df_load.empty:
            try:
                load_model = _get_model("model_load")
                from processing.export_pipeline import LOAD_FEATURE_COLS  # noqa: PLC0415
                load_row = df_load.iloc[0]
                load_features = {
                    col: float(load_row[col])
                    for col in LOAD_FEATURE_COLS
                    if col in df_load.columns
                }
                net_load = predict_from_dict(load_model, load_features)
            except (FileNotFoundError, Exception):
                net_load = None

        # Compute tariff breakdown
        if net_load is not None:
            tariff = compute_tariff(net_load=net_load, epex_eur_mwh=epex_price)
        else:
            # Fallback: energy-only estimate (no network component)
            from processing.tariff_formulas import energiepreis, gesamttarif  # noqa: PLC0415
            from processing.tariff_formulas import DEFAULT_NETZ_STANDARD  # noqa: PLC0415
            energie = energiepreis(epex_price)
            tariff = {
                "netzpreis_rp_kwh":    round(DEFAULT_NETZ_STANDARD, 2),
                "energiepreis_rp_kwh": round(energie, 2),
                "gesamttarif_rp_kwh":  round(gesamttarif(DEFAULT_NETZ_STANDARD, energie), 2),
            }

        gesamt = tariff["gesamttarif_rp_kwh"]
        # Traffic-light thresholds on gesamttarif (Rp./kWh)
        level = "low" if gesamt < 15 else ("high" if gesamt > 22 else "medium")

        return {
            "time":                   row["time"].isoformat(),
            "predicted_price_eur_mwh": round(epex_price, 2),
            "netzpreis_rp_kwh":       tariff["netzpreis_rp_kwh"],
            "energiepreis_rp_kwh":    tariff["energiepreis_rp_kwh"],
            "gesamttarif_rp_kwh":     gesamt,
            "price_rp_kwh":           gesamt,   # backward-compat alias
            "price_level":            level,
            "net_load_available":     net_load is not None,
        }
    except FileNotFoundError:
        return JSONResponse({"error": "No trained model available"}, status_code=503)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/price-history")
def price_history(hours: int = Query(default=24, ge=1, le=168)):
    """Return the last *hours* of actual ENTSO-E day-ahead prices."""
    try:
        conn = _connect()
        df = pd.read_sql(
            "SELECT time, price_eur_mwh FROM entsoe_day_ahead_prices "
            "ORDER BY time DESC LIMIT %s",
            conn,
            params=(hours,),
            parse_dates=["time"],
        )
        conn.close()
        df = df.sort_values("time")
        return {
            "times": [t.isoformat() for t in df["time"]],
            "prices": [float(p) for p in df["price_eur_mwh"]],
        }
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


# ── Admin DB / Airflow endpoints (unchanged from Phase 1) ─────────────────────


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
    """Return column names, types and nullability for all tables."""
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


@app.get("/api/timeseries/{table}")
def timeseries(
    table: str,
    horizon: str = Query(default="1w"),
):
    """
    Return Plotly-ready time series traces for a whitelisted table.

    horizon: 1d | 1w | 1m | 1y | all
    """
    if table not in _ALLOWED_TABLES:
        return JSONResponse({"error": f"Unknown table: {table!r}"}, status_code=400)

    horizon_map = {
        "1d": "1 day", "1w": "7 days", "1m": "30 days", "1y": "365 days",
    }
    where_extra = (
        f"AND time >= NOW() - INTERVAL '{horizon_map[horizon]}'"
        if horizon in horizon_map else ""
    )

    # Categorical grouping columns: pivot these into separate traces
    _GROUP_COLS: dict[str, list[str]] = {
        "entsoe_generation":        ["domain", "psr_type"],
        "entsoe_crossborder_flows": ["in_domain", "out_domain"],
        "ckw_tariffs_raw":          ["tariff_type"],
        "groupe_e_tariffs_raw":     ["tariff_type"],
        "ekz_tariffs_raw":          ["tariff_type"],
        "bafu_hydro":               ["station_id"],
        "entsoe_day_ahead_prices":  [],
        "entsoe_actual_load":       [],
        "entsoe_load_forecast":     [],
        "weather_hourly":           [],
        "winterthur_load":          [],
        "winterthur_pv":            [],
        "training_features":        [],
    }

    # Columns to skip even if numeric (coordinates, IDs)
    _SKIP_COLS = {"latitude", "longitude", "id"}

    try:
        conn = _connect()
        cur = conn.cursor()

        # Fetch column metadata
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        schema = cur.fetchall()

        numeric_types = {"double precision", "numeric", "integer", "bigint", "real", "smallint"}
        group_cols = _GROUP_COLS.get(table, [])
        num_cols = [
            c for c, t in schema
            if t in numeric_types and c not in _SKIP_COLS
        ]

        if not num_cols:
            cur.close()
            conn.close()
            return {"traces": []}

        select_cols = ["time"] + group_cols + num_cols
        # Use parameterized query for horizon but safe string interpolation for
        # column/table names (already validated via whitelist)
        sql = (
            f"SELECT {', '.join(select_cols)} "  # noqa: S608
            f"FROM {table} "
            f"WHERE TRUE {where_extra} "
            f"ORDER BY time ASC "
            f"LIMIT 10000"
        )
        cur.execute(sql)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()

        if not rows:
            return {"traces": []}

        col_idx = {c: i for i, c in enumerate(col_names)}

        if group_cols:
            # Pivot: one trace per unique combination of group columns
            from collections import defaultdict
            buckets: dict[str, dict] = defaultdict(lambda: {"x": [], "y_map": {}})

            for row in rows:
                t = row[col_idx["time"]].isoformat()
                cat_key = " | ".join(str(row[col_idx[g]]) for g in group_cols)
                bucket = buckets[cat_key]
                bucket["x"].append(t)
                for nc in num_cols:
                    v = row[col_idx[nc]]
                    bucket["y_map"].setdefault(nc, []).append(
                        float(v) if v is not None else None
                    )

            traces = []
            for cat_key, bucket in buckets.items():
                for nc, ys in bucket["y_map"].items():
                    name = f"{cat_key}" if len(num_cols) == 1 else f"{cat_key} · {nc}"
                    traces.append({"name": name, "x": bucket["x"], "y": ys})
        else:
            # Simple: one trace per numeric column
            times = [row[col_idx["time"]].isoformat() for row in rows]
            traces = [
                {
                    "name": nc,
                    "x": times,
                    "y": [
                        float(row[col_idx[nc]]) if row[col_idx[nc]] is not None else None
                        for row in rows
                    ],
                }
                for nc in num_cols
            ]

        return {"traces": traces}

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


# ── Rate Limit endpoints ──────────────────────────────────────────────────────


@app.get("/api/rate-limits")
def rate_limits():
    """Current API usage summary: last 24h + last 7d per source."""
    sql_24h = """
        SELECT source,
               COUNT(*)                                      AS calls_24h,
               COUNT(*) FILTER (WHERE was_rate_limited)      AS rl_24h,
               MAX(called_at) FILTER (WHERE was_rate_limited) AS last_429
        FROM api_call_log
        WHERE called_at >= NOW() - INTERVAL '24 hours'
        GROUP BY source
    """
    sql_7d = """
        SELECT source, COUNT(*) AS calls_7d
        FROM api_call_log
        WHERE called_at >= NOW() - INTERVAL '7 days'
        GROUP BY source
    """
    try:
        conn = _connect()
        _ensure_api_call_log_table(conn)
        cur = conn.cursor()
        cur.execute(sql_24h)
        rows_24h = {r[0]: {"calls_last_24h": r[1], "rate_limited_last_24h": r[2],
                            "last_429": r[3].isoformat() if r[3] else None}
                    for r in cur.fetchall()}
        cur.execute(sql_7d)
        rows_7d = {r[0]: r[1] for r in cur.fetchall()}
        cur.close()
        conn.close()
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)

    sources = {}
    for src, limits in _KNOWN_LIMITS.items():
        d24 = rows_24h.get(src, {"calls_last_24h": 0, "rate_limited_last_24h": 0, "last_429": None})
        calls_24h = d24["calls_last_24h"]
        daily_limit = limits["daily"]
        usage_pct = round(calls_24h / daily_limit, 4) if daily_limit else None
        sources[src] = {
            "calls_last_24h":       calls_24h,
            "calls_last_7d":        rows_7d.get(src, 0),
            "rate_limited_last_24h": d24["rate_limited_last_24h"],
            "last_429":             d24["last_429"],
            "known_daily_limit":    daily_limit,
            "limit_source":         limits["source"],
            "usage_pct":            usage_pct,
        }

    return {
        "sources":      sources,
        "generated_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
    }


@app.get("/api/rate-limits/history")
def rate_limits_history():
    """Hourly call counts per source for the last 7 days (for chart rendering)."""
    sql = """
        SELECT source,
               time_bucket('1 hour', called_at) AS hour,
               COUNT(*) AS calls
        FROM api_call_log
        WHERE called_at >= NOW() - INTERVAL '7 days'
        GROUP BY source, hour
        ORDER BY source, hour
    """
    try:
        conn = _connect()
        _ensure_api_call_log_table(conn)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)

    result: dict = {}
    for src, hour, calls in rows:
        result.setdefault(src, []).append({"hour": hour.isoformat(), "calls": calls})
    return result


# ── Backfill endpoints ────────────────────────────────────────────────────────


class BackfillRequest(BaseModel):
    start_date: str  # "YYYY-MM-DD"
    end_date: str    # "YYYY-MM-DD"


@app.post("/api/backfill/estimate")
def backfill_estimate(req: BackfillRequest):
    """
    Estimate the work required to backfill the given date range.

    Checks the DB for already-loaded data per source and returns
    estimated API call counts and duration.
    """
    import datetime as _dt

    try:
        start = _dt.date.fromisoformat(req.start_date)
        end = _dt.date.fromisoformat(req.end_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {exc}") from exc
    if end < start:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")

    total_days = (end - start).days + 1

    # Table → (source_key, calls_per_day)
    _SOURCE_TABLES = {
        "entsoe":    ("entsoe_day_ahead_prices", 1),
        "openmeteo": ("weather_hourly",          1),
        "ekz":       ("ekz_tariffs_raw",         2),
        "ckw":       ("ckw_tariffs_raw",         1),
        "groupe_e":  ("groupe_e_tariffs_raw",    1),
        "bafu":      ("bafu_hydro",              1),
    }

    sources = {}
    total_calls = 0
    try:
        conn = _connect()
        cur = conn.cursor()
        for src, (table, cpd) in _SOURCE_TABLES.items():
            cur.execute(
                f"SELECT MIN(time)::date, MAX(time)::date FROM {table}"  # noqa: S608
            )
            db_min, db_max = cur.fetchone()
            # Count days not yet in DB (simple heuristic: days outside DB range)
            if db_min is None or db_max is None:
                to_fetch = total_days
            else:
                already = sum(
                    1 for i in range(total_days)
                    if db_min <= (start + _dt.timedelta(days=i)) <= db_max
                )
                to_fetch = total_days - already
            calls = to_fetch * cpd
            total_calls += calls
            sources[src] = {
                "calls":        calls,
                "already_in_db": total_days - to_fetch,
                "to_fetch":     to_fetch,
            }
        cur.close()
        conn.close()
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)

    estimated_minutes = round(total_calls * 1.5 / 60, 1)
    return {
        "total_days":               total_days,
        "estimated_api_calls":      total_calls,
        "estimated_duration_minutes": estimated_minutes,
        "sources":                  sources,
    }


@app.post("/api/backfill/trigger")
def backfill_trigger(req: BackfillRequest):
    """Trigger the bdsp_backfill Airflow DAG with the given date range."""
    import datetime as _dt

    try:
        start = _dt.date.fromisoformat(req.start_date)
        end = _dt.date.fromisoformat(req.end_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {exc}") from exc
    if end < start:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")
    today = _dt.date.today()
    if start > today or end > today:
        raise HTTPException(status_code=400, detail="Dates must not be in the future")

    url = f"{_AIRFLOW_URL.rstrip('/')}/api/v1/dags/bdsp_backfill/dagRuns"
    payload = {
        "conf": {
            "backfill_start": req.start_date,
            "backfill_end":   req.end_date,
        }
    }
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(url, json=payload, auth=(_AIRFLOW_USER, _AIRFLOW_PASS))
            if resp.status_code not in (200, 201):
                return JSONResponse(
                    {"error": f"Airflow returned {resp.status_code}: {resp.text}"},
                    status_code=502,
                )
            data = resp.json()
            return {
                "dag_run_id": data.get("dag_run_id"),
                "state":      data.get("state", "queued"),
                "message":    "Backfill triggered.",
            }
    except httpx.ConnectError:
        return JSONResponse({"error": "Airflow not reachable"}, status_code=503)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/backfill/status/{dag_run_id}")
def backfill_status(dag_run_id: str):
    """
    Poll the state of a specific bdsp_backfill DAG run.

    Returns the run's state (queued / running / success / failed) so the
    admin dashboard can track the *current* run rather than the last recorded
    one (which may be a previous failed run and would mislead the UI).
    """
    url = (
        f"{_AIRFLOW_URL.rstrip('/')}/api/v1"
        f"/dags/bdsp_backfill/dagRuns/{dag_run_id}"
    )
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url, auth=(_AIRFLOW_USER, _AIRFLOW_PASS))
            if resp.status_code == 404:
                return JSONResponse(
                    {"error": f"DAG run '{dag_run_id}' not found"}, status_code=404
                )
            resp.raise_for_status()
            data = resp.json()
            return {
                "dag_run_id": data.get("dag_run_id"),
                "state":      data.get("state"),
                "start_date": data.get("start_date"),
                "end_date":   data.get("end_date"),
            }
    except httpx.ConnectError:
        return JSONResponse({"error": "Airflow not reachable"}, status_code=503)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


# ── HTML pages ────────────────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
def index():
    return (_STATIC / "admin_dash.html").read_text()


@app.get("/dashboard", response_class=HTMLResponse)
def user_dashboard():
    return (_STATIC / "user_dash.html").read_text()
