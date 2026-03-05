# Coding Agent Prompt — BDSP: Backfill, Rate Limiting & Admin Dashboard Extension

## Project Overview

You are working on **BigDataSmallPrice (BDSP)**, a Python-based Big Data pipeline that forecasts
dynamic electricity prices for Winterthur, Switzerland. The project is a university course project
(DS23t PM4).

**Tech stack:**
- Python 3.11+, FastAPI, Apache Airflow (>=2.9), TimescaleDB (PostgreSQL extension), XGBoost
- Frontend: plain HTML + Tailwind CSS served statically via FastAPI
- Containerisation: Docker Compose

**Repository root:** `BigDataSmallPrice/`

```
src/
  api/main.py                        ← FastAPI app (all endpoints)
  data_collection/
    base_collector.py                ← Abstract BaseCollector with _fetch_with_retry()
    entsoe_collector.py              ← ENTSO-E Day-Ahead prices (1 day window, datetime.now())
    openmeteo_collector.py           ← Open-Meteo forecast (forecast_days=2, datetime.now())
    ekz_collector.py                 ← EKZ tariffs (date=today)
    ckw_collector.py                 ← CKW tariffs (date=today)
    groupe_e_collector.py            ← Groupe E tariffs (date=today)
    bafu_collector.py                ← BAFU hydro Rhein-Rekingen (days_back=2)
    stadtwerk_winterthur_collector.py← OGD CSVs for Winterthur load + PV
  db/timescale_client.py             ← Connection pool + upsert helpers
  processing/export_pipeline.py      ← Feature export (FEATURE_COLS, LOAD_FEATURE_COLS)
  frontend/static/admin_dash.html    ← Admin dashboard (Tailwind, fetches /api/* endpoints)
airflow/dags/
  etl_pipeline_dag.py                ← bdsp_etl_daily, schedule 06:00 UTC, catchup=False
  feature_pipeline_dag.py            ← bdsp_feature_daily, 07:00 UTC, catchup=False
  training_dag.py                    ← bdsp_training_daily, 08:00 UTC, catchup=False
infra/db/
  init.sql                           ← TimescaleDB schema (hypertables, continuous aggregates)
  features.sql                       ← Phase 2/3 migration (feature views)
```

---

## Known API Rate Limits

| Source        | Known limit                     | Notes                                    |
|---------------|---------------------------------|------------------------------------------|
| ENTSO-E       | ~400 requests/day (free token)  | Documented                               |
| Open-Meteo    | 10,000 requests/day             | Documented, free tier                    |
| Open-Meteo Archive | 10,000 requests/day        | Separate endpoint (see Task 1)           |
| EKZ           | Unknown                         | Track 429s to infer                      |
| CKW           | Unknown                         | Track 429s to infer                      |
| Groupe E      | Unknown                         | Track 429s to infer                      |
| BAFU/existenz.ch | Unknown                      | Track 429s to infer                      |

---

## Your Tasks

Implement the following **6 tasks** in order. Run `pytest src/testing/` after each task to ensure
no regressions. The full test suite must be green before you consider the work done.

---

### Task 1 — Open-Meteo: Auto-switch Forecast ↔ Archive Endpoint

**Problem:** `OpenMeteoCollector` currently always uses `api.open-meteo.com/v1/forecast`, which
only covers the last few days and near-future. Historical backfills require
`archive-api.open-meteo.com/v1/archive`, which uses a `start_date`/`end_date` range parameter
instead of `forecast_days`.

**Implementation:**

1. Refactor `OpenMeteoCollector.__init__` to accept an optional `date: str | None = None`
   parameter (`"YYYY-MM-DD"` format, like the other collectors).

2. Add a module-level helper `_is_historical(date_str: str) -> bool` that returns `True` if the
   given date is **more than 5 days in the past** relative to today UTC. The 5-day buffer accounts
   for Open-Meteo's rolling window.

3. In `fetch()`, branch on `_is_historical(self.date)`:
   - **Historical:** `GET https://archive-api.open-meteo.com/v1/archive` with params
     `latitude`, `longitude`, `start_date=self.date`, `end_date=self.date`,
     `hourly=temperature_2m,wind_speed_10m,shortwave_radiation,cloud_cover,precipitation`,
     `timezone=UTC`
   - **Forecast (default):** keep current behaviour (`/v1/forecast` with `forecast_days=2`).
     When `self.date` is None, use forecast mode.

4. The `parse()` method response structure is identical for both endpoints — no changes needed
   to parsing logic.

5. **Tests:** Add `src/testing/unittests/test_openmeteo_archive.py` with:
   - `test_historical_flag_true()` — date 10 days ago → `_is_historical` returns True
   - `test_historical_flag_false()` — date tomorrow → returns False
   - `test_historical_flag_boundary()` — date exactly 5 days ago → returns True
   - `test_forecast_collector_uses_forecast_url()` — mock `_fetch_with_retry`, assert URL
     contains `open-meteo.com/v1/forecast` when no date given
   - `test_archive_collector_uses_archive_url()` — mock `_fetch_with_retry`, assert URL
     contains `archive-api.open-meteo.com/v1/archive` when date is 30 days ago
   - `test_parse_archive_response()` — assert correct record structure from a fixture response

---

### Task 2 — API Rate Limit Tracking (DB-persisted, isolated from ML data)

**Problem:** There is no tracking of how many API calls have been made per source. We need
persistent counters (survive container restarts) that are **strictly isolated from the ML
training data** (must never appear in `training_features`, `winterthur_net_load_features`, or
any parquet export).

**2a — New DB table (add to `infra/db/init.sql` and `infra/db/features.sql`):**

```sql
CREATE TABLE IF NOT EXISTS api_call_log (
    id              BIGSERIAL,
    source          TEXT        NOT NULL,  -- 'entsoe'|'openmeteo'|'ekz'|'ckw'|'groupe_e'|'bafu'
    called_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status_code     INT         NOT NULL,  -- actual HTTP status returned
    was_rate_limited BOOLEAN    NOT NULL DEFAULT FALSE,  -- TRUE if status_code == 429
    response_ms     INT,                   -- response time in milliseconds
    date_fetched    TEXT,                  -- the data-date requested ("YYYY-MM-DD" or NULL)
    CONSTRAINT api_call_log_pkey PRIMARY KEY (id, called_at)
);

SELECT create_hypertable(
    'api_call_log', 'called_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS api_call_log_source_idx ON api_call_log (source, called_at DESC);
```

**2b — Tracking in `BaseCollector`:**

Add a `_source_name: str` class attribute to each concrete collector (e.g. `_source_name =
"entsoe"` in `EntsoeCollector`). Add it to all 7 collectors.

Modify `_fetch_with_retry()` in `BaseCollector` to:
- Record wall-clock time before/after the HTTP call
- After each response (success OR 429/5xx), call a new module-level function
  `_log_api_call(source, status_code, response_ms, date_fetched)` that inserts a row into
  `api_call_log` via a direct `psycopg2` connection. Do **not** use the connection pool
  from `timescale_client.py` (to avoid circular imports) — use a standalone connection
  built from the same env vars.
- If the DB is unreachable, log a warning and continue — never let tracking break the
  actual data fetch.
- Add the `date_fetched` parameter to `_fetch_with_retry` signature (optional, default None),
  and thread it through from each collector's `fetch()` method.

**2c — New FastAPI endpoints in `src/api/main.py`:**

```
GET /api/rate-limits          → current usage summary (last 24h + last 7d per source)
GET /api/rate-limits/history  → hourly call counts for the last 7 days (for chart)
```

`/api/rate-limits` response schema:
```json
{
  "sources": {
    "entsoe": {
      "calls_last_24h": 3,
      "calls_last_7d": 18,
      "rate_limited_last_24h": 0,
      "known_daily_limit": 400,
      "limit_source": "documented",
      "usage_pct": 0.75
    },
    "openmeteo": { ... },
    "ekz":       { "known_daily_limit": null, "limit_source": "unknown", ... },
    ...
  },
  "generated_at": "2026-03-05T10:00:00Z"
}
```

Known limits are hardcoded constants in `main.py`:
```python
_KNOWN_LIMITS = {
    "entsoe":   {"daily": 400,   "source": "documented"},
    "openmeteo": {"daily": 10000, "source": "documented"},
    "ekz":      {"daily": None,  "source": "unknown"},
    "ckw":      {"daily": None,  "source": "unknown"},
    "groupe_e": {"daily": None,  "source": "unknown"},
    "bafu":     {"daily": None,  "source": "unknown"},
}
```

**Isolation guarantee:** Add a comment block in `export_pipeline.py` and `infra/db/init.sql`
explicitly documenting that `api_call_log` must never be joined into `training_features` or
`winterthur_net_load_features`. Also assert in the existing `validate_no_leakage()` function that
none of the `api_call_log` columns appear in `FEATURE_COLS` or `LOAD_FEATURE_COLS`.

---

### Task 3 — Airflow: Timezone, Catchup, Execution-Date-Aware Collectors

**3a — Timezone to Europe/Zurich:**

In **all three DAGs** (`etl_pipeline_dag.py`, `feature_pipeline_dag.py`, `training_dag.py`):

1. Replace `from datetime import datetime, timezone` imports with the addition of
   `from pendulum import timezone as pendulum_tz` (Airflow ships with pendulum).
2. Change `start_date=datetime(2026, 1, 1, tzinfo=timezone.utc)` to
   `start_date=datetime(2026, 1, 1, tzinfo=pendulum_tz("Europe/Zurich"))`.
3. Add `timezone="Europe/Zurich"` as a DAG-level parameter in each DAG definition.
4. Update schedule strings to reflect local time intent:
   - ETL DAG: `"0 6 * * *"` → keep same value but now interpreted as 06:00 Europe/Zurich
   - Feature DAG: `"0 7 * * *"` → same
   - Training DAG: `"0 8 * * *"` → same

**3b — Enable catchup for the ETL DAG only:**

- In `etl_pipeline_dag.py`: change `catchup=False` → `catchup=True`.
  Set `max_active_runs=1` on the DAG to prevent parallel backfill overloading APIs.
- Leave `catchup=False` on `feature_pipeline_dag.py` and `training_dag.py` — backfilling
  feature exports and model retraining for historical runs is not needed.

**3c — Make all ETL collectors execution-date-aware:**

Each task function in `etl_pipeline_dag.py` receives an Airflow context dict via `**ctx`.
The `logical_date` (formerly `execution_date`) is available as `ctx["logical_date"]`.

Update all 8 task functions in `etl_pipeline_dag.py` to extract the execution date and pass
it to their respective collectors. Use this pattern:

```python
def _fetch_entsoe(**ctx) -> None:
    from data_collection.entsoe_collector import EntsoeCollector
    from db.timescale_client import upsert_entsoe

    exec_date = ctx["logical_date"]          # pendulum.DateTime, UTC-aware
    date_str = exec_date.strftime("%Y-%m-%d")

    # ENTSO-E: fetch the full day for exec_date
    from datetime import timedelta
    period_start = exec_date.replace(hour=0, minute=0, second=0, microsecond=0)
    period_end = period_start + timedelta(days=1)

    collector = EntsoeCollector(period_start=period_start, period_end=period_end)
    records = collector.run()
    inserted = upsert_entsoe(records)
    print(f"ENTSO-E: {len(records)} fetched, {inserted} inserted for {date_str}.")
```

Apply analogously to all other collectors. For collectors that take a `date: str` parameter
(EKZ, CKW, GroupeE, BAFU), pass `date_str`. For OpenMeteoCollector, pass `date=date_str`
so it auto-selects archive vs. forecast endpoint (Task 1). For
`BruttolastgangCollector` and `NetzEinspeisungCollector` (Stadtwerk Winterthur), leave
them unchanged — they use CSV downloads, not date-parameterised APIs.

**3d — Airflow Task Pools for rate limiting:**

Add two Airflow Pools in a new file `airflow/pools.py` (documentation only — pools must be
created in the Airflow UI or via CLI on first setup):

```python
# Airflow Pools — create via `airflow pools set <name> <slots> <description>`
# or import via the Airflow UI (Admin > Pools > Import)
POOLS = {
    "entsoe_pool":   {"slots": 1, "description": "Max 1 concurrent ENTSO-E request"},
    "tariff_pool":   {"slots": 2, "description": "Max 2 concurrent tariff API requests (EKZ/CKW/GroupeE)"},
    "meteo_pool":    {"slots": 3, "description": "Max 3 concurrent Open-Meteo requests"},
    "bafu_pool":     {"slots": 1, "description": "Max 1 concurrent BAFU request"},
}
```

In `etl_pipeline_dag.py`, assign pools to each `PythonOperator`:
- `fetch_entsoe` → `pool="entsoe_pool"`
- `fetch_ekz`, `fetch_ckw`, `fetch_groupe_e` → `pool="tariff_pool"`
- `fetch_weather` → `pool="meteo_pool"`
- `fetch_bafu` → `pool="bafu_pool"`
- `fetch_winterthur_load`, `fetch_winterthur_pv` → no pool needed (no external API)

Also add a 1-second `pool_slots=1` sleep between tariff API requests when backfilling by
adding `time.sleep(1)` at the start of `_fetch_ekz`, `_fetch_ckw`, `_fetch_groupe_e` task
functions only during catchup runs. Detect catchup via:
```python
is_catchup = ctx.get("run_type") == "backfill"
if is_catchup:
    import time; time.sleep(1)
```

---

### Task 4 — Backfill DAG

Create a new DAG `airflow/dags/backfill_dag.py`:

```
DAG id:   bdsp_backfill
Schedule: None  (manual trigger only, no automatic schedule)
catchup:  False
```

This DAG accepts two Airflow Variables or DAG run conf parameters:
- `backfill_start`: `"YYYY-MM-DD"` (inclusive)
- `backfill_end`: `"YYYY-MM-DD"` (inclusive)

The DAG should:
1. Read `backfill_start` and `backfill_end` from `dag_run.conf` (fall back to Airflow Variables
   `BDSP_BACKFILL_START` / `BDSP_BACKFILL_END` if conf not set).
2. For each date in the range, run the same fetch functions as in `etl_pipeline_dag.py`,
   but pass each specific date explicitly. Use a `PythonOperator` per source (not per day) —
   each operator iterates over all dates for its source, with a 1-second delay between days
   to respect API limits.
3. Task dependency order: `[fetch_entsoe, fetch_weather, fetch_bafu]` run first (independent),
   then `[fetch_ekz, fetch_ckw, fetch_groupe_e]` (tariff APIs, sequential to reduce hammering).
   Stadtwerk Winterthur tasks run independently.
4. After all fetch tasks succeed, trigger a single `PythonOperator` `compute_eta_done` that
   reads the actual DB row counts and logs: `"Backfill complete: {n} rows across all tables."`

**ETA estimation helper** — add a new FastAPI endpoint:
```
POST /api/backfill/estimate
Body: {"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}
Response: {
    "total_days": 45,
    "estimated_api_calls": 315,
    "estimated_duration_minutes": 8,
    "sources": {
        "entsoe":   {"calls": 45, "already_in_db": 20, "to_fetch": 25},
        "openmeteo": {...},
        ...
    }
}
```

The estimate checks the DB for existing data (using `MIN`/`MAX` time per table) to skip
already-loaded date ranges. Duration estimate: `total_calls × 1.5 seconds / 60`.

---

### Task 5 — Admin Dashboard: Rate Limits Widget

Add a new section to `src/frontend/static/admin_dash.html` **between the existing DB Status
section and the Airflow DAGs section**.

The widget title is **"API Rate Limits"** and contains:

1. One card per data source (ENTSO-E, Open-Meteo, EKZ, CKW, Groupe E, BAFU) showing:
   - Source name + colour-coded status dot (green if <70% used, amber 70–90%, red >90% or
     if 429 was seen in the last 24h)
   - Calls today (last 24h) vs. known limit (or "limit unknown" if `null`)
   - A horizontal progress bar (hidden if limit is unknown)
   - Last 429 timestamp (if any in last 7 days)

2. Style: use the same card pattern already present in the dashboard (white card, rounded-xl,
   border border-slate-200, shadow-sm). Use the existing `primary` colour (#137fec) for the
   progress bar fill.

3. Data is fetched from `GET /api/rate-limits` on page load and auto-refreshed every 60 seconds
   using `setInterval`. Use the same `fetch()` pattern already used for `/api/db-status` in
   the existing dashboard JS.

---

### Task 6 — Admin Dashboard: Historical Backfill UI

Add a new section at the **bottom of the main content area** in `admin_dash.html`, titled
**"Historical Data Backfill"**.

The section contains:

1. **Input form:**
   - `Start date` date picker (HTML `<input type="date">`)
   - `End date` date picker
   - "Estimate" button → calls `POST /api/backfill/estimate`, displays the estimate result
     inline (total days, estimated API calls, estimated duration, per-source breakdown)
   - "Start Backfill" button → calls `POST /api/backfill/trigger`, which triggers the
     `bdsp_backfill` Airflow DAG via the Airflow REST API with the given date range as
     `dag_run.conf`

2. **New FastAPI endpoint** `POST /api/backfill/trigger`:
   - Accepts `{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}`
   - Validates that end_date >= start_date and neither date is in the future
   - Posts to Airflow REST API:
     `POST {AIRFLOW_URL}/api/v1/dags/bdsp_backfill/dagRuns`
     with body `{"conf": {"backfill_start": start_date, "backfill_end": end_date}}`
   - Returns `{"dag_run_id": "...", "state": "queued", "message": "Backfill triggered."}`
     or a 4xx/5xx error with a clear message.

3. **Progress panel** (shown after "Start Backfill" is clicked):
   - Polls `GET /api/airflow/dags` (already exists) every 10 seconds
   - Displays the state of the `bdsp_backfill` DAG run (queued / running / success / failed)
   - Shows a simple animated progress bar while state == "running"
   - On success: shows "✓ Backfill complete — data available for [start] to [end]."
   - On failure: shows the error in red with a "Retry" button

4. Disable the "Start Backfill" button while a run is already in "running" or "queued" state.
   Check current state on page load by polling `/api/airflow/dags` and filtering for
   `dag_id == "bdsp_backfill"`.

---

## Constraints & Non-Negotiables

1. **ML data isolation:** `api_call_log` must never appear in `FEATURE_COLS`, `LOAD_FEATURE_COLS`,
   `training_features` view, or `winterthur_net_load_features` view. Add an assertion in
   `validate_no_leakage()` that raises `ValueError` if any `api_call_log` column name is found
   in the feature column lists.

2. **No breaking changes to existing endpoints:** All existing `/api/*` endpoints in `main.py`
   must keep their current response schema. New endpoints are additive only.

3. **Stadtwerk Winterthur (OGD) is CSV-based, not date-parameterised.** Do not attempt to
   make it date-aware in the backfill. It will remain as a one-time bulk load.

4. **`catchup=True` only on the ETL DAG.** Feature export and training DAGs stay `catchup=False`.

5. **Airflow connections:** All Airflow REST API calls from FastAPI use the existing
   `_AIRFLOW_URL`, `_AIRFLOW_USER`, `_AIRFLOW_PASS` env vars already defined in `main.py`.

6. **Error resilience:** If `api_call_log` inserts fail (e.g. DB not ready), log a warning and
   continue. Never let tracking errors break data collection.

7. **Ruff & existing code style:** The project uses ruff for linting (`pyproject.toml`).
   All new code must pass `ruff check src/ airflow/`. Do not use bare `except:` clauses.

---

## Acceptance Criteria

- [ ] `pytest src/testing/` passes with no failures (including new tests from Task 1)
- [ ] `ruff check src/ airflow/` passes with 0 errors
- [ ] `api_call_log` table is created in `init.sql` and `features.sql` (idempotent)
- [ ] `GET /api/rate-limits` returns valid JSON with entries for all 6 sources
- [ ] `POST /api/backfill/estimate` returns correct day counts and source breakdown
- [ ] `POST /api/backfill/trigger` successfully creates an Airflow DAG run (test with mock)
- [ ] All 3 DAGs have `timezone="Europe/Zurich"` set
- [ ] ETL DAG has `catchup=True` and `max_active_runs=1`
- [ ] ETL DAG task functions use `ctx["logical_date"]` not `datetime.now()`
- [ ] `OpenMeteoCollector` uses archive endpoint for dates >5 days old
- [ ] Admin dashboard Rate Limits section renders and auto-refreshes
- [ ] Admin dashboard Backfill section renders, estimate works, trigger works, progress polls
- [ ] No column from `api_call_log` appears in `FEATURE_COLS` or `LOAD_FEATURE_COLS`

---

## Suggested Implementation Order

1. Task 1 (Open-Meteo archive) — self-contained, testable immediately
2. Task 2a (DB schema) — unblocks Task 2b/2c
3. Task 2b (BaseCollector tracking) — touches many files, do in one commit
4. Task 2c (FastAPI rate-limit endpoints)
5. Task 3 (Airflow timezone + catchup + execution-date + pools)
6. Task 4 (Backfill DAG + estimate endpoint + trigger endpoint)
7. Task 5 (Admin dashboard rate-limit widget)
8. Task 6 (Admin dashboard backfill UI)
9. Final: run full pytest + ruff, fix any issues
