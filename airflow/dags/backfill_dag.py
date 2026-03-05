"""
DAG: bdsp_backfill

Manual-trigger-only DAG for backfilling historical data.
No automatic schedule – triggered via Airflow UI or POST /api/backfill/trigger.

Configuration (via dag_run.conf or Airflow Variables):
    backfill_start: "YYYY-MM-DD"  (inclusive)
    backfill_end:   "YYYY-MM-DD"  (inclusive)

Task dependency order:
  [fetch_entsoe, fetch_weather, fetch_bafu] run in parallel (independent)
  then [fetch_ekz, fetch_ckw, fetch_groupe_e] run sequentially (tariff APIs)
  fetch_winterthur_load, fetch_winterthur_pv run independently (CSV, no date param)
  finally: compute_eta_done logs completion row counts
"""

import sys
import time

from datetime import datetime, timedelta, date as _date

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/src")

# ─── Default args ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "bdsp",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── Helpers ──────────────────────────────────────────────────────────────────


def _get_date_range(conf: dict) -> tuple[_date, _date]:
    """Extract backfill_start / backfill_end from conf or Airflow Variables.

    Priority:
      1. dag_run.conf keys  "backfill_start" / "backfill_end"   (admin dashboard)
      2. Airflow Variables  BDSP_BACKFILL_START / BDSP_BACKFILL_END

    The recommended way to trigger this DAG is via the admin dashboard at /.
    If you must trigger it manually from the Airflow UI, use
    "Trigger DAG w/ config" and supply JSON like:
        {"backfill_start": "2026-01-01", "backfill_end": "2026-01-31"}
    """
    start_str = conf.get("backfill_start") or Variable.get("BDSP_BACKFILL_START", default_var=None)
    end_str   = conf.get("backfill_end")   or Variable.get("BDSP_BACKFILL_END",   default_var=None)
    if not start_str or not end_str:
        raise ValueError(
            "Missing backfill date range.\n"
            "  Option 1 (recommended): use the admin dashboard at http://<host>/ "
            "– enter start/end dates and click 'Trigger Backfill'.\n"
            "  Option 2: in the Airflow UI, choose 'Trigger DAG w/ config' and "
            'supply {"backfill_start": "YYYY-MM-DD", "backfill_end": "YYYY-MM-DD"}.\n'
            "  Option 3: set Airflow Variables BDSP_BACKFILL_START and "
            "BDSP_BACKFILL_END before triggering."
        )
    try:
        start = _date.fromisoformat(start_str)
        end = _date.fromisoformat(end_str)
    except ValueError as exc:
        raise ValueError(
            "Invalid backfill date format. Expected YYYY-MM-DD for "
            "backfill_start and backfill_end."
        ) from exc
    if end < start:
        raise ValueError("Invalid backfill range: backfill_end must be >= backfill_start.")
    return start, end


def _date_range(start: _date, end: _date) -> list[str]:
    if end < start:
        raise ValueError("Invalid date range: end must be >= start.")
    days = (end - start).days + 1
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]


# ─── Task functions ───────────────────────────────────────────────────────────


def _backfill_entsoe(**ctx) -> None:
    from data_collection.entsoe_collector import EntsoeCollector
    from db.timescale_client import upsert_entsoe

    start, end = _get_date_range(ctx["dag_run"].conf or {})
    for date_str in _date_range(start, end):
        period_start = datetime.fromisoformat(date_str).replace(tzinfo=pendulum.UTC)
        period_end = period_start + timedelta(days=1)
        collector = EntsoeCollector(period_start=period_start, period_end=period_end)
        records = collector.run()
        inserted = upsert_entsoe(records)
        print(f"  ENTSO-E {date_str}: {len(records)} fetched, {inserted} inserted.")
        time.sleep(1)


def _backfill_weather(**ctx) -> None:
    from data_collection.openmeteo_collector import OpenMeteoCollector
    from db.timescale_client import upsert_weather

    start, end = _get_date_range(ctx["dag_run"].conf or {})
    for date_str in _date_range(start, end):
        collector = OpenMeteoCollector(date=date_str)
        records = collector.run()
        inserted = upsert_weather(records)
        print(f"  Weather {date_str}: {len(records)} fetched, {inserted} inserted.")
        time.sleep(1)


def _backfill_bafu(**ctx) -> None:
    from data_collection.bafu_collector import BafuCollector
    from db.timescale_client import upsert_bafu

    start, end = _get_date_range(ctx["dag_run"].conf or {})
    for date_str in _date_range(start, end):
        collector = BafuCollector(date=date_str)
        records = collector.run()
        inserted = upsert_bafu(records)
        print(f"  BAFU {date_str}: {len(records)} fetched, {inserted} inserted.")
        time.sleep(1)


def _backfill_ekz(**ctx) -> None:
    from data_collection.ekz_collector import EkzCollector
    from db.timescale_client import upsert_ekz

    start, end = _get_date_range(ctx["dag_run"].conf or {})
    for date_str in _date_range(start, end):
        collector = EkzCollector(date=date_str)
        records = collector.run()
        inserted = upsert_ekz(records)
        print(f"  EKZ {date_str}: {len(records)} fetched, {inserted} inserted.")
        time.sleep(1)


def _backfill_ckw(**ctx) -> None:
    from data_collection.ckw_collector import CKWCollector
    from db.timescale_client import upsert_ckw

    start, end = _get_date_range(ctx["dag_run"].conf or {})
    for date_str in _date_range(start, end):
        collector = CKWCollector(date=date_str)
        records = collector.run()
        inserted = upsert_ckw(records)
        print(f"  CKW {date_str}: {len(records)} fetched, {inserted} inserted.")
        time.sleep(1)


def _backfill_groupe_e(**ctx) -> None:
    from data_collection.groupe_e_collector import GroupeECollector
    from db.timescale_client import upsert_groupe_e

    start, end = _get_date_range(ctx["dag_run"].conf or {})
    for date_str in _date_range(start, end):
        collector = GroupeECollector(date=date_str)
        records = collector.run()
        inserted = upsert_groupe_e(records)
        print(f"  Groupe E {date_str}: {len(records)} fetched, {inserted} inserted.")
        time.sleep(1)


def _backfill_winterthur_load(**ctx) -> None:
    from data_collection.stadtwerk_winterthur_collector import BruttolastgangCollector
    from db.timescale_client import upsert_winterthur_load

    # CSV-based: one bulk fetch regardless of date range
    collector = BruttolastgangCollector(all_files=True)
    records = collector.run()
    inserted = upsert_winterthur_load(records)
    print(f"Winterthur Load (bulk): {len(records)} records, {inserted} inserted.")


def _backfill_winterthur_pv(**ctx) -> None:
    from data_collection.stadtwerk_winterthur_collector import NetzEinspeisungCollector
    from db.timescale_client import upsert_winterthur_pv

    collector = NetzEinspeisungCollector()
    records = collector.run()
    inserted = upsert_winterthur_pv(records)
    print(f"Winterthur PV (bulk): {len(records)} records, {inserted} inserted.")


def _compute_eta_done(**ctx) -> None:
    import os
    import psycopg2

    _TABLES = [
        "entsoe_day_ahead_prices",
        "weather_hourly",
        "bafu_hydro",
        "ekz_tariffs_raw",
        "ckw_tariffs_raw",
        "groupe_e_tariffs_raw",
        "winterthur_load",
        "winterthur_pv",
    ]
    try:
        conn = psycopg2.connect(
            host=os.getenv("BDSP_DB_HOST", "timescaledb"),
            port=int(os.getenv("BDSP_DB_PORT", 5432)),
            dbname=os.getenv("BDSP_DB_NAME", "bdsp"),
            user=os.getenv("BDSP_DB_USER", "bdsp"),
            password=os.getenv("BDSP_DB_PASSWORD", ""),
        )
        total = 0
        with conn.cursor() as cur:
            for table in _TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
                n = cur.fetchone()[0]
                total += n
                print(f"  {table}: {n} rows")
        conn.close()
        start, end = _get_date_range(ctx["dag_run"].conf or {})
        print(f"Backfill complete: {total} rows across all tables "
              f"({start.isoformat()} → {end.isoformat()}).")
    except Exception as exc:  # noqa: BLE001
        print(f"Row count failed (non-critical): {exc}")


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="bdsp_backfill",
    description="Manual backfill of historical data for all ETL sources",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=pendulum.timezone("Europe/Zurich")),
    catchup=False,
    default_args=default_args,
    tags=["bdsp", "backfill"],
) as dag:

    t_entsoe = PythonOperator(task_id="fetch_entsoe",    python_callable=_backfill_entsoe)
    t_weather = PythonOperator(task_id="fetch_weather",  python_callable=_backfill_weather)
    t_bafu    = PythonOperator(task_id="fetch_bafu",     python_callable=_backfill_bafu)
    t_ekz     = PythonOperator(task_id="fetch_ekz",      python_callable=_backfill_ekz)
    t_ckw     = PythonOperator(task_id="fetch_ckw",      python_callable=_backfill_ckw)
    t_groupe_e = PythonOperator(task_id="fetch_groupe_e", python_callable=_backfill_groupe_e)
    t_wt_load  = PythonOperator(task_id="fetch_winterthur_load", python_callable=_backfill_winterthur_load)
    t_wt_pv    = PythonOperator(task_id="fetch_winterthur_pv",   python_callable=_backfill_winterthur_pv)
    t_done     = PythonOperator(task_id="compute_eta_done",       python_callable=_compute_eta_done)

    # First wave: independent sources
    # Second wave: tariff APIs (sequential to reduce API hammering)
    [t_entsoe, t_weather, t_bafu] >> t_ekz >> t_ckw >> t_groupe_e >> t_done
    [t_wt_load, t_wt_pv] >> t_done
