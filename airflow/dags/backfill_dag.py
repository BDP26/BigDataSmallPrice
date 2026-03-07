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

Task logic lives in src/etl/fetch_tasks.py (shared with etl_pipeline_dag).
"""

import sys
from datetime import datetime, timedelta, date as _date

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/src")

default_args = {
    "owner": "bdsp",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── Helpers ──────────────────────────────────────────────────────────────────


def _get_date_range(conf: dict) -> tuple[_date, _date]:
    """Extract backfill_start / backfill_end from conf or Airflow Variables."""
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
        end   = _date.fromisoformat(end_str)
    except ValueError as exc:
        raise ValueError(
            "Invalid backfill date format. Expected YYYY-MM-DD."
        ) from exc
    if end < start:
        raise ValueError("Invalid backfill range: backfill_end must be >= backfill_start.")
    return start, end


def _date_range(start: _date, end: _date) -> list[str]:
    days = (end - start).days + 1
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]


# ─── Task callables ───────────────────────────────────────────────────────────


def _backfill_entsoe(**ctx) -> None:
    from etl.fetch_tasks import fetch_entsoe
    start, end = _get_date_range(ctx["dag_run"].conf or {})
    fetch_entsoe(_date_range(start, end), sleep_s=1)


def _backfill_weather(**ctx) -> None:
    from etl.fetch_tasks import fetch_weather
    start, end = _get_date_range(ctx["dag_run"].conf or {})
    fetch_weather(_date_range(start, end), sleep_s=1)


def _backfill_ekz(**ctx) -> None:
    from etl.fetch_tasks import fetch_ekz
    start, end = _get_date_range(ctx["dag_run"].conf or {})
    fetch_ekz(_date_range(start, end), sleep_s=1)


def _backfill_ckw(**ctx) -> None:
    from etl.fetch_tasks import fetch_ckw
    start, end = _get_date_range(ctx["dag_run"].conf or {})
    fetch_ckw(_date_range(start, end), sleep_s=1)


def _backfill_groupe_e(**ctx) -> None:
    from etl.fetch_tasks import fetch_groupe_e
    start, end = _get_date_range(ctx["dag_run"].conf or {})
    fetch_groupe_e(_date_range(start, end), sleep_s=1)


def _backfill_bafu(**ctx) -> None:
    from etl.fetch_tasks import fetch_bafu
    start, end = _get_date_range(ctx["dag_run"].conf or {})
    fetch_bafu(_date_range(start, end), sleep_s=1)


def _backfill_winterthur_load(**ctx) -> None:
    from etl.fetch_tasks import fetch_winterthur_load
    fetch_winterthur_load(all_files=True)


def _backfill_winterthur_pv(**ctx) -> None:
    from etl.fetch_tasks import fetch_winterthur_pv
    fetch_winterthur_pv()


def _compute_eta_done(**ctx) -> None:
    from etl.fetch_tasks import log_row_counts
    start, end = _get_date_range(ctx["dag_run"].conf or {})
    print(f"Backfill complete: {start.isoformat()} → {end.isoformat()}")
    log_row_counts()


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

    t_entsoe   = PythonOperator(task_id="fetch_entsoe",          python_callable=_backfill_entsoe)
    t_weather  = PythonOperator(task_id="fetch_weather",         python_callable=_backfill_weather)
    t_bafu     = PythonOperator(task_id="fetch_bafu",            python_callable=_backfill_bafu)
    t_ekz      = PythonOperator(task_id="fetch_ekz",             python_callable=_backfill_ekz)
    t_ckw      = PythonOperator(task_id="fetch_ckw",             python_callable=_backfill_ckw)
    t_groupe_e = PythonOperator(task_id="fetch_groupe_e",        python_callable=_backfill_groupe_e)
    t_wt_load  = PythonOperator(task_id="fetch_winterthur_load", python_callable=_backfill_winterthur_load)
    t_wt_pv    = PythonOperator(task_id="fetch_winterthur_pv",   python_callable=_backfill_winterthur_pv)
    t_done     = PythonOperator(task_id="compute_eta_done",      python_callable=_compute_eta_done, trigger_rule="all_done")

    [t_entsoe, t_weather, t_bafu] >> t_ekz >> t_ckw >> t_groupe_e >> t_done
    [t_wt_load, t_wt_pv] >> t_done
