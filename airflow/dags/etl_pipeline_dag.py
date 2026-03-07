"""
DAG: bdsp_etl_daily

Daily ETL pipeline for BigDataSmallPrice.
Runs at 06:00 UTC – after ENTSO-E publishes next-day prices.

Task graph:
  fetch_entsoe          ─┐
  fetch_weather         ─┤
  fetch_bafu            ─┤─► (parallel)  ─► log_summary
  fetch_winterthur_load ─┤
  fetch_winterthur_pv   ─┤
  fetch_ekz ─► fetch_ckw ─► fetch_groupe_e ─┘  (tariff APIs sequential)

Task logic lives in src/etl/fetch_tasks.py (shared with backfill_dag).
"""

import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.insert(0, "/opt/airflow/src")

default_args = {
    "owner": "bdsp",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ─── Task callables ───────────────────────────────────────────────────────────


def _fetch_entsoe(**ctx) -> None:
    from etl.fetch_tasks import fetch_entsoe
    # ENTSO-E day-ahead prices are for the delivery day = data_interval_end
    date_str = ctx["data_interval_end"].strftime("%Y-%m-%d")
    fetch_entsoe([date_str])


def _fetch_weather(**ctx) -> None:
    from etl.fetch_tasks import fetch_weather
    fetch_weather([ctx["logical_date"].strftime("%Y-%m-%d")])


def _fetch_ekz(**ctx) -> None:
    from etl.fetch_tasks import fetch_ekz
    fetch_ekz([ctx["logical_date"].strftime("%Y-%m-%d")])


def _fetch_ckw(**ctx) -> None:
    from etl.fetch_tasks import fetch_ckw
    fetch_ckw([ctx["logical_date"].strftime("%Y-%m-%d")])


def _fetch_groupe_e(**ctx) -> None:
    from etl.fetch_tasks import fetch_groupe_e
    fetch_groupe_e([ctx["logical_date"].strftime("%Y-%m-%d")])


def _fetch_bafu(**ctx) -> None:
    from etl.fetch_tasks import fetch_bafu
    fetch_bafu([ctx["logical_date"].strftime("%Y-%m-%d")])


def _fetch_winterthur_load(**ctx) -> None:
    from etl.fetch_tasks import fetch_winterthur_load
    fetch_winterthur_load(all_files=False)


def _fetch_winterthur_pv(**ctx) -> None:
    from etl.fetch_tasks import fetch_winterthur_pv
    fetch_winterthur_pv()


def _log_summary(**ctx) -> None:
    from etl.fetch_tasks import log_row_counts
    log_row_counts(date_str=ctx["logical_date"].strftime("%Y-%m-%d"))


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="bdsp_etl_daily",
    description="Daily ETL: ENTSO-E, Open-Meteo, CKW, Groupe E, BAFU → TimescaleDB",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=pendulum.timezone("Europe/Zurich")),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bdsp", "etl", "phase-1"],
) as dag:

    fetch_entsoe = PythonOperator(task_id="fetch_entsoe", python_callable=_fetch_entsoe)
    fetch_weather = PythonOperator(task_id="fetch_weather", python_callable=_fetch_weather)
    fetch_ekz = PythonOperator(task_id="fetch_ekz", python_callable=_fetch_ekz)
    fetch_ckw = PythonOperator(task_id="fetch_ckw", python_callable=_fetch_ckw)
    fetch_groupe_e = PythonOperator(task_id="fetch_groupe_e", python_callable=_fetch_groupe_e)
    fetch_bafu = PythonOperator(task_id="fetch_bafu", python_callable=_fetch_bafu)
    fetch_winterthur_load = PythonOperator(task_id="fetch_winterthur_load", python_callable=_fetch_winterthur_load)
    fetch_winterthur_pv = PythonOperator(task_id="fetch_winterthur_pv", python_callable=_fetch_winterthur_pv)
    log_summary = PythonOperator(task_id="log_summary", python_callable=_log_summary, trigger_rule="all_done")

    # Tariff APIs sequential; everything else parallel
    fetch_ekz >> fetch_ckw >> fetch_groupe_e

    [
        fetch_entsoe,
        fetch_weather,
        fetch_bafu,
        fetch_winterthur_load,
        fetch_winterthur_pv,
        fetch_groupe_e,
    ] >> log_summary
