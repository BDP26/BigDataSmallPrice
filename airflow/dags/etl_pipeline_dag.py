"""
DAG: bdsp_etl_daily

Daily ETL pipeline for BigDataSmallPrice.
Runs at 06:00 Europe/Zurich – after ENTSO-E publishes next-day prices.

Task graph:
  fetch_entsoe          ─► (independent)
  fetch_weather         ─► (independent)    (all fetch tasks run in parallel)
  fetch_ekz             ─► (independent)
  fetch_ckw             ─► (independent)
  fetch_groupe_e        ─► (independent)
  fetch_bafu            ─► (independent)
  fetch_winterthur_load ─► (independent)
  fetch_winterthur_pv   ─► (independent)

catchup=True + max_active_runs=1 so missed runs are backfilled one-at-a-time.
All task functions use ctx["logical_date"] to fetch the correct date's data.
"""

import sys
import time

from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Make src/ importable inside the Airflow container
sys.path.insert(0, "/opt/airflow/src")

# ─── Default args ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "bdsp",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ─── Task functions ───────────────────────────────────────────────────────────


def _fetch_entsoe(**ctx) -> None:
    from data_collection.entsoe_collector import EntsoeCollector
    from db.timescale_client import upsert_entsoe

    # ENTSO-E day-ahead prices are for the *delivery* day, which is
    # data_interval_end (the day the Airflow run covers, published the day
    # before at ~12:30 CET and already available when this task runs at 06:00).
    # Using data_interval_start would fetch the previous day's prices.
    delivery_day = ctx["data_interval_end"]
    period_start = delivery_day.replace(hour=0, minute=0, second=0, microsecond=0)
    period_end = period_start + timedelta(days=1)
    date_str = period_start.strftime("%Y-%m-%d")

    collector = EntsoeCollector(period_start=period_start, period_end=period_end)
    records = collector.run()
    inserted = upsert_entsoe(records)
    print(f"ENTSO-E: {len(records)} fetched, {inserted} inserted for {date_str}.")


def _fetch_weather(**ctx) -> None:
    from data_collection.openmeteo_collector import OpenMeteoCollector
    from db.timescale_client import upsert_weather

    exec_date = ctx["logical_date"]
    date_str = exec_date.strftime("%Y-%m-%d")

    collector = OpenMeteoCollector(date=date_str)
    records = collector.run()
    inserted = upsert_weather(records)
    print(f"Weather: {len(records)} fetched, {inserted} inserted for {date_str}.")


def _fetch_ekz(**ctx) -> None:
    from data_collection.ekz_collector import EkzCollector
    from db.timescale_client import upsert_ekz

    exec_date = ctx["logical_date"]
    date_str = exec_date.strftime("%Y-%m-%d")
    is_catchup = ctx.get("run_type") == "backfill"
    if is_catchup:
        time.sleep(1)

    collector = EkzCollector(date=date_str)
    records = collector.run()
    inserted = upsert_ekz(records)
    print(f"EKZ: {len(records)} fetched, {inserted} inserted for {date_str}.")


def _fetch_ckw(**ctx) -> None:
    from data_collection.ckw_collector import CKWCollector
    from db.timescale_client import upsert_ckw

    exec_date = ctx["logical_date"]
    date_str = exec_date.strftime("%Y-%m-%d")
    is_catchup = ctx.get("run_type") == "backfill"
    if is_catchup:
        time.sleep(1)

    collector = CKWCollector(date=date_str)
    records = collector.run()
    inserted = upsert_ckw(records)
    print(f"CKW: {len(records)} fetched, {inserted} inserted for {date_str}.")


def _fetch_groupe_e(**ctx) -> None:
    from data_collection.groupe_e_collector import GroupeECollector
    from db.timescale_client import upsert_groupe_e

    exec_date = ctx["logical_date"]
    date_str = exec_date.strftime("%Y-%m-%d")
    is_catchup = ctx.get("run_type") == "backfill"
    if is_catchup:
        time.sleep(1)

    collector = GroupeECollector(date=date_str)
    records = collector.run()
    inserted = upsert_groupe_e(records)
    print(f"Groupe E: {len(records)} fetched, {inserted} inserted for {date_str}.")


def _fetch_bafu(**ctx) -> None:
    from data_collection.bafu_collector import BafuCollector
    from db.timescale_client import upsert_bafu

    exec_date = ctx["logical_date"]
    date_str = exec_date.strftime("%Y-%m-%d")

    collector = BafuCollector(date=date_str)
    records = collector.run()
    inserted = upsert_bafu(records)
    print(f"BAFU: {len(records)} fetched, {inserted} inserted for {date_str}.")


def _fetch_winterthur_load(**ctx) -> None:
    from data_collection.stadtwerk_winterthur_collector import BruttolastgangCollector
    from db.timescale_client import upsert_winterthur_load

    # CSV-based: not date-parameterised; always fetches the latest file.
    collector = BruttolastgangCollector(all_files=False)
    records = collector.run()
    inserted = upsert_winterthur_load(records)
    print(f"Winterthur Load: fetched {len(records)} records, inserted {inserted}.")


def _fetch_winterthur_pv(**ctx) -> None:
    from data_collection.stadtwerk_winterthur_collector import NetzEinspeisungCollector
    from db.timescale_client import upsert_winterthur_pv

    # CSV-based: not date-parameterised; always fetches the latest file.
    collector = NetzEinspeisungCollector()
    records = collector.run()
    inserted = upsert_winterthur_pv(records)
    print(f"Winterthur PV: fetched {len(records)} records, inserted {inserted}.")


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="bdsp_etl_daily",
    description="Daily ETL: ENTSO-E, Open-Meteo, CKW, Groupe E, BAFU → TimescaleDB",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=pendulum.timezone("Europe/Zurich")),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["bdsp", "etl", "phase-1"],
) as dag:

    fetch_entsoe = PythonOperator(
        task_id="fetch_entsoe",
        python_callable=_fetch_entsoe,
        pool="entsoe_pool",
    )

    fetch_weather = PythonOperator(
        task_id="fetch_weather",
        python_callable=_fetch_weather,
        pool="meteo_pool",
    )

    fetch_ekz = PythonOperator(
        task_id="fetch_ekz",
        python_callable=_fetch_ekz,
        pool="tariff_pool",
    )

    fetch_ckw = PythonOperator(
        task_id="fetch_ckw",
        python_callable=_fetch_ckw,
        pool="tariff_pool",
    )

    fetch_groupe_e = PythonOperator(
        task_id="fetch_groupe_e",
        python_callable=_fetch_groupe_e,
        pool="tariff_pool",
    )

    fetch_bafu = PythonOperator(
        task_id="fetch_bafu",
        python_callable=_fetch_bafu,
        pool="bafu_pool",
    )

    fetch_winterthur_load = PythonOperator(
        task_id="fetch_winterthur_load",
        python_callable=_fetch_winterthur_load,
    )

    fetch_winterthur_pv = PythonOperator(
        task_id="fetch_winterthur_pv",
        python_callable=_fetch_winterthur_pv,
    )

    # All fetch tasks are independent – they run in parallel automatically
    # when the LocalExecutor picks them up.
