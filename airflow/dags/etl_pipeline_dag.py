"""
DAG: bdsp_etl_daily

Daily ETL pipeline for BigDataSmallPrice.
Runs at 06:00 UTC – after ENTSO-E publishes next-day prices (~12:00 CET = 11:00 UTC,
but previous-day prices are available from early morning).

Task graph:
  fetch_entsoe ─► load_entsoe
  fetch_weather ─► load_weather      (all fetch tasks run in parallel)
  fetch_ekz ─► load_ekz
  fetch_bafu ─► load_bafu
"""

import sys
import os

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make src/ importable inside the Airflow container
sys.path.insert(0, "/opt/airflow/src")

# ─── Default args ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "bdsp",
    "retries": 2,
    "retry_delay": __import__("datetime").timedelta(minutes=5),
}

# ─── Task functions ───────────────────────────────────────────────────────────


def _fetch_entsoe(**ctx) -> None:
    from data_collection.entsoe_collector import EntsoeCollector
    from db.timescale_client import upsert_entsoe

    collector = EntsoeCollector()
    records = collector.run()
    inserted = upsert_entsoe(records)
    print(f"ENTSO-E: fetched {len(records)} records, inserted {inserted}.")


def _fetch_weather(**ctx) -> None:
    from data_collection.openmeteo_collector import OpenMeteoCollector
    from db.timescale_client import upsert_weather

    collector = OpenMeteoCollector()
    records = collector.run()
    inserted = upsert_weather(records)
    print(f"Weather: fetched {len(records)} records, inserted {inserted}.")


def _fetch_ekz(**ctx) -> None:
    from data_collection.ekz_collector import EkzCollector
    from db.timescale_client import upsert_ekz

    collector = EkzCollector()
    records = collector.run()
    inserted = upsert_ekz(records)
    print(f"EKZ: fetched {len(records)} records, inserted {inserted}.")


def _fetch_bafu(**ctx) -> None:
    from data_collection.bafu_collector import BafuCollector
    from db.timescale_client import upsert_bafu

    collector = BafuCollector()
    records = collector.run()
    inserted = upsert_bafu(records)
    print(f"BAFU: fetched {len(records)} records, inserted {inserted}.")


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="bdsp_etl_daily",
    description="Daily ETL: ENTSO-E, Open-Meteo, EKZ, BAFU → TimescaleDB",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["bdsp", "etl", "phase-1"],
) as dag:

    fetch_entsoe = PythonOperator(
        task_id="fetch_entsoe",
        python_callable=_fetch_entsoe,
    )

    fetch_weather = PythonOperator(
        task_id="fetch_weather",
        python_callable=_fetch_weather,
    )

    fetch_ekz = PythonOperator(
        task_id="fetch_ekz",
        python_callable=_fetch_ekz,
    )

    fetch_bafu = PythonOperator(
        task_id="fetch_bafu",
        python_callable=_fetch_bafu,
    )

    # All fetch tasks are independent – they run in parallel automatically
    # when the LocalExecutor picks them up.
    # (No explicit dependency between the four pipelines.)
