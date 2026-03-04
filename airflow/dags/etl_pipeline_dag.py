"""
DAG: bdsp_etl_daily

Daily ETL pipeline for BigDataSmallPrice.
Runs at 06:00 UTC – after ENTSO-E publishes next-day prices (~12:00 CET = 11:00 UTC,
but previous-day prices are available from early morning).

Task graph:
  fetch_entsoe          ─► (independent)
  fetch_weather         ─► (independent)    (all fetch tasks run in parallel)
  fetch_ekz             ─► (independent)
  fetch_ckw             ─► (independent)
  fetch_groupe_e        ─► (independent)
  fetch_bafu            ─► (independent)
  fetch_winterthur_load ─► (independent)
  fetch_winterthur_pv   ─► (independent)
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


def _fetch_ckw(**ctx) -> None:
    from data_collection.ckw_collector import CKWCollector
    from db.timescale_client import upsert_ckw

    collector = CKWCollector()
    records = collector.run()
    inserted = upsert_ckw(records)
    print(f"CKW: fetched {len(records)} records, inserted {inserted}.")


def _fetch_groupe_e(**ctx) -> None:
    from data_collection.groupe_e_collector import GroupeECollector
    from db.timescale_client import upsert_groupe_e

    collector = GroupeECollector()
    records = collector.run()
    inserted = upsert_groupe_e(records)
    print(f"Groupe E: fetched {len(records)} records, inserted {inserted}.")


def _fetch_bafu(**ctx) -> None:
    from data_collection.bafu_collector import BafuCollector
    from db.timescale_client import upsert_bafu

    collector = BafuCollector()
    records = collector.run()
    inserted = upsert_bafu(records)
    print(f"BAFU: fetched {len(records)} records, inserted {inserted}.")


def _fetch_winterthur_load(**ctx) -> None:
    from data_collection.stadtwerk_winterthur_collector import BruttolastgangCollector
    from db.timescale_client import upsert_winterthur_load

    collector = BruttolastgangCollector(all_files=False)
    records = collector.run()
    inserted = upsert_winterthur_load(records)
    print(f"Winterthur Load: fetched {len(records)} records, inserted {inserted}.")


def _fetch_winterthur_pv(**ctx) -> None:
    from data_collection.stadtwerk_winterthur_collector import NetzEinspeisungCollector
    from db.timescale_client import upsert_winterthur_pv

    collector = NetzEinspeisungCollector()
    records = collector.run()
    inserted = upsert_winterthur_pv(records)
    print(f"Winterthur PV: fetched {len(records)} records, inserted {inserted}.")


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="bdsp_etl_daily",
    description="Daily ETL: ENTSO-E, Open-Meteo, CKW, Groupe E, BAFU → TimescaleDB",
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

    fetch_ckw = PythonOperator(
        task_id="fetch_ckw",
        python_callable=_fetch_ckw,
    )

    fetch_groupe_e = PythonOperator(
        task_id="fetch_groupe_e",
        python_callable=_fetch_groupe_e,
    )

    fetch_bafu = PythonOperator(
        task_id="fetch_bafu",
        python_callable=_fetch_bafu,
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
    # (No explicit dependency between the eight pipelines.)
