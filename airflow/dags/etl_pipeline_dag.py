"""
DAG: bdsp_etl_daily

Daily ETL pipeline for BigDataSmallPrice.
Runs at 06:00 Europe/Zurich – after ENTSO-E publishes next-day prices.

Task graph:
  fetch_entsoe          ─┐
  fetch_weather         ─┤
  fetch_bafu            ─┤─► (parallel)  ─► log_summary
  fetch_winterthur_load ─┤
  fetch_winterthur_pv   ─┤
  fetch_ekz ─► fetch_ckw ─► fetch_groupe_e ─┘  (tariff APIs sequential)

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


def _is_historical_catchup_run(ctx: dict) -> bool:
    """
    Detect catchup-style historical runs robustly.

    Airflow catchup runs are usually "scheduled" dag runs for older logical dates;
    explicit backfill jobs use run_type "backfill".
    """
    dag_run = ctx.get("dag_run")
    if dag_run is None:
        return False

    run_type = str(getattr(dag_run, "run_type", ""))
    if run_type == "backfill":
        return True

    logical_date = ctx.get("logical_date")
    if logical_date is None:
        return False

    logical_local = pendulum.instance(logical_date).in_timezone("Europe/Zurich").date()
    today_local = pendulum.now("Europe/Zurich").date()
    return run_type == "scheduled" and logical_local < today_local


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
    if _is_historical_catchup_run(ctx):
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
    if _is_historical_catchup_run(ctx):
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
    if _is_historical_catchup_run(ctx):
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
    catchup=False,
    max_active_runs=1,
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

    def _log_summary(**ctx) -> None:
        from db.timescale_client import get_conn
        date_str = ctx["logical_date"].strftime("%Y-%m-%d")
        tables = [
            "entsoe_day_ahead_prices",
            "weather_hourly",
            "ekz_tariffs",
            "ckw_tariffs",
            "groupe_e_tariffs",
            "bafu_hydro",
            "winterthur_load",
            "winterthur_pv",
        ]
        lines = [f"ETL summary for {date_str}:"]
        with get_conn() as conn:
            with conn.cursor() as cur:
                for table in tables:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE time::date = %s",
                        (date_str,),
                    )
                    count = cur.fetchone()[0]
                    lines.append(f"  {table}: {count} rows")
        print("\n".join(lines))

    log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=_log_summary,
        trigger_rule="all_done",
    )

    # Tariff APIs run sequentially to avoid hammering the same endpoints.
    # All other fetch tasks run in parallel.
    fetch_ekz >> fetch_ckw >> fetch_groupe_e

    [
        fetch_entsoe,
        fetch_weather,
        fetch_bafu,
        fetch_winterthur_load,
        fetch_winterthur_pv,
        fetch_groupe_e,
    ] >> log_summary
