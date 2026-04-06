"""
DAG: bdsp_etl_daily

Daily ETL pipeline for BigDataSmallPrice.
Runs at 06:00 UTC – after ENTSO-E publishes next-day prices.

Task graph:
  fetch_entsoe               ─┐
  fetch_entsoe_actual_load   ─┤
  fetch_entsoe_gen_ch_b12    ─┤
  fetch_entsoe_gen_ch_b16    ─┤
  fetch_entsoe_gen_de_b19    ─┤
  fetch_entsoe_flow_ch_de    ─┤
  fetch_entsoe_flow_de_ch    ─┤
  fetch_entsoe_flow_ch_it    ─┤
  fetch_entsoe_flow_it_ch    ─┤─► (parallel)  ─► log_summary
  fetch_entsoe_flow_ch_fr    ─┤
  fetch_entsoe_flow_fr_ch    ─┤
  fetch_entsoe_flow_ch_at    ─┤
  fetch_entsoe_flow_at_ch    ─┤
  fetch_entsoe_load_forecast ─┤
  fetch_weather              ─┤
  fetch_bafu                 ─┤
  fetch_winterthur_load      ─┤
  fetch_winterthur_pv        ─┤
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


def _fetch_entsoe_actual_load(**ctx) -> None:
    from etl.fetch_tasks import fetch_entsoe_actual_load
    # A65 actual load is for the previous day (historical realised values)
    date_str = ctx["logical_date"].strftime("%Y-%m-%d")
    fetch_entsoe_actual_load([date_str])


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


def _make_gen_callable(domain: str, psr_type: str):
    def _fetch(**ctx):
        from etl.fetch_tasks import fetch_entsoe_generation
        fetch_entsoe_generation([ctx["logical_date"].strftime("%Y-%m-%d")], domain=domain, psr_type=psr_type)
    return _fetch


def _make_flow_callable(in_domain: str, out_domain: str):
    def _fetch(**ctx):
        from etl.fetch_tasks import fetch_entsoe_crossborder
        fetch_entsoe_crossborder([ctx["logical_date"].strftime("%Y-%m-%d")], in_domain=in_domain, out_domain=out_domain)
    return _fetch


def _fetch_entsoe_load_forecast(**ctx) -> None:
    from etl.fetch_tasks import fetch_entsoe_load_forecast
    # Load forecast is for the delivery day = data_interval_end
    fetch_entsoe_load_forecast([ctx["data_interval_end"].strftime("%Y-%m-%d")])


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
    fetch_entsoe_actual_load = PythonOperator(task_id="fetch_entsoe_actual_load", python_callable=_fetch_entsoe_actual_load)
    fetch_weather = PythonOperator(task_id="fetch_weather", python_callable=_fetch_weather)
    fetch_ekz = PythonOperator(task_id="fetch_ekz", python_callable=_fetch_ekz)
    fetch_ckw = PythonOperator(task_id="fetch_ckw", python_callable=_fetch_ckw)
    fetch_groupe_e = PythonOperator(task_id="fetch_groupe_e", python_callable=_fetch_groupe_e)
    fetch_bafu = PythonOperator(task_id="fetch_bafu", python_callable=_fetch_bafu)
    fetch_winterthur_load = PythonOperator(task_id="fetch_winterthur_load", python_callable=_fetch_winterthur_load)
    fetch_winterthur_pv = PythonOperator(task_id="fetch_winterthur_pv", python_callable=_fetch_winterthur_pv)
    log_summary = PythonOperator(task_id="log_summary", python_callable=_log_summary, trigger_rule="all_done")

    _CH = "10YCH-SWISSGRIDZ"
    _DE = "10Y1001A1001A83F"
    _IT = "10YIT-GRTN-----B"
    _FR = "10YFR-RTE------C"
    _AT = "10YAT-APG------L"

    fetch_entsoe_gen_ch_b12 = PythonOperator(task_id="fetch_entsoe_gen_ch_b12", python_callable=_make_gen_callable(_CH, "B12"))
    fetch_entsoe_gen_ch_b16 = PythonOperator(task_id="fetch_entsoe_gen_ch_b16", python_callable=_make_gen_callable(_CH, "B16"))
    fetch_entsoe_gen_de_b19 = PythonOperator(task_id="fetch_entsoe_gen_de_b19", python_callable=_make_gen_callable(_DE, "B19"))
    fetch_entsoe_flow_ch_de = PythonOperator(task_id="fetch_entsoe_flow_ch_de", python_callable=_make_flow_callable(_CH, _DE))
    fetch_entsoe_flow_de_ch = PythonOperator(task_id="fetch_entsoe_flow_de_ch", python_callable=_make_flow_callable(_DE, _CH))
    fetch_entsoe_flow_ch_it = PythonOperator(task_id="fetch_entsoe_flow_ch_it", python_callable=_make_flow_callable(_CH, _IT))
    fetch_entsoe_flow_it_ch = PythonOperator(task_id="fetch_entsoe_flow_it_ch", python_callable=_make_flow_callable(_IT, _CH))
    fetch_entsoe_flow_ch_fr = PythonOperator(task_id="fetch_entsoe_flow_ch_fr", python_callable=_make_flow_callable(_CH, _FR))
    fetch_entsoe_flow_fr_ch = PythonOperator(task_id="fetch_entsoe_flow_fr_ch", python_callable=_make_flow_callable(_FR, _CH))
    fetch_entsoe_flow_ch_at = PythonOperator(task_id="fetch_entsoe_flow_ch_at", python_callable=_make_flow_callable(_CH, _AT))
    fetch_entsoe_flow_at_ch = PythonOperator(task_id="fetch_entsoe_flow_at_ch", python_callable=_make_flow_callable(_AT, _CH))
    fetch_entsoe_load_fc    = PythonOperator(task_id="fetch_entsoe_load_forecast", python_callable=_fetch_entsoe_load_forecast)

    # Tariff APIs sequential; everything else parallel
    fetch_ekz >> fetch_ckw >> fetch_groupe_e

    [
        fetch_entsoe,
        fetch_entsoe_actual_load,
        fetch_entsoe_gen_ch_b12,
        fetch_entsoe_gen_ch_b16,
        fetch_entsoe_gen_de_b19,
        fetch_entsoe_flow_ch_de,
        fetch_entsoe_flow_de_ch,
        fetch_entsoe_flow_ch_it,
        fetch_entsoe_flow_it_ch,
        fetch_entsoe_flow_ch_fr,
        fetch_entsoe_flow_fr_ch,
        fetch_entsoe_flow_ch_at,
        fetch_entsoe_flow_at_ch,
        fetch_entsoe_load_fc,
        fetch_weather,
        fetch_bafu,
        fetch_winterthur_load,
        fetch_winterthur_pv,
        fetch_groupe_e,
    ] >> log_summary
