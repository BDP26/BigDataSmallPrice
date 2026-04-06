"""
DAG: bdsp_backfill

Manual-trigger-only DAG for backfilling historical data.
No automatic schedule – triggered via Airflow UI or POST /api/backfill/trigger.

Configuration (via dag_run.conf or Airflow Variables):
    backfill_start: "YYYY-MM-DD"  (inclusive)
    backfill_end:   "YYYY-MM-DD"  (inclusive)

Task dependency order:
  Parallel ENTSO-E tasks: fetch_entsoe, fetch_entsoe_actual_load,
    fetch_entsoe_gen_ch_b12, fetch_entsoe_gen_ch_b16, fetch_entsoe_gen_de_b19,
    fetch_entsoe_flow_ch_de, fetch_entsoe_flow_de_ch,
    fetch_entsoe_flow_ch_it, fetch_entsoe_flow_it_ch,
    fetch_entsoe_flow_ch_fr, fetch_entsoe_flow_fr_ch,
    fetch_entsoe_flow_ch_at, fetch_entsoe_flow_at_ch,
    fetch_entsoe_load_forecast
  Plus parallel: fetch_weather, fetch_bafu
  Then sequential: fetch_ekz >> fetch_ckw >> fetch_groupe_e
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


_ENTSOE_SLEEP_S = 2  # seconds between requests per task; kept conservative to protect the API token


def _backfill_entsoe(**ctx) -> None:
    from etl.fetch_tasks import fetch_entsoe
    start, end = _get_date_range(ctx["dag_run"].conf or {})
    fetch_entsoe(_date_range(start, end), sleep_s=_ENTSOE_SLEEP_S)


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


def _backfill_entsoe_actual_load(**ctx) -> None:
    from etl.fetch_tasks import fetch_entsoe_actual_load
    start, end = _get_date_range(ctx["dag_run"].conf or {})
    fetch_entsoe_actual_load(_date_range(start, end), sleep_s=_ENTSOE_SLEEP_S)


def _make_gen_callable(domain: str, psr_type: str):
    def _backfill(**ctx):
        from etl.fetch_tasks import fetch_entsoe_generation
        start, end = _get_date_range(ctx["dag_run"].conf or {})
        fetch_entsoe_generation(_date_range(start, end), domain=domain, psr_type=psr_type, sleep_s=_ENTSOE_SLEEP_S)
    return _backfill


def _make_flow_callable(in_domain: str, out_domain: str):
    def _backfill(**ctx):
        from etl.fetch_tasks import fetch_entsoe_crossborder
        start, end = _get_date_range(ctx["dag_run"].conf or {})
        fetch_entsoe_crossborder(_date_range(start, end), in_domain=in_domain, out_domain=out_domain, sleep_s=_ENTSOE_SLEEP_S)
    return _backfill


def _backfill_entsoe_load_forecast(**ctx) -> None:
    from etl.fetch_tasks import fetch_entsoe_load_forecast
    start, end = _get_date_range(ctx["dag_run"].conf or {})
    fetch_entsoe_load_forecast(_date_range(start, end), sleep_s=_ENTSOE_SLEEP_S)


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

    _CH = "10YCH-SWISSGRIDZ"
    _DE = "10Y1001A1001A83F"
    _IT = "10YIT-GRTN-----B"
    _FR = "10YFR-RTE------C"
    _AT = "10YAT-APG------L"

    # ── ENTSO-E tasks (pool enforces max 1 concurrent; chain enforces strict order)
    # Both mechanisms together guarantee the token is never hammered simultaneously.
    _EP = {"pool": "entsoe_pool"}

    t_entsoe        = PythonOperator(task_id="fetch_entsoe",               python_callable=_backfill_entsoe,                              **_EP)
    t_actual_load   = PythonOperator(task_id="fetch_entsoe_actual_load",   python_callable=_backfill_entsoe_actual_load,                  **_EP)
    t_load_fc       = PythonOperator(task_id="fetch_entsoe_load_forecast", python_callable=_backfill_entsoe_load_forecast,                **_EP)
    t_gen_ch_b12    = PythonOperator(task_id="fetch_entsoe_gen_ch_b12",    python_callable=_make_gen_callable(_CH, "B12"),                **_EP)
    t_gen_ch_b16    = PythonOperator(task_id="fetch_entsoe_gen_ch_b16",    python_callable=_make_gen_callable(_CH, "B16"),                **_EP)
    t_gen_de_b19    = PythonOperator(task_id="fetch_entsoe_gen_de_b19",    python_callable=_make_gen_callable(_DE, "B19"),                **_EP)
    t_flow_ch_de    = PythonOperator(task_id="fetch_entsoe_flow_ch_de",    python_callable=_make_flow_callable(_CH, _DE),                 **_EP)
    t_flow_de_ch    = PythonOperator(task_id="fetch_entsoe_flow_de_ch",    python_callable=_make_flow_callable(_DE, _CH),                 **_EP)
    t_flow_ch_it    = PythonOperator(task_id="fetch_entsoe_flow_ch_it",    python_callable=_make_flow_callable(_CH, _IT),                 **_EP)
    t_flow_it_ch    = PythonOperator(task_id="fetch_entsoe_flow_it_ch",    python_callable=_make_flow_callable(_IT, _CH),                 **_EP)
    t_flow_ch_fr    = PythonOperator(task_id="fetch_entsoe_flow_ch_fr",    python_callable=_make_flow_callable(_CH, _FR),                 **_EP)
    t_flow_fr_ch    = PythonOperator(task_id="fetch_entsoe_flow_fr_ch",    python_callable=_make_flow_callable(_FR, _CH),                 **_EP)
    t_flow_ch_at    = PythonOperator(task_id="fetch_entsoe_flow_ch_at",    python_callable=_make_flow_callable(_CH, _AT),                 **_EP)
    t_flow_at_ch    = PythonOperator(task_id="fetch_entsoe_flow_at_ch",    python_callable=_make_flow_callable(_AT, _CH),                 **_EP)

    # ── Non-ENTSO-E tasks (no token risk, run in parallel)
    t_weather       = PythonOperator(task_id="fetch_weather",              python_callable=_backfill_weather)
    t_bafu          = PythonOperator(task_id="fetch_bafu",                 python_callable=_backfill_bafu)
    t_ekz           = PythonOperator(task_id="fetch_ekz",                  python_callable=_backfill_ekz)
    t_ckw           = PythonOperator(task_id="fetch_ckw",                  python_callable=_backfill_ckw)
    t_groupe_e      = PythonOperator(task_id="fetch_groupe_e",             python_callable=_backfill_groupe_e)
    t_wt_load       = PythonOperator(task_id="fetch_winterthur_load",      python_callable=_backfill_winterthur_load)
    t_wt_pv         = PythonOperator(task_id="fetch_winterthur_pv",        python_callable=_backfill_winterthur_pv)
    t_done          = PythonOperator(task_id="compute_eta_done",           python_callable=_compute_eta_done, trigger_rule="all_done")

    # ENTSO-E: strictly sequential chain — one task finishes before the next starts
    # Max rate: 1 request / _ENTSOE_SLEEP_S seconds → well within ENTSO-E limits
    (
        t_entsoe
        >> t_actual_load
        >> t_load_fc
        >> t_gen_ch_b12
        >> t_gen_ch_b16
        >> t_gen_de_b19
        >> t_flow_ch_de
        >> t_flow_de_ch
        >> t_flow_ch_it
        >> t_flow_it_ch
        >> t_flow_ch_fr
        >> t_flow_fr_ch
        >> t_flow_ch_at
        >> t_flow_at_ch
    )

    # Tariff APIs: sequential (EKZ API can also be sensitive to load)
    t_ekz >> t_ckw >> t_groupe_e

    # All branches converge at t_done
    [t_flow_at_ch, t_weather, t_bafu, t_groupe_e, t_wt_load, t_wt_pv] >> t_done
