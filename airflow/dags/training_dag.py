"""
DAG: bdsp_training_daily

On-demand model retraining pipeline for BigDataSmallPrice.
Schedule is None – the DAG is triggered manually via the Airflow UI or the
admin dashboard POST /api/training/trigger endpoint.

Task graph:
  run_training      → train naive + linear + XGBoost (Model B, EPEX price)
  train_load_model  → train naive + linear + XGBoost (Model A, net load)

Both tasks run in parallel (independent datasets) and save:
  - <name>_<YYYYMMDD>.joblib  model files
  - metrics_<YYYYMMDD>.json   evaluation metrics

The trained models are automatically picked up by the FastAPI /api/predict
and /api/forecast endpoints on their next request (lazy model cache reload).
"""

import sys
import os

from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Make src/ importable inside the Airflow container
sys.path.insert(0, "/opt/airflow/src")

# ─── Default args ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "bdsp",
    "retries": 1,
    "retry_delay": __import__("datetime").timedelta(minutes=10),
}

# ─── Task functions ───────────────────────────────────────────────────────────


def _run_training(**ctx) -> None:
    from modelling.train import run_training

    data_dir   = os.environ.get(
        "BDSP_ENERGY_EXPORT_DIR",
        os.environ.get("BDSP_EXPORT_DIR", "/opt/airflow/data/energy/"),
    )
    models_dir = os.environ.get("BDSP_MODELS_DIR",  "/opt/airflow/models/")

    paths = run_training(data_dir=data_dir, models_dir=models_dir)
    for name, path in paths.items():
        print(f"Saved {name}: {path}")


def _run_load_training(**ctx) -> None:
    from modelling.train import run_load_training

    data_dir   = os.environ.get(
        "BDSP_LOAD_EXPORT_DIR",
        os.environ.get("BDSP_EXPORT_DIR", "/opt/airflow/data/load/"),
    )
    models_dir = os.environ.get("BDSP_MODELS_DIR",  "/opt/airflow/models/")

    paths = run_load_training(data_dir=data_dir, models_dir=models_dir)
    for name, path in paths.items():
        print(f"Saved {name}: {path}")


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="bdsp_training_daily",
    description="On-demand model retraining: naive + linear + XGBoost → /opt/airflow/models/",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=pendulum.timezone("Europe/Zurich")),
    catchup=False,
    default_args=default_args,
    tags=["bdsp", "ml", "phase-3"],
) as dag:

    run_training_task = PythonOperator(
        task_id="run_training",
        python_callable=_run_training,
    )

    run_load_training_task = PythonOperator(
        task_id="train_load_model",
        python_callable=_run_load_training,
    )

    # Both training tasks are independent – Model B (EPEX) and Model A (load)
    # use separate parquet files and can train in parallel.
