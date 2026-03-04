"""
DAG: bdsp_training_daily

Daily model retraining pipeline for BigDataSmallPrice.
Runs at 08:00 UTC – one hour after the feature export DAG (bdsp_feature_daily
at 07:00 UTC) to ensure fresh parquet files are available.

Task graph:
  run_training → single task: train naive + linear + XGBoost on latest parquet
                 and save to /opt/airflow/models/

The trained XGBoost model is automatically picked up by the FastAPI /api/predict
and /api/forecast endpoints on their next request (lazy model cache reload).
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
    "retries": 1,
    "retry_delay": __import__("datetime").timedelta(minutes=10),
}

# ─── Task functions ───────────────────────────────────────────────────────────


def _run_training(**ctx) -> None:
    from modelling.train import run_training

    data_dir   = os.environ.get("BDSP_EXPORT_DIR",  "/opt/airflow/data/")
    models_dir = os.environ.get("BDSP_MODELS_DIR",  "/opt/airflow/models/")

    paths = run_training(data_dir=data_dir, models_dir=models_dir)
    for name, path in paths.items():
        print(f"Saved {name}: {path}")


def _run_load_training(**ctx) -> None:
    from modelling.train import run_load_training

    data_dir   = os.environ.get("BDSP_EXPORT_DIR",  "/opt/airflow/data/")
    models_dir = os.environ.get("BDSP_MODELS_DIR",  "/opt/airflow/models/")

    paths = run_load_training(data_dir=data_dir, models_dir=models_dir)
    for name, path in paths.items():
        print(f"Saved {name}: {path}")


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="bdsp_training_daily",
    description="Daily model retraining: naive + linear + XGBoost → /opt/airflow/models/",
    schedule="0 8 * * *",          # 08:00 UTC, one hour after feature export
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
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
