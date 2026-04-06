"""
DAG: bdsp_training_daily

On-demand model retraining pipeline for BigDataSmallPrice.
Schedule is None – the DAG is triggered manually via the Airflow UI or the
admin dashboard POST /api/training/trigger endpoint.

Task graph (all five tasks run in parallel – independent datasets/models):
  run_training          → naive + linear + XGBoost  (Model B, EPEX price)
  train_load_model      → naive + linear + XGBoost  (Model A, net load)
  train_lstm_energy     → LSTM                       (Model B, EPEX price)
  train_lstm_load       → LSTM                       (Model A, net load)
  train_transformer_load→ Transformer                (Model A, net load)

Sklearn models save:
  - <name>_<YYYYMMDD>.joblib  model files
  - metrics_<YYYYMMDD>.json   evaluation metrics

Sequence models (LSTM / Transformer) save:
  - <name>_<YYYYMMDD>.pt        model bundle (state_dict + scaler + config)
  - <name>_loss_<YYYYMMDD>.json per-epoch validation MSE history
  - metrics_<name>_<YYYYMMDD>.json evaluation metrics

All trained models are automatically picked up by the FastAPI /api/predict
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


def _run_lstm_energy_training(**ctx) -> None:
    from modelling.train import run_lstm_energy_training

    data_dir   = os.environ.get(
        "BDSP_ENERGY_EXPORT_DIR",
        os.environ.get("BDSP_EXPORT_DIR", "/opt/airflow/data/energy/"),
    )
    models_dir = os.environ.get("BDSP_MODELS_DIR", "/opt/airflow/models/")

    paths = run_lstm_energy_training(data_dir=data_dir, models_dir=models_dir)
    for name, path in paths.items():
        print(f"Saved {name}: {path}")


def _run_lstm_load_training(**ctx) -> None:
    from modelling.train import run_lstm_load_training

    data_dir   = os.environ.get(
        "BDSP_LOAD_EXPORT_DIR",
        os.environ.get("BDSP_EXPORT_DIR", "/opt/airflow/data/load/"),
    )
    models_dir = os.environ.get("BDSP_MODELS_DIR", "/opt/airflow/models/")

    paths = run_lstm_load_training(data_dir=data_dir, models_dir=models_dir)
    for name, path in paths.items():
        print(f"Saved {name}: {path}")


def _run_transformer_load_training(**ctx) -> None:
    from modelling.train import run_transformer_load_training

    data_dir   = os.environ.get(
        "BDSP_LOAD_EXPORT_DIR",
        os.environ.get("BDSP_EXPORT_DIR", "/opt/airflow/data/load/"),
    )
    models_dir = os.environ.get("BDSP_MODELS_DIR", "/opt/airflow/models/")

    paths = run_transformer_load_training(data_dir=data_dir, models_dir=models_dir)
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

    train_lstm_energy_task = PythonOperator(
        task_id="train_lstm_energy",
        python_callable=_run_lstm_energy_training,
    )

    train_lstm_load_task = PythonOperator(
        task_id="train_lstm_load",
        python_callable=_run_lstm_load_training,
    )

    train_transformer_load_task = PythonOperator(
        task_id="train_transformer_load",
        python_callable=_run_transformer_load_training,
    )

    # All five tasks are independent – different models, different parquet files.
    # Airflow runs them in parallel up to the LocalExecutor worker limit.
