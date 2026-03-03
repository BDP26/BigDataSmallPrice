"""
DAG: bdsp_feature_daily

Daily feature engineering and export pipeline for BigDataSmallPrice.
Runs at 07:00 UTC – one hour after the ETL DAG (06:00 UTC) to ensure
fresh data is available in TimescaleDB before the export runs.

Task graph:
  run_feature_export   (single task – queries training_features view,
                        splits data, writes parquet files to /opt/airflow/data/)
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

# ─── Task function ────────────────────────────────────────────────────────────


def _run_feature_export(**ctx) -> None:
    from processing.export_pipeline import run_export

    output_dir = os.environ.get("BDSP_EXPORT_DIR", "/opt/airflow/data/")
    test_ratio = float(os.environ.get("BDSP_TEST_RATIO", "0.2"))
    paths = run_export(output_dir=output_dir, test_ratio=test_ratio)
    for name, path in paths.items():
        print(f"  {name}: {path}")


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="bdsp_feature_daily",
    description="Daily feature engineering & parquet export for ML training",
    schedule="0 7 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["bdsp", "features", "phase-2"],
) as dag:

    run_feature_export = PythonOperator(
        task_id="run_feature_export",
        python_callable=_run_feature_export,
    )
