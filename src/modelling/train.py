"""
Model training for BigDataSmallPrice – Phase 3.

Trains three models on the exported parquet files:
  1. Naive baseline  (DummyRegressor – mean strategy)
  2. Linear baseline (LinearRegression)
  3. XGBoost         (XGBRegressor)

Saves each model as a timestamped joblib file under models/.

Usage (standalone):
    python src/modelling/train.py

Usage (from Airflow / Python):
    from modelling.train import run_training
    run_training(data_dir="/opt/airflow/data/", models_dir="/opt/airflow/models/")
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sklearn.dummy import DummyRegressor
from sklearn.linear_model import LinearRegression
from xgboost import XGBRegressor


# ── Training functions ────────────────────────────────────────────────────────


def train_naive(X_train: pd.DataFrame, y_train: pd.DataFrame) -> DummyRegressor:
    """Naive baseline: always predict the mean of the training target."""
    model = DummyRegressor(strategy="mean")
    model.fit(X_train, y_train.values.ravel())
    return model


def train_linear(X_train: pd.DataFrame, y_train: pd.DataFrame) -> LinearRegression:
    """Linear regression baseline (NaN → median imputation)."""
    model = LinearRegression()
    model.fit(X_train.fillna(X_train.median(numeric_only=True)), y_train.values.ravel())
    return model


def train_xgboost(X_train: pd.DataFrame, y_train: pd.DataFrame) -> XGBRegressor:
    """XGBoost regressor (handles NaN natively)."""
    model = XGBRegressor(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
        verbosity=0,
    )
    model.fit(X_train, y_train.values.ravel())
    return model


# ── Serialisation ─────────────────────────────────────────────────────────────


def save_model(model: Any, name: str, models_dir: str = "models/") -> Path:
    """
    Serialize *model* to ``<models_dir>/<name>_<YYYYMMDD>.joblib``.

    Args:
        model:      Any sklearn-compatible model with a ``predict`` method.
        name:       Short identifier, e.g. ``"xgb"``, ``"naive"``.
        models_dir: Target directory (created if missing).

    Returns:
        Path to the written file.
    """
    out = Path(models_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = out / f"{name}_{ts}.joblib"
    joblib.dump(model, path)
    return path


# ── Orchestration ─────────────────────────────────────────────────────────────


def run_training(
    data_dir: str = "data/",
    models_dir: str = "models/",
) -> dict[str, Path]:
    """
    End-to-end training pipeline:

    1. Locate the latest ``X_train_*.parquet`` and ``y_train_*.parquet``.
    2. Train naive, linear, and XGBoost models.
    3. Serialize each model to *models_dir*.

    Args:
        data_dir:   Directory containing parquet files from the export pipeline.
        models_dir: Directory for serialised model files.

    Returns:
        Dict mapping model name to the saved Path.

    Raises:
        FileNotFoundError: If no training parquet files are found in *data_dir*.
    """
    data_path = Path(data_dir)
    x_files = sorted(data_path.glob("X_train_*.parquet"))
    y_files = sorted(data_path.glob("y_train_*.parquet"))
    if not x_files or not y_files:
        raise FileNotFoundError(
            f"No training parquet files found in {data_dir!r}. "
            "Run the export pipeline first (src/processing/export_pipeline.py)."
        )
    X_train = pd.read_parquet(x_files[-1])
    y_train = pd.read_parquet(y_files[-1])

    paths: dict[str, Path] = {}

    naive = train_naive(X_train, y_train)
    paths["naive"] = save_model(naive, "naive", models_dir)
    print(f"Saved naive model:   {paths['naive']}")

    linear = train_linear(X_train, y_train)
    paths["linear"] = save_model(linear, "linear", models_dir)
    print(f"Saved linear model:  {paths['linear']}")

    xgb = train_xgboost(X_train, y_train)
    paths["xgb"] = save_model(xgb, "xgb", models_dir)
    print(f"Saved XGBoost model: {paths['xgb']}")

    return paths


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _data_dir = os.environ.get("BDSP_EXPORT_DIR", "data/")
    _models_dir = os.environ.get("BDSP_MODELS_DIR", "models/")
    run_training(data_dir=_data_dir, models_dir=_models_dir)
