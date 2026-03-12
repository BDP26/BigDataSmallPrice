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


# ── Model A: Load forecasting ─────────────────────────────────────────────────


def train_load_model(X_train: pd.DataFrame, y_train: pd.DataFrame) -> XGBRegressor:
    """
    Train XGBoost model for grid-load forecasting (Model A).

    Interface is identical to ``train_xgboost()`` so the same evaluate.py
    helpers work for both models.

    Target: ``net_load_kwh`` (= bruttolastgang − PV feed-in, kWh)
    Quality goal: MAPE < 8 % on the test set (req.md).
    """
    model = XGBRegressor(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=7,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=3,
        random_state=42,
        n_jobs=-1,
        verbosity=0,
    )
    model.fit(X_train, y_train.values.ravel())
    return model


def run_load_training(
    data_dir: str = "data/load/",
    models_dir: str = "models/",
) -> dict[str, Path]:
    """
    End-to-end training pipeline for Model A (grid-load forecasting).

    1. Read ``X_train.parquet`` and ``y_train.parquet`` from *data_dir*.
    2. Train the XGBoost load model.
    3. Serialize the model to *models_dir* as ``model_load_<YYYYMMDD>.joblib``.

    Args:
        data_dir:   Directory containing parquet files from run_load_export().
        models_dir: Directory for serialised model files.

    Returns:
        Dict mapping model name to the saved Path.

    Raises:
        FileNotFoundError: If the training parquet files are not found.
    """
    data_path = Path(data_dir)
    x_path = data_path / "X_train.parquet"
    y_path = data_path / "y_train.parquet"
    if not x_path.exists() or not y_path.exists():
        raise FileNotFoundError(
            f"Load training parquet files not found in {data_dir!r}. "
            "Run run_load_export() first."
        )
    X_train = pd.read_parquet(x_path)
    y_train = pd.read_parquet(y_path)

    model = train_load_model(X_train, y_train)
    path = save_model(model, "model_load", models_dir)
    print(f"Saved load model: {path}")
    return {"model_load": path}


# ── Orchestration ─────────────────────────────────────────────────────────────


def run_training(
    data_dir: str = "data/energy/",
    models_dir: str = "models/",
) -> dict[str, Path]:
    """
    End-to-end training pipeline:

    1. Read ``X_train.parquet`` and ``y_train.parquet`` from *data_dir*.
    2. Train naive, linear, and XGBoost models.
    3. Serialize each model to *models_dir*.

    Args:
        data_dir:   Directory containing parquet files from the export pipeline.
        models_dir: Directory for serialised model files.

    Returns:
        Dict mapping model name to the saved Path.

    Raises:
        FileNotFoundError: If training parquet files are not found in *data_dir*.
    """
    data_path = Path(data_dir)
    x_path = data_path / "X_train.parquet"
    y_path = data_path / "y_train.parquet"
    if not x_path.exists() or not y_path.exists():
        raise FileNotFoundError(
            f"Training parquet files not found in {data_dir!r}. "
            "Run the export pipeline first (src/processing/export_pipeline.py)."
        )
    X_train = pd.read_parquet(x_path)
    y_train = pd.read_parquet(y_path)

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
    _data_dir = os.environ.get("BDSP_ENERGY_EXPORT_DIR", "data/energy/")
    _models_dir = os.environ.get("BDSP_MODELS_DIR", "models/")
    run_training(data_dir=_data_dir, models_dir=_models_dir)
