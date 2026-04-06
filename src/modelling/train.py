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
    """Linear regression baseline (NaN → median imputation, then 0 fallback)."""
    model = LinearRegression()
    X_filled = X_train.fillna(X_train.median(numeric_only=True)).fillna(0)
    model.fit(X_filled, y_train.values.ravel())
    return model


def train_xgboost(
    X_train: pd.DataFrame,
    y_train: pd.DataFrame,
    X_val: pd.DataFrame | None = None,
    y_val: pd.DataFrame | None = None,
) -> XGBRegressor:
    """XGBoost regressor (handles NaN natively).

    When *X_val* / *y_val* are provided, early stopping with
    ``early_stopping_rounds=20`` is activated on the validation set.

    Regularisation notes:
    - ``max_depth=5`` (down from 6): shallower trees generalise better to
      unseen price regimes (e.g. post-energy-crisis normalisation).
    - ``reg_lambda=2.0``: L2 weight penalty reduces sensitivity to extreme
      lag-price features during low/negative price hours.
    - ``min_child_weight=5``: requires at least 5 samples per leaf, preventing
      the model from over-specialising on rare price spikes.
    """
    use_early_stopping = X_val is not None and y_val is not None
    model = XGBRegressor(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_lambda=2.0,
        reg_alpha=0.1,
        random_state=42,
        n_jobs=-1,
        verbosity=0,
        early_stopping_rounds=20 if use_early_stopping else None,
    )
    if use_early_stopping:
        X_val_filled = X_val.fillna(X_val.median(numeric_only=True))
        model.fit(
            X_train,
            y_train.values.ravel(),
            eval_set=[(X_val_filled, y_val.values.ravel())],
            verbose=False,
        )
    else:
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


def train_load_model(
    X_train: pd.DataFrame,
    y_train: pd.DataFrame,
    X_val: pd.DataFrame | None = None,
    y_val: pd.DataFrame | None = None,
) -> XGBRegressor:
    """
    Train XGBoost model for grid-load forecasting (Model A).

    Interface is identical to ``train_xgboost()`` so the same evaluate.py
    helpers work for both models.

    Target: ``net_load_kwh`` (= bruttolastgang − PV feed-in, kWh)
    Quality goal: MAPE < 8 % on the test set (req.md).

    When *X_val* / *y_val* are provided, early stopping with
    ``early_stopping_rounds=20`` is activated on the validation set.
    """
    use_early_stopping = X_val is not None and y_val is not None
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
        early_stopping_rounds=20 if use_early_stopping else None,
    )
    if use_early_stopping:
        X_val_filled = X_val.fillna(X_val.median(numeric_only=True))
        model.fit(
            X_train,
            y_train.values.ravel(),
            eval_set=[(X_val_filled, y_val.values.ravel())],
            verbose=False,
        )
    else:
        model.fit(X_train, y_train.values.ravel())
    return model


def run_load_training(
    data_dir: str = "data/load/",
    models_dir: str = "models/",
) -> dict[str, Path]:
    """
    End-to-end training pipeline for Model A (grid-load forecasting).

    1. Read ``X_train.parquet``, ``y_train.parquet`` (+ optional val/test)
       from *data_dir*.
    2. Train Naive + Linear baselines and the XGBoost load model.
    3. Evaluate all models on the test set (if available) and save metrics.
    4. Emit a UserWarning when XGBoost test MAPE exceeds 8% (req.md).
    5. Serialize all models to *models_dir*.

    Args:
        data_dir:   Directory containing parquet files from run_load_export().
        models_dir: Directory for serialised model files.

    Returns:
        Dict mapping model name to the saved Path.

    Raises:
        FileNotFoundError: If the training parquet files are not found.
    """
    from modelling.evaluate import check_load_quality, evaluate_all, save_metrics

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

    # Optional validation + test splits
    xv_path = data_path / "X_val.parquet"
    yv_path = data_path / "y_val.parquet"
    xt_path = data_path / "X_test.parquet"
    yt_path = data_path / "y_test.parquet"
    X_val = pd.read_parquet(xv_path) if xv_path.exists() else None
    y_val = pd.read_parquet(yv_path) if yv_path.exists() else None
    X_test = pd.read_parquet(xt_path) if xt_path.exists() else None
    y_test = pd.read_parquet(yt_path) if yt_path.exists() else None

    # Train all models
    naive = train_naive(X_train, y_train)
    linear = train_linear(X_train, y_train)
    load_model = train_load_model(X_train, y_train, X_val=X_val, y_val=y_val)

    paths: dict[str, Path] = {}
    paths["naive_load"] = save_model(naive, "naive_load", models_dir)
    paths["linear_load"] = save_model(linear, "linear_load", models_dir)
    paths["model_load"] = save_model(load_model, "model_load", models_dir)
    print(f"Saved naive_load:   {paths['naive_load']}")
    print(f"Saved linear_load:  {paths['linear_load']}")
    print(f"Saved model_load:   {paths['model_load']}")
    if X_val is not None and getattr(load_model, "evals_result_", None):
        import json as _json
        ts_loss = datetime.now(timezone.utc).strftime("%Y%m%d")
        loss_path = Path(models_dir) / f"model_load_loss_{ts_loss}.json"
        with loss_path.open("w") as _fh:
            _json.dump(load_model.evals_result_, _fh, indent=2)
        print(f"Saved load model loss history: {loss_path}")

    # Evaluate on test set when available
    if X_test is not None and y_test is not None:
        print("\n── Load model evaluation on test set ──")
        metrics = evaluate_all(
            {"naive_load": naive, "linear_load": linear, "model_load": load_model},
            X_test,
            y_test,
        )
        save_metrics(metrics, "metrics_load", models_dir)
        check_load_quality(metrics)

    return paths


# ── Orchestration ─────────────────────────────────────────────────────────────


def run_training(
    data_dir: str = "data/energy/",
    models_dir: str = "models/",
) -> dict[str, Path]:
    """
    End-to-end training pipeline for Model B (EPEX energy price forecasting).

    1. Read ``X_train.parquet``, ``y_train.parquet`` (+ optional val/test)
       from *data_dir*.
    2. Train naive, linear, and XGBoost models.
       XGBoost uses early stopping when a validation set is available.
    3. Evaluate all models on the test set (if available) and save metrics.
    4. Serialize each model to *models_dir*.

    Args:
        data_dir:   Directory containing parquet files from the export pipeline.
        models_dir: Directory for serialised model files.

    Returns:
        Dict mapping model name to the saved Path.

    Raises:
        FileNotFoundError: If training parquet files are not found in *data_dir*.
    """
    from modelling.evaluate import evaluate_all, save_metrics

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

    # Optional validation + test splits
    xv_path = data_path / "X_val.parquet"
    yv_path = data_path / "y_val.parquet"
    xt_path = data_path / "X_test.parquet"
    yt_path = data_path / "y_test.parquet"
    X_val = pd.read_parquet(xv_path) if xv_path.exists() else None
    y_val = pd.read_parquet(yv_path) if yv_path.exists() else None
    X_test = pd.read_parquet(xt_path) if xt_path.exists() else None
    y_test = pd.read_parquet(yt_path) if yt_path.exists() else None

    paths: dict[str, Path] = {}

    naive = train_naive(X_train, y_train)
    paths["naive"] = save_model(naive, "naive", models_dir)
    print(f"Saved naive model:   {paths['naive']}")

    linear = train_linear(X_train, y_train)
    paths["linear"] = save_model(linear, "linear", models_dir)
    print(f"Saved linear model:  {paths['linear']}")

    xgb = train_xgboost(X_train, y_train, X_val=X_val, y_val=y_val)
    paths["xgb"] = save_model(xgb, "xgb", models_dir)
    print(f"Saved XGBoost model: {paths['xgb']}")
    if X_val is not None and getattr(xgb, "evals_result_", None):
        import json as _json
        ts_loss = datetime.now(timezone.utc).strftime("%Y%m%d")
        loss_path = Path(models_dir) / f"xgb_loss_{ts_loss}.json"
        with loss_path.open("w") as _fh:
            _json.dump(xgb.evals_result_, _fh, indent=2)
        print(f"Saved XGBoost loss history: {loss_path}")

    # Evaluate on test set when available
    if X_test is not None and y_test is not None:
        print("\n── Energy model evaluation on test set ──")
        metrics = evaluate_all(
            {"naive": naive, "linear": linear, "xgb": xgb},
            X_test,
            y_test,
        )
        save_metrics(metrics, "metrics", models_dir)

    return paths


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _data_dir = os.environ.get("BDSP_ENERGY_EXPORT_DIR", "data/energy/")
    _models_dir = os.environ.get("BDSP_MODELS_DIR", "models/")
    run_training(data_dir=_data_dir, models_dir=_models_dir)
