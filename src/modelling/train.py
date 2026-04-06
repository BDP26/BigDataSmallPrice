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


# ── Sequence model training (LSTM / Transformer) ─────────────────────────────


def _fit_scalers(
    X: "pd.DataFrame",
    y_col: "pd.DataFrame",
) -> "tuple[np.ndarray, np.ndarray, float, float]":
    """
    Compute z-score parameters from training data.

    Returns:
        (X_mean, X_scale, y_mean, y_scale) – all computed on *X* / *y_col*.
        Scale values that are near-zero are clamped to 1.0 to avoid division
        by zero for constant features.
    """
    import numpy as np  # noqa: PLC0415

    X_np = X.values.astype("float32")
    X_mean = np.nanmean(X_np, axis=0)
    X_scale = np.nanstd(X_np, axis=0)
    X_scale[X_scale < 1e-8] = 1.0

    y_np = y_col.values.ravel().astype("float32")
    y_mean = float(np.nanmean(y_np))
    y_scale = float(np.nanstd(y_np))
    if y_scale < 1e-8:
        y_scale = 1.0

    return X_mean, X_scale, y_mean, y_scale


def run_lstm_load_training(
    data_dir: str = "data/load/",
    models_dir: str = "models/",
    lookback: int = 96,
    max_train_seqs: int = 30_000,
    epochs: int = 30,
    batch_size: int = 512,
) -> dict[str, Path]:
    """
    End-to-end LSTM training pipeline for Model A (Winterthur net-load forecasting).

    Sequence design:
      lookback = 96 steps at 15-min resolution = 24 h of history per input window.
      Training rows are capped at *max_train_seqs* most-recent sequences so that
      the training run completes in a few minutes on a CPU-only environment.

    Steps:
      1. Load parquet splits from *data_dir*.
      2. Fit z-score scalers on the training split.
      3. Build SequenceDataset (lazy sliding-window).
      4. Train LSTMModel with early stopping on the validation MSE.
      5. Evaluate on the test split and save metrics to ``metrics_lstm_load_<date>.json``.
      6. Save model bundle to ``lstm_load_<date>.pt``.

    Args:
        data_dir:       Directory containing parquet files from run_load_export().
        models_dir:     Output directory for model files and metrics.
        lookback:       Sliding-window length in time steps (default 96 = 24 h).
        max_train_seqs: Maximum number of training sequences (most-recent rows).
        epochs:         Maximum training epochs.
        batch_size:     Mini-batch size.

    Returns:
        Dict mapping ``"lstm_load"`` to the saved ``.pt`` path.
    """
    import numpy as np  # noqa: PLC0415

    from modelling.evaluate import check_load_quality, compute_metrics, save_metrics  # noqa: PLC0415
    from modelling.sequence_models import (  # noqa: PLC0415
        LSTMModel,
        predict_from_sequences,
        save_torch_model,
        train_sequence_model,
    )

    data_path = Path(data_dir)
    for fname in ("X_train.parquet", "y_train.parquet"):
        if not (data_path / fname).exists():
            raise FileNotFoundError(
                f"{fname} not found in {data_dir!r}. Run run_load_export() first."
            )

    X_train = pd.read_parquet(data_path / "X_train.parquet").fillna(0)
    y_train = pd.read_parquet(data_path / "y_train.parquet")
    X_val   = pd.read_parquet(data_path / "X_val.parquet").fillna(0) if (data_path / "X_val.parquet").exists() else None
    y_val   = pd.read_parquet(data_path / "y_val.parquet") if (data_path / "y_val.parquet").exists() else None
    X_test  = pd.read_parquet(data_path / "X_test.parquet").fillna(0) if (data_path / "X_test.parquet").exists() else None
    y_test  = pd.read_parquet(data_path / "y_test.parquet") if (data_path / "y_test.parquet").exists() else None

    feature_cols = list(X_train.columns)
    X_mean, X_scale, y_mean, y_scale = _fit_scalers(X_train, y_train)

    X_tr_np = X_train.values.astype("float32")
    y_tr_np = y_train.values.ravel().astype("float32")
    X_tr_sc = (X_tr_np - X_mean) / X_scale
    y_tr_sc = (y_tr_np - y_mean) / y_scale

    # Validation context: prepend last `lookback` train rows so val seq 0 has context
    X_val_ctx_sc: np.ndarray | None = None
    y_val_ctx_sc: np.ndarray | None = None
    if X_val is not None and y_val is not None:
        X_val_np = X_val.values.astype("float32")
        y_val_np = y_val.values.ravel().astype("float32")
        X_val_ctx = np.concatenate([X_tr_np[-lookback:], X_val_np], axis=0)
        y_val_ctx = np.concatenate([y_tr_np[-lookback:], y_val_np])
        X_val_ctx_sc = (X_val_ctx - X_mean) / X_scale
        y_val_ctx_sc = (y_val_ctx - y_mean) / y_scale

    model = LSTMModel(input_size=len(feature_cols), hidden_size=64, num_layers=2, dropout=0.2)
    print(f"Training LSTM (Model A load), lookback={lookback}, max_seqs={max_train_seqs}")

    model, val_losses = train_sequence_model(
        model, X_tr_sc, y_tr_sc,
        lookback=lookback,
        X_val=X_val_ctx_sc, y_val=y_val_ctx_sc,
        epochs=epochs, batch_size=batch_size,
        max_train_seqs=max_train_seqs,
    )

    # Evaluate on test set
    paths: dict[str, Path] = {}
    if X_test is not None and y_test is not None:
        X_test_np = X_test.values.astype("float32")
        y_test_np = y_test.values.ravel().astype("float32")
        X_val_np_last = (X_val.values.astype("float32") if X_val is not None else X_tr_np)
        X_test_ctx = np.concatenate([X_val_np_last[-lookback:], X_test_np], axis=0)
        X_test_ctx_sc = (X_test_ctx - X_mean) / X_scale
        preds_sc = predict_from_sequences(model, X_test_ctx_sc, lookback)
        preds = preds_sc * y_scale + y_mean

        print("\n── LSTM Load evaluation on test set ──")
        metrics = {"lstm_load": compute_metrics(y_test_np, preds)}
        save_metrics(metrics, "metrics_lstm_load", models_dir)
        m = metrics["lstm_load"]
        print(f"lstm_load  MAE={m['mae']:.1f}  RMSE={m['rmse']:.1f}  MAPE={m['mape']:.2f}%")
        check_load_quality({"model_load": m})

    config = {
        "model_type": "lstm",
        "input_size": len(feature_cols),
        "hidden_size": 64,
        "num_layers": 2,
        "dropout": 0.2,
    }
    paths["lstm_load"] = save_torch_model(
        model, config, X_mean, X_scale, y_mean, y_scale,
        feature_cols, lookback, name="lstm_load", models_dir=models_dir,
        val_losses=val_losses,
    )
    print(f"Saved lstm_load: {paths['lstm_load']}")
    return paths


def run_lstm_energy_training(
    data_dir: str = "data/energy/",
    models_dir: str = "models/",
    lookback: int = 48,
    epochs: int = 30,
    batch_size: int = 256,
) -> dict[str, Path]:
    """
    End-to-end LSTM training pipeline for Model B (EPEX day-ahead price).

    Sequence design:
      lookback = 48 steps at hourly resolution = 48 h (2 days) of history.
      The full training set is used (only ~13 k rows, so no subsampling needed).

    Steps:
      1. Load parquet splits from *data_dir*.
      2. Fit z-score scalers on the training split.
      3. Train LSTMModel with early stopping on the validation MSE.
      4. Evaluate on the test split and save metrics to ``metrics_lstm_energy_<date>.json``.
      5. Save model bundle to ``lstm_energy_<date>.pt``.

    Args:
        data_dir:   Directory containing parquet files from run_export().
        models_dir: Output directory for model files and metrics.
        lookback:   Sliding-window length in time steps (default 48 = 2 days).
        epochs:     Maximum training epochs.
        batch_size: Mini-batch size.

    Returns:
        Dict mapping ``"lstm_energy"`` to the saved ``.pt`` path.
    """
    import numpy as np  # noqa: PLC0415

    from modelling.evaluate import compute_metrics, save_metrics  # noqa: PLC0415
    from modelling.sequence_models import (  # noqa: PLC0415
        LSTMModel,
        predict_from_sequences,
        save_torch_model,
        train_sequence_model,
    )

    data_path = Path(data_dir)
    for fname in ("X_train.parquet", "y_train.parquet"):
        if not (data_path / fname).exists():
            raise FileNotFoundError(
                f"{fname} not found in {data_dir!r}. Run run_export() first."
            )

    X_train = pd.read_parquet(data_path / "X_train.parquet").fillna(0)
    y_train = pd.read_parquet(data_path / "y_train.parquet")
    X_val   = pd.read_parquet(data_path / "X_val.parquet").fillna(0) if (data_path / "X_val.parquet").exists() else None
    y_val   = pd.read_parquet(data_path / "y_val.parquet") if (data_path / "y_val.parquet").exists() else None
    X_test  = pd.read_parquet(data_path / "X_test.parquet").fillna(0) if (data_path / "X_test.parquet").exists() else None
    y_test  = pd.read_parquet(data_path / "y_test.parquet") if (data_path / "y_test.parquet").exists() else None

    feature_cols = list(X_train.columns)
    X_mean, X_scale, y_mean, y_scale = _fit_scalers(X_train, y_train)

    X_tr_np = X_train.values.astype("float32")
    y_tr_np = y_train.values.ravel().astype("float32")
    X_tr_sc = (X_tr_np - X_mean) / X_scale
    y_tr_sc = (y_tr_np - y_mean) / y_scale

    X_val_ctx_sc: np.ndarray | None = None
    y_val_ctx_sc: np.ndarray | None = None
    if X_val is not None and y_val is not None:
        X_val_np = X_val.values.astype("float32")
        y_val_np = y_val.values.ravel().astype("float32")
        X_val_ctx = np.concatenate([X_tr_np[-lookback:], X_val_np], axis=0)
        y_val_ctx = np.concatenate([y_tr_np[-lookback:], y_val_np])
        X_val_ctx_sc = (X_val_ctx - X_mean) / X_scale
        y_val_ctx_sc = (y_val_ctx - y_mean) / y_scale

    model = LSTMModel(input_size=len(feature_cols), hidden_size=64, num_layers=2, dropout=0.2)
    print(f"Training LSTM (Model B EPEX), lookback={lookback}")

    model, val_losses = train_sequence_model(
        model, X_tr_sc, y_tr_sc,
        lookback=lookback,
        X_val=X_val_ctx_sc, y_val=y_val_ctx_sc,
        epochs=epochs, batch_size=batch_size,
        max_train_seqs=None,  # use all data (only ~13 k rows)
    )

    paths: dict[str, Path] = {}
    if X_test is not None and y_test is not None:
        X_test_np = X_test.values.astype("float32")
        y_test_np = y_test.values.ravel().astype("float32")
        X_val_np_last = (X_val.values.astype("float32") if X_val is not None else X_tr_np)
        X_test_ctx = np.concatenate([X_val_np_last[-lookback:], X_test_np], axis=0)
        X_test_ctx_sc = (X_test_ctx - X_mean) / X_scale
        preds_sc = predict_from_sequences(model, X_test_ctx_sc, lookback)
        preds = preds_sc * y_scale + y_mean

        print("\n── LSTM Energy evaluation on test set ──")
        metrics = {"lstm_energy": compute_metrics(y_test_np, preds)}
        save_metrics(metrics, "metrics_lstm_energy", models_dir)
        m = metrics["lstm_energy"]
        print(f"lstm_energy  MAE={m['mae']:.3f}  RMSE={m['rmse']:.3f}  MAPE={m['mape']:.2f}%")

    config = {
        "model_type": "lstm",
        "input_size": len(feature_cols),
        "hidden_size": 64,
        "num_layers": 2,
        "dropout": 0.2,
    }
    paths["lstm_energy"] = save_torch_model(
        model, config, X_mean, X_scale, y_mean, y_scale,
        feature_cols, lookback, name="lstm_energy", models_dir=models_dir,
        val_losses=val_losses,
    )
    print(f"Saved lstm_energy: {paths['lstm_energy']}")
    return paths


def run_transformer_load_training(
    data_dir: str = "data/load/",
    models_dir: str = "models/",
    lookback: int = 96,
    max_train_seqs: int = 30_000,
    epochs: int = 30,
    batch_size: int = 512,
) -> dict[str, Path]:
    """
    End-to-end Transformer training pipeline for Model A (Winterthur net-load).

    Uses a compact encoder-only Transformer (Pre-LN, 2 layers, d_model=64, 4 heads)
    with the same sliding-window design as the LSTM variant.  The same
    *max_train_seqs* subsampling strategy is applied to keep CPU training time
    manageable.

    Steps:
      1. Load parquet splits from *data_dir*.
      2. Fit z-score scalers on the training split.
      3. Train TransformerModel with early stopping on the validation MSE.
      4. Evaluate on the test split and save ``metrics_transformer_load_<date>.json``.
      5. Save model bundle to ``transformer_load_<date>.pt``.

    Args:
        data_dir:       Directory containing parquet files from run_load_export().
        models_dir:     Output directory for model files and metrics.
        lookback:       Sliding-window length (default 96 = 24 h at 15-min).
        max_train_seqs: Maximum number of training sequences (most-recent rows).
        epochs:         Maximum training epochs.
        batch_size:     Mini-batch size.

    Returns:
        Dict mapping ``"transformer_load"`` to the saved ``.pt`` path.
    """
    import numpy as np  # noqa: PLC0415

    from modelling.evaluate import check_load_quality, compute_metrics, save_metrics  # noqa: PLC0415
    from modelling.sequence_models import (  # noqa: PLC0415
        TransformerModel,
        predict_from_sequences,
        save_torch_model,
        train_sequence_model,
    )

    data_path = Path(data_dir)
    for fname in ("X_train.parquet", "y_train.parquet"):
        if not (data_path / fname).exists():
            raise FileNotFoundError(
                f"{fname} not found in {data_dir!r}. Run run_load_export() first."
            )

    X_train = pd.read_parquet(data_path / "X_train.parquet").fillna(0)
    y_train = pd.read_parquet(data_path / "y_train.parquet")
    X_val   = pd.read_parquet(data_path / "X_val.parquet").fillna(0) if (data_path / "X_val.parquet").exists() else None
    y_val   = pd.read_parquet(data_path / "y_val.parquet") if (data_path / "y_val.parquet").exists() else None
    X_test  = pd.read_parquet(data_path / "X_test.parquet").fillna(0) if (data_path / "X_test.parquet").exists() else None
    y_test  = pd.read_parquet(data_path / "y_test.parquet") if (data_path / "y_test.parquet").exists() else None

    feature_cols = list(X_train.columns)
    X_mean, X_scale, y_mean, y_scale = _fit_scalers(X_train, y_train)

    X_tr_np = X_train.values.astype("float32")
    y_tr_np = y_train.values.ravel().astype("float32")
    X_tr_sc = (X_tr_np - X_mean) / X_scale
    y_tr_sc = (y_tr_np - y_mean) / y_scale

    X_val_ctx_sc: np.ndarray | None = None
    y_val_ctx_sc: np.ndarray | None = None
    if X_val is not None and y_val is not None:
        X_val_np = X_val.values.astype("float32")
        y_val_np = y_val.values.ravel().astype("float32")
        X_val_ctx = np.concatenate([X_tr_np[-lookback:], X_val_np], axis=0)
        y_val_ctx = np.concatenate([y_tr_np[-lookback:], y_val_np])
        X_val_ctx_sc = (X_val_ctx - X_mean) / X_scale
        y_val_ctx_sc = (y_val_ctx - y_mean) / y_scale

    model = TransformerModel(input_size=len(feature_cols), d_model=64, nhead=4, num_layers=2, dropout=0.1)
    print(f"Training Transformer (Model A load), lookback={lookback}, max_seqs={max_train_seqs}")

    model, val_losses = train_sequence_model(
        model, X_tr_sc, y_tr_sc,
        lookback=lookback,
        X_val=X_val_ctx_sc, y_val=y_val_ctx_sc,
        epochs=epochs, batch_size=batch_size,
        max_train_seqs=max_train_seqs,
    )

    paths: dict[str, Path] = {}
    if X_test is not None and y_test is not None:
        X_test_np = X_test.values.astype("float32")
        y_test_np = y_test.values.ravel().astype("float32")
        X_val_np_last = (X_val.values.astype("float32") if X_val is not None else X_tr_np)
        X_test_ctx = np.concatenate([X_val_np_last[-lookback:], X_test_np], axis=0)
        X_test_ctx_sc = (X_test_ctx - X_mean) / X_scale
        preds_sc = predict_from_sequences(model, X_test_ctx_sc, lookback)
        preds = preds_sc * y_scale + y_mean

        print("\n── Transformer Load evaluation on test set ──")
        metrics = {"transformer_load": compute_metrics(y_test_np, preds)}
        save_metrics(metrics, "metrics_transformer_load", models_dir)
        m = metrics["transformer_load"]
        print(f"transformer_load  MAE={m['mae']:.1f}  RMSE={m['rmse']:.1f}  MAPE={m['mape']:.2f}%")
        check_load_quality({"model_load": m})

    config = {
        "model_type": "transformer",
        "input_size": len(feature_cols),
        "d_model": 64,
        "nhead": 4,
        "num_layers": 2,
        "dropout": 0.1,
    }
    paths["transformer_load"] = save_torch_model(
        model, config, X_mean, X_scale, y_mean, y_scale,
        feature_cols, lookback, name="transformer_load", models_dir=models_dir,
        val_losses=val_losses,
    )
    print(f"Saved transformer_load: {paths['transformer_load']}")
    return paths


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _data_dir = os.environ.get("BDSP_ENERGY_EXPORT_DIR", "data/energy/")
    _models_dir = os.environ.get("BDSP_MODELS_DIR", "models/")
    run_training(data_dir=_data_dir, models_dir=_models_dir)
