"""
Inference wrapper for BigDataSmallPrice – Phase 3.

Loads a joblib-serialised model and runs predictions on feature dicts or
DataFrames. Designed to be called from the FastAPI /api/predict endpoint.

Usage:
    from modelling.predict import load_model, find_latest_model, predict_from_dict

    model = load_model(find_latest_model("models/", prefix="xgb"))
    price = predict_from_dict(model, {"lag_1h": 75.0, "temperature_2m": 8.5, ...})
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd


# ── Helpers ───────────────────────────────────────────────────────────────────

# Feature column order must match what the model was trained on.
# Imported lazily to avoid a hard dependency on export_pipeline at import time.
def _feature_cols() -> list[str]:
    from processing.export_pipeline import FEATURE_COLS  # noqa: PLC0415
    return FEATURE_COLS


# ── Sklearn / XGBoost (joblib) models ────────────────────────────────────────


def load_model(path: str | Path) -> Any:
    """Load a joblib-serialised sklearn/XGBoost model from *path*."""
    return joblib.load(path)


def find_latest_model(models_dir: str = "models/", prefix: str = "xgb") -> Path:
    """
    Return the path to the most recently created ``.joblib`` file with *prefix*.

    Args:
        models_dir: Directory to search.
        prefix:     Model name prefix, e.g. ``"xgb"``, ``"naive"``.

    Raises:
        FileNotFoundError: If no matching file is found.
    """
    files = sorted(Path(models_dir).glob(f"{prefix}_*.joblib"))
    if not files:
        raise FileNotFoundError(
            f"No {prefix!r} model found in {models_dir!r}. "
            "Train a model first (src/modelling/train.py)."
        )
    return files[-1]


def predict(model: Any, X: pd.DataFrame) -> list[float]:
    """
    Run *model* on a feature DataFrame, returning predictions as a list.

    NaN values are filled with column medians before prediction.
    """
    X_filled = X.fillna(X.median(numeric_only=True))
    return model.predict(X_filled).tolist()


def predict_from_dict(model: Any, feature_dict: dict[str, float]) -> float:
    """
    Run *model* on a single feature dictionary.

    Missing features are replaced with ``NaN`` (XGBoost handles this natively;
    other models benefit from the median imputation in ``predict()``).

    Args:
        model:        Fitted sklearn-compatible model.
        feature_dict: Mapping of feature name → value.

    Returns:
        Scalar predicted price in EUR/MWh.
    """
    cols = _feature_cols()
    row = {col: feature_dict.get(col, float("nan")) for col in cols}
    X = pd.DataFrame([row])
    return float(predict(model, X)[0])


# ── PyTorch sequence models (LSTM / Transformer) ──────────────────────────────


def find_latest_torch_model(models_dir: str = "models/", prefix: str = "lstm_load") -> Path:
    """
    Return the path to the most recently created ``.pt`` bundle with *prefix*.

    Raises:
        FileNotFoundError: If no matching file is found.
    """
    files = sorted(Path(models_dir).glob(f"{prefix}_*.pt"))
    if not files:
        raise FileNotFoundError(
            f"No torch model with prefix {prefix!r} found in {models_dir!r}. "
            "Train a sequence model first."
        )
    return files[-1]


def predict_sequence_val(
    bundle_path: str | Path,
    X_train_tail: pd.DataFrame,
    X_val: pd.DataFrame,
    batch_size: int = 1024,
) -> np.ndarray:
    """
    Run a trained LSTM/Transformer on the validation set.

    Prepends the last *lookback* rows of *X_train_tail* as context so that
    the very first validation step has a full input window.  Applies the
    scaler stored in the bundle and rescales predictions to original units.

    Args:
        bundle_path:   Path to a ``.pt`` bundle created by save_torch_model().
        X_train_tail:  Last *lookback* rows of training features (unscaled).
        X_val:         Validation feature DataFrame (unscaled).
        batch_size:    Inference batch size.

    Returns:
        Array of shape (len(X_val),) in original target units.
    """
    from modelling.sequence_models import (  # noqa: PLC0415
        load_torch_bundle,
        predict_from_sequences,
        reconstruct_model,
    )

    bundle = load_torch_bundle(bundle_path)
    model = reconstruct_model(bundle)

    lookback: int = bundle["lookback"]
    X_mean: np.ndarray = bundle["scaler_mean"]
    X_scale: np.ndarray = bundle["scaler_scale"]
    y_mean: float = bundle["y_mean"]
    y_scale: float = bundle["y_scale"]
    feature_cols: list[str] = bundle["feature_cols"]

    # Align columns to what the model was trained on
    def _align(df: pd.DataFrame) -> np.ndarray:
        aligned = pd.DataFrame(
            {col: df[col] if col in df.columns else 0.0 for col in feature_cols}
        )
        return aligned.fillna(0).values.astype("float32")

    tail_np = _align(X_train_tail)[-lookback:]
    val_np  = _align(X_val)

    X_ctx = np.concatenate([tail_np, val_np], axis=0)
    X_ctx_sc = (X_ctx - X_mean) / X_scale

    preds_sc = predict_from_sequences(model, X_ctx_sc, lookback, batch_size)
    return preds_sc * y_scale + y_mean
