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
import pandas as pd


# ── Helpers ───────────────────────────────────────────────────────────────────

# Feature column order must match what the model was trained on.
# Imported lazily to avoid a hard dependency on export_pipeline at import time.
def _feature_cols() -> list[str]:
    from processing.export_pipeline import FEATURE_COLS  # noqa: PLC0415
    return FEATURE_COLS


# ── Public API ────────────────────────────────────────────────────────────────


def load_model(path: str | Path) -> Any:
    """Load a joblib-serialised model from *path*."""
    return joblib.load(path)


def find_latest_model(models_dir: str = "models/", prefix: str = "xgb") -> Path:
    """
    Return the path to the most recently created model file with *prefix*.

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
