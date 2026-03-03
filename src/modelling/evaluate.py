"""
Model evaluation for BigDataSmallPrice – Phase 3.

Computes MAE, RMSE, and MAPE for one or more trained models.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ── Metrics ───────────────────────────────────────────────────────────────────


def compute_metrics(
    y_true: np.ndarray | list,
    y_pred: np.ndarray | list,
) -> dict[str, float]:
    """
    Compute MAE, RMSE, and MAPE between *y_true* and *y_pred*.

    MAPE skips rows where ``y_true == 0`` to avoid division by zero.
    If all rows have ``y_true == 0`` the MAPE is returned as ``NaN``.

    Args:
        y_true: Ground-truth target values.
        y_pred: Model predictions.

    Returns:
        Dict with keys ``"mae"``, ``"rmse"``, ``"mape"`` (all floats).
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    mae = float(np.mean(np.abs(y_true - y_pred)))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    mask = y_true != 0
    if mask.any():
        mape = float(
            np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
        )
    else:
        mape = float("nan")

    return {"mae": mae, "rmse": rmse, "mape": mape}


def evaluate_all(
    models: dict,
    X_test: pd.DataFrame,
    y_test: pd.DataFrame,
) -> dict[str, dict[str, float]]:
    """
    Evaluate all models on *X_test* / *y_test*.

    NaN values in *X_test* are imputed with column medians before prediction.

    Args:
        models: Dict mapping model name → fitted sklearn-compatible model.
        X_test: Feature DataFrame.
        y_test: Target DataFrame (single column).

    Returns:
        Dict mapping model name → ``{mae, rmse, mape}``.
    """
    results: dict[str, dict[str, float]] = {}
    y_true = y_test.values.ravel()
    X_filled = X_test.fillna(X_test.median(numeric_only=True))

    for name, model in models.items():
        y_pred = model.predict(X_filled)
        m = compute_metrics(y_true, y_pred)
        results[name] = m
        print(
            f"{name:12s}  MAE={m['mae']:.3f}  "
            f"RMSE={m['rmse']:.3f}  MAPE={m['mape']:.2f}%"
        )

    return results
