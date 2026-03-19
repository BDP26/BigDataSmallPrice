"""
Model evaluation for BigDataSmallPrice – Phase 3.

Computes MAE, RMSE, and MAPE for one or more trained models.
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

# MAPE threshold for Model A quality gate (req.md)
LOAD_MAPE_THRESHOLD = 8.0


# ── Metrics ───────────────────────────────────────────────────────────────────


def compute_metrics(
    y_true: np.ndarray | list,
    y_pred: np.ndarray | list,
) -> dict[str, float]:
    """
    Compute MAE, RMSE, and MAPE between *y_true* and *y_pred*.

    MAPE uses only rows where ``|y_true| >= 10.0`` to avoid near-zero inflation.
    In practice, prices below 10 EUR/MWh (or loads below 10 kWh) occur rarely and
    cause massive percentage errors even for small absolute deviations.
    The denominator uses ``|y_true|`` so negative prices are handled correctly.
    If no rows pass the threshold, MAPE is returned as ``NaN``.

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

    # Filter rows where |y_true| < 10.0 to avoid near-zero inflation.
    # Using |y_true| in the denominator handles negative prices correctly.
    mask = np.abs(y_true) >= 10.0
    if mask.any():
        mape = float(
            np.mean(
                np.abs(y_true[mask] - y_pred[mask]) / np.abs(y_true[mask])
            ) * 100
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
    X_filled = X_test.fillna(X_test.median(numeric_only=True)).fillna(0)

    for name, model in models.items():
        y_pred = model.predict(X_filled)
        m = compute_metrics(y_true, y_pred)
        results[name] = m
        print(
            f"{name:12s}  MAE={m['mae']:.3f}  "
            f"RMSE={m['rmse']:.3f}  MAPE={m['mape']:.2f}%"
        )

    return results


def save_metrics(
    metrics: dict[str, dict[str, float]],
    name: str,
    models_dir: str = "models/",
) -> Path:
    """
    Persist evaluation metrics to ``<models_dir>/<name>_<YYYYMMDD>.json``.

    Args:
        metrics:    Dict mapping model name → ``{mae, rmse, mape}``.
        name:       Short identifier, e.g. ``"metrics"`` or ``"metrics_load"``.
        models_dir: Target directory (created if missing).

    Returns:
        Path to the written JSON file.
    """
    from datetime import datetime, timezone

    out = Path(models_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = out / f"{name}_{ts}.json"
    with path.open("w") as fh:
        json.dump(metrics, fh, indent=2)
    print(f"Saved metrics: {path}")
    return path


def check_load_quality(metrics: dict[str, dict[str, float]]) -> None:
    """
    Emit a UserWarning if the load model MAPE exceeds the quality threshold.

    Checks for the key ``"model_load"`` in *metrics* (as produced by
    ``run_load_training()``).  Issues a warning – not an error – so the DAG
    continues running and the model file is still saved.

    Args:
        metrics: Dict mapping model name → ``{mae, rmse, mape}``.
    """
    xgb_mape = metrics.get("model_load", {}).get("mape")
    if xgb_mape is not None and not np.isnan(xgb_mape) and xgb_mape > LOAD_MAPE_THRESHOLD:
        warnings.warn(
            f"Model A MAPE={xgb_mape:.2f}% exceeds quality threshold of "
            f"{LOAD_MAPE_THRESHOLD:.0f}% (req.md). Consider retraining or "
            "collecting more load/PV data.",
            UserWarning,
            stacklevel=2,
        )
