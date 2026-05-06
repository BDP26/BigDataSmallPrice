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
    Compute MAE, RMSE, MAPE, and residual std between *y_true* and *y_pred*.

    MAPE uses only rows where ``|y_true| >= 10.0`` to avoid near-zero inflation.
    In practice, prices below 10 EUR/MWh (or loads below 10 kWh) occur rarely and
    cause massive percentage errors even for small absolute deviations.
    The denominator uses ``|y_true|`` so negative prices are handled correctly.
    If no rows pass the threshold, MAPE is returned as ``NaN``.

    ``residual_std`` is the sample standard deviation of ``y_true - y_pred``
    (ddof=1) — used by the API to derive 95% prediction intervals.

    Args:
        y_true: Ground-truth target values.
        y_pred: Model predictions.

    Returns:
        Dict with keys ``"mae"``, ``"rmse"``, ``"mape"``, ``"residual_std"``,
        ``"n_residuals"``.
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    residuals = y_true - y_pred
    mae = float(np.mean(np.abs(residuals)))
    rmse = float(np.sqrt(np.mean(residuals ** 2)))
    residual_std = (
        float(np.std(residuals, ddof=1)) if len(residuals) > 1 else float("nan")
    )

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

    return {
        "mae": mae,
        "rmse": rmse,
        "mape": mape,
        "residual_std": residual_std,
        "n_residuals": int(len(residuals)),
    }


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


BEST_MODELS_MANIFEST = "best_models.json"


def select_best_model(
    metrics: dict[str, dict[str, float]],
    candidates: list[str],
    metric: str = "rmse",
) -> tuple[str, dict[str, float]] | None:
    """
    Pick the candidate with the lowest *metric* from *metrics*.

    Candidates missing the metric key (or with NaN) are skipped. ``naive``
    baselines should not appear in *candidates*; this function does not
    exclude them implicitly.

    Args:
        metrics:    Dict mapping model name → metrics dict.
        candidates: Allow-list of model names eligible for selection.
        metric:     Key inside the per-model metrics dict to minimize.

    Returns:
        ``(name, metrics_dict)`` of the winner, or ``None`` if no candidate
        has a usable metric value.
    """
    best: tuple[str, dict[str, float]] | None = None
    best_value: float = float("inf")
    for name in candidates:
        m = metrics.get(name)
        if not m:
            continue
        v = m.get(metric)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        if v < best_value:
            best_value = v
            best = (name, m)
    return best


def write_best_models_manifest(
    models_dir: str | Path,
    picks: dict[str, dict],
) -> Path:
    """
    Upsert ``best_models.json`` with the provided *picks*.

    Existing entries for other targets are preserved so that
    ``run_training()`` and ``run_load_training()`` can run independently
    without clobbering each other's manifest entries.

    Args:
        models_dir: Directory where the manifest lives.
        picks:      Dict mapping target (e.g. ``"energy"``, ``"load"``)
                    to a manifest entry, e.g.
                    ``{"prefix": "xgb", "rmse": 15.4, "residual_std": 14.9,
                       "n_residuals": 1234, "trained_on": "20260505"}``.

    Returns:
        Path to the written manifest.
    """
    out = Path(models_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / BEST_MODELS_MANIFEST

    existing: dict = {}
    if path.exists():
        try:
            with path.open("r") as fh:
                existing = json.load(fh) or {}
        except (json.JSONDecodeError, OSError):
            existing = {}

    existing.update(picks)
    with path.open("w") as fh:
        json.dump(existing, fh, indent=2)
    print(f"Updated best-models manifest: {path}  targets={list(picks)}")
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
