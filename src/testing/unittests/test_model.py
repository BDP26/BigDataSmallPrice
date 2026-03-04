"""
Unit tests for Phase 3 modelling – train.py, evaluate.py, predict.py.

All tests use synthetic in-memory data; no database or disk model files
are required (tmp_path fixture handles serialisation tests).
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ── Path setup (mirrors the pattern in test_feature_pipeline.py) ──────────────
sys.path.insert(0, str(Path(__file__).parents[3]))

from src.modelling.evaluate import compute_metrics, evaluate_all
from src.modelling.predict import load_model, predict_from_dict
from src.modelling.train import (
    save_model,
    train_linear,
    train_naive,
    train_xgboost,
)
from src.processing.export_pipeline import FEATURE_COLS


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def synthetic_data():
    """
    Small synthetic dataset with a clear (but noisy) linear relationship.
    Returns (X_train, X_test, y_train, y_test).
    """
    rng = np.random.default_rng(42)
    n = 200

    X = pd.DataFrame(
        rng.standard_normal((n, len(FEATURE_COLS))),
        columns=FEATURE_COLS,
    )
    # Target is a simple linear combination of the first 3 features + constant
    y_vals = X.iloc[:, 0] * 10 + X.iloc[:, 1] * 5 + X.iloc[:, 2] * 3 + 80.0
    y_vals += rng.normal(0, 0.5, n)
    y = pd.DataFrame({"price_eur_mwh": y_vals})

    split = int(n * 0.8)
    return X.iloc[:split], X.iloc[split:], y.iloc[:split], y.iloc[split:]


# ── compute_metrics ───────────────────────────────────────────────────────────


def test_metrics_perfect_prediction():
    y = np.array([10.0, 20.0, 30.0])
    m = compute_metrics(y, y)
    assert m["mae"] == pytest.approx(0.0)
    assert m["rmse"] == pytest.approx(0.0)
    assert m["mape"] == pytest.approx(0.0)


def test_metrics_known_values():
    y_true = np.array([100.0, 200.0])
    y_pred = np.array([110.0, 190.0])
    m = compute_metrics(y_true, y_pred)
    assert m["mae"] == pytest.approx(10.0)
    assert m["rmse"] == pytest.approx(10.0)
    # MAPE = ((10/100 + 10/200) / 2) * 100 = 7.5 %
    assert m["mape"] == pytest.approx(7.5)


def test_metrics_zero_target_skipped():
    """MAPE must be finite even when some y_true values are 0."""
    y_true = np.array([0.0, 100.0])
    y_pred = np.array([5.0, 90.0])
    m = compute_metrics(y_true, y_pred)
    assert not np.isnan(m["mape"])
    assert m["mape"] == pytest.approx(10.0)


def test_metrics_all_zero_target_returns_nan():
    y_true = np.array([0.0, 0.0])
    y_pred = np.array([1.0, 2.0])
    m = compute_metrics(y_true, y_pred)
    assert np.isnan(m["mape"])


def test_metrics_returns_all_keys():
    m = compute_metrics([50.0], [55.0])
    assert {"mae", "rmse", "mape"} == set(m.keys())


# ── train_naive ───────────────────────────────────────────────────────────────


def test_naive_output_length(synthetic_data):
    X_train, X_test, y_train, _ = synthetic_data
    model = train_naive(X_train, y_train)
    preds = model.predict(X_test)
    assert len(preds) == len(X_test)


def test_naive_constant_prediction(synthetic_data):
    """DummyRegressor with strategy='mean' returns the same value for all rows."""
    X_train, X_test, y_train, _ = synthetic_data
    model = train_naive(X_train, y_train)
    preds = model.predict(X_test)
    assert np.all(preds == preds[0]), "Naive model should predict a constant value"


# ── train_linear ──────────────────────────────────────────────────────────────


def test_linear_output_length(synthetic_data):
    X_train, X_test, y_train, _ = synthetic_data
    model = train_linear(X_train, y_train)
    preds = model.predict(X_test.fillna(X_test.median(numeric_only=True)))
    assert len(preds) == len(X_test)


def test_linear_better_than_naive_on_linear_data(synthetic_data):
    X_train, X_test, y_train, y_test = synthetic_data
    naive = train_naive(X_train, y_train)
    linear = train_linear(X_train, y_train)

    naive_rmse = compute_metrics(
        y_test.values.ravel(), naive.predict(X_test)
    )["rmse"]
    linear_rmse = compute_metrics(
        y_test.values.ravel(),
        linear.predict(X_test.fillna(X_test.median(numeric_only=True))),
    )["rmse"]
    assert linear_rmse < naive_rmse, (
        f"Linear RMSE {linear_rmse:.3f} should beat naive RMSE {naive_rmse:.3f}"
    )


# ── train_xgboost ─────────────────────────────────────────────────────────────


def test_xgboost_output_length(synthetic_data):
    X_train, X_test, y_train, _ = synthetic_data
    model = train_xgboost(X_train, y_train)
    preds = model.predict(X_test)
    assert len(preds) == len(X_test)


def test_xgboost_beats_naive(synthetic_data):
    """XGBoost RMSE must be lower than naive on synthetic linear data."""
    X_train, X_test, y_train, y_test = synthetic_data
    naive = train_naive(X_train, y_train)
    xgb = train_xgboost(X_train, y_train)

    naive_rmse = compute_metrics(y_test.values.ravel(), naive.predict(X_test))["rmse"]
    xgb_rmse = compute_metrics(y_test.values.ravel(), xgb.predict(X_test))["rmse"]
    assert xgb_rmse < naive_rmse, (
        f"XGBoost RMSE {xgb_rmse:.3f} not better than naive RMSE {naive_rmse:.3f}"
    )


def test_xgboost_overfits_tiny_dataset():
    """Sanity: XGBoost should achieve a very low training RMSE on tiny data."""
    rng = np.random.default_rng(0)
    n = 30
    X = pd.DataFrame(rng.standard_normal((n, len(FEATURE_COLS))), columns=FEATURE_COLS)
    y = pd.DataFrame({"price_eur_mwh": rng.uniform(10, 100, n)})
    model = train_xgboost(X, y)
    m = compute_metrics(y.values.ravel(), model.predict(X))
    assert m["rmse"] < 5.0, f"Expected XGBoost to overfit tiny data; got RMSE={m['rmse']:.3f}"


# ── save_model / load_model ───────────────────────────────────────────────────


def test_save_creates_file(synthetic_data, tmp_path):
    X_train, _, y_train, _ = synthetic_data
    model = train_naive(X_train, y_train)
    path = save_model(model, "naive_test", models_dir=str(tmp_path))
    assert path.exists()
    assert path.suffix == ".joblib"


def test_load_model_identical_predictions(synthetic_data, tmp_path):
    X_train, X_test, y_train, _ = synthetic_data
    model = train_naive(X_train, y_train)
    path = save_model(model, "naive_test", models_dir=str(tmp_path))

    loaded = load_model(path)
    np.testing.assert_array_equal(model.predict(X_test), loaded.predict(X_test))


# ── predict_from_dict ─────────────────────────────────────────────────────────


def test_predict_from_dict_returns_float(synthetic_data):
    X_train, X_test, y_train, _ = synthetic_data
    model = train_xgboost(X_train, y_train)
    row = X_test.iloc[0].to_dict()
    result = predict_from_dict(model, row)
    assert isinstance(result, float)
    assert not np.isnan(result)


def test_predict_from_dict_empty_uses_nan(synthetic_data):
    """Missing features produce NaN inputs (handled gracefully by the model)."""
    X_train, _, y_train, _ = synthetic_data
    model = train_naive(X_train, y_train)
    result = predict_from_dict(model, {})
    assert isinstance(result, float)


# ── evaluate_all ──────────────────────────────────────────────────────────────


def test_evaluate_all_returns_all_models(synthetic_data):
    X_train, X_test, y_train, y_test = synthetic_data
    models = {
        "naive": train_naive(X_train, y_train),
        "linear": train_linear(X_train, y_train),
    }
    results = evaluate_all(models, X_test, y_test)
    assert set(results.keys()) == {"naive", "linear"}
    for m in results.values():
        assert m["mae"] >= 0
        assert m["rmse"] >= 0
