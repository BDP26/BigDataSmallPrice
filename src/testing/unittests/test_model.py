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

from src.modelling.evaluate import (
    LOAD_MAPE_THRESHOLD,
    check_load_quality,
    compute_metrics,
    evaluate_all,
    save_metrics,
)
from src.modelling.predict import load_model, predict_from_dict
from src.modelling.train import (
    save_model,
    train_linear,
    train_load_model,
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


def test_metrics_near_zero_target_skipped():
    """MAPE must be finite even when some y_true values are near-zero (< 10)."""
    # 0.0 and 5.0 are below the |y_true| >= 10.0 threshold → excluded.
    # Only the 100.0 row is used: |100-90|/100 * 100 = 10 %
    y_true = np.array([0.0, 5.0, 100.0])
    y_pred = np.array([5.0, 8.0, 90.0])
    m = compute_metrics(y_true, y_pred)
    assert not np.isnan(m["mape"])
    assert m["mape"] == pytest.approx(10.0)


def test_metrics_negative_price_handled():
    """Negative prices use |y_true| in denominator (electricity markets can go negative)."""
    # y_true=-20, y_pred=10 → |(-20-10)|/|-20| * 100 = 150%
    # y_true=100, y_pred=90 → |100-90|/100 * 100 = 10%
    y_true = np.array([-20.0, 100.0])
    y_pred = np.array([10.0, 90.0])
    m = compute_metrics(y_true, y_pred)
    # MAPE = mean([150%, 10%]) = 80%
    assert m["mape"] == pytest.approx(80.0)


def test_metrics_all_near_zero_returns_nan():
    """When all |y_true| < 10.0, MAPE cannot be computed → NaN."""
    y_true = np.array([0.0, 5.0, -3.0])
    y_pred = np.array([1.0, 2.0, 3.0])
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


# ── save_metrics ──────────────────────────────────────────────────────────────


def test_save_metrics_creates_json(tmp_path):
    metrics = {"xgb": {"mae": 1.2, "rmse": 1.8, "mape": 5.0}}
    path = save_metrics(metrics, "metrics", models_dir=str(tmp_path))
    assert path.exists()
    assert path.suffix == ".json"
    import json
    loaded = json.loads(path.read_text())
    assert loaded["xgb"]["mae"] == pytest.approx(1.2)


def test_save_metrics_content_roundtrips(tmp_path):
    """All three metric keys survive JSON serialisation."""
    metrics = {"naive": {"mae": 10.0, "rmse": 12.0, "mape": 8.5}}
    path = save_metrics(metrics, "metrics_load", models_dir=str(tmp_path))
    import json
    loaded = json.loads(path.read_text())
    assert set(loaded["naive"].keys()) == {"mae", "rmse", "mape"}


# ── check_load_quality ────────────────────────────────────────────────────────


def test_check_load_quality_warns_above_threshold():
    """A UserWarning is emitted when model_load MAPE exceeds the threshold."""
    bad_metrics = {"model_load": {"mae": 5.0, "rmse": 7.0, "mape": LOAD_MAPE_THRESHOLD + 0.1}}
    with pytest.warns(UserWarning, match="MAPE="):
        check_load_quality(bad_metrics)


def test_check_load_quality_silent_below_threshold():
    """No warning when model_load MAPE is within the threshold."""
    good_metrics = {"model_load": {"mae": 1.0, "rmse": 2.0, "mape": LOAD_MAPE_THRESHOLD - 0.1}}
    # Should not raise or warn
    check_load_quality(good_metrics)


def test_check_load_quality_missing_key_no_crash():
    """Gracefully handles missing 'model_load' key."""
    check_load_quality({})
    check_load_quality({"other_model": {"mae": 1.0, "rmse": 1.0, "mape": 20.0}})


# ── early stopping (XGBoost with val data) ────────────────────────────────────


def test_xgboost_accepts_val_args(synthetic_data):
    """train_xgboost with val data still produces correct-length predictions."""
    X_train, X_test, y_train, y_test = synthetic_data
    # Use a small slice as val set
    X_val = X_train.iloc[:20]
    y_val = y_train.iloc[:20]
    model = train_xgboost(X_train, y_train, X_val=X_val, y_val=y_val)
    preds = model.predict(X_test)
    assert len(preds) == len(X_test)
    assert not np.isnan(preds).any()


def test_load_model_accepts_val_args(synthetic_data):
    """train_load_model with val data still produces correct-length predictions."""
    X_train, X_test, y_train, _ = synthetic_data
    X_val = X_train.iloc[:20]
    y_val = y_train.iloc[:20]
    model = train_load_model(X_train, y_train, X_val=X_val, y_val=y_val)
    preds = model.predict(X_test)
    assert len(preds) == len(X_test)
    assert not np.isnan(preds).any()


# ── Model A baselines ──────────────────────────────────────────────────────────


def test_load_model_baselines_exist(synthetic_data):
    """Naive and Linear baselines can be trained and produce valid predictions."""
    X_train, X_test, y_train, y_test = synthetic_data
    naive = train_naive(X_train, y_train)
    linear = train_linear(X_train, y_train)
    load_xgb = train_load_model(X_train, y_train)

    for model in (naive, linear, load_xgb):
        preds = model.predict(X_test.fillna(X_test.median(numeric_only=True)))
        assert len(preds) == len(X_test)


def test_load_xgb_beats_naive(synthetic_data):
    """Model A XGBoost RMSE should beat the naive baseline on synthetic data."""
    X_train, X_test, y_train, y_test = synthetic_data
    naive = train_naive(X_train, y_train)
    load_xgb = train_load_model(X_train, y_train)

    naive_rmse = compute_metrics(y_test.values.ravel(), naive.predict(X_test))["rmse"]
    xgb_rmse = compute_metrics(y_test.values.ravel(), load_xgb.predict(X_test))["rmse"]
    assert xgb_rmse < naive_rmse, (
        f"Model A XGBoost RMSE {xgb_rmse:.3f} not better than naive RMSE {naive_rmse:.3f}"
    )
