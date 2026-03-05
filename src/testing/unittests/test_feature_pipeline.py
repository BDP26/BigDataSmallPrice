"""
Unit tests for src/processing/export_pipeline.py – Phase 2 feature pipeline.

All tests operate on in-memory DataFrames; no database connection is required.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Make project root importable so `from src.processing...` works
sys.path.insert(0, str(Path(__file__).parents[3]))

from src.processing.export_pipeline import (
    FEATURE_COLS,
    LOAD_FEATURE_COLS,
    LOAD_TARGET_COL,
    TARGET_COL,
    _add_holiday_flags,
    save_parquet,
    split_by_dates,
    split_chronological,
    validate_no_leakage,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _make_feature_df(n: int = 200) -> pd.DataFrame:
    """
    Build a minimal feature DataFrame with *n* hourly rows.

    Prices increase linearly so chronological ordering is easy to verify.
    All feature columns are filled with deterministic floats.
    """
    times = pd.date_range("2026-01-01", periods=n, freq="h", tz="UTC")
    data: dict = {
        "time": times,
        TARGET_COL: [float(50 + i % 30) for i in range(n)],
    }
    for col in FEATURE_COLS:
        data[col] = [float(i % 10) for i in range(n)]
    return pd.DataFrame(data)


# ─── Test: lag feature logic ──────────────────────────────────────────────────


class TestLagFeaturesCorrect:
    """Verify that the lag feature semantics are correct."""

    def test_lag_24h_column_defined_as_feature(self):
        """lag_24h must be in FEATURE_COLS, not the target."""
        assert "lag_24h" in FEATURE_COLS

    def test_target_not_in_feature_cols(self):
        """The prediction target must never appear in the feature list."""
        assert TARGET_COL not in FEATURE_COLS

    def test_lag_shift_semantics(self):
        """
        Simulates what the SQL LAG(price, 24) produces:
        row i should carry the price from row i-24.
        """
        prices = [float(i * 5) for i in range(50)]
        df = pd.DataFrame({"price_eur_mwh": prices})
        df["lag_24h"] = df["price_eur_mwh"].shift(24)

        # Row 24 → lag_24h should equal price at row 0
        assert df["lag_24h"].iloc[24] == pytest.approx(df["price_eur_mwh"].iloc[0])
        # Row 0 → lag_24h is NaN (no prior data)
        assert pd.isna(df["lag_24h"].iloc[0])

    def test_all_lag_columns_present(self):
        """All four lag columns must be declared in FEATURE_COLS."""
        for col in ("lag_1h", "lag_2h", "lag_24h", "lag_168h"):
            assert col in FEATURE_COLS, f"Missing lag column: {col}"


# ─── Test: data leakage validation ───────────────────────────────────────────


class TestNoDataLeakage:
    """Ensure validate_no_leakage correctly detects target-in-features."""

    def test_passes_with_valid_feature_list(self):
        """Default FEATURE_COLS must pass without raising."""
        validate_no_leakage(FEATURE_COLS, TARGET_COL)  # should not raise

    def test_raises_when_target_in_feature_list(self):
        """Including the target in feature_cols must raise ValueError."""
        bad_features = FEATURE_COLS + [TARGET_COL]
        with pytest.raises(ValueError, match="leakage"):
            validate_no_leakage(bad_features, TARGET_COL)

    def test_raises_message_mentions_column_name(self):
        """Error message must name the offending column for easy debugging."""
        bad_features = ["lag_1h", TARGET_COL]
        with pytest.raises(ValueError, match=TARGET_COL):
            validate_no_leakage(bad_features, TARGET_COL)


# ─── Test: chronological split ───────────────────────────────────────────────


class TestChronologicalSplit:
    """Verify train/test split preserves temporal ordering with no leakage."""

    def test_test_data_is_strictly_after_train_data(self):
        """The latest train timestamp must be earlier than the earliest test timestamp."""
        df = _make_feature_df(100)
        train, test = split_chronological(df, test_ratio=0.2)
        assert train["time"].max() < test["time"].min()

    def test_split_sizes_match_ratio(self):
        """80 / 20 split on 100 rows → 80 train, 20 test."""
        df = _make_feature_df(100)
        train, test = split_chronological(df, test_ratio=0.2)
        assert len(train) == 80
        assert len(test) == 20

    def test_no_timestamp_overlap(self):
        """No timestamp should appear in both train and test sets."""
        df = _make_feature_df(100)
        train, test = split_chronological(df, test_ratio=0.2)
        overlap = set(train["time"]) & set(test["time"])
        assert len(overlap) == 0

    def test_train_plus_test_equals_full_dataset(self):
        """Row counts must sum to the original dataset size."""
        df = _make_feature_df(100)
        train, test = split_chronological(df, test_ratio=0.2)
        assert len(train) + len(test) == len(df)

    def test_invalid_ratio_zero_raises(self):
        df = _make_feature_df(50)
        with pytest.raises(ValueError):
            split_chronological(df, test_ratio=0.0)

    def test_invalid_ratio_one_raises(self):
        df = _make_feature_df(50)
        with pytest.raises(ValueError):
            split_chronological(df, test_ratio=1.0)


# ─── Test: parquet export ─────────────────────────────────────────────────────


class TestExportFilesExist:
    """Verify that save_parquet creates all four expected files."""

    def _prepare_splits(self, n: int = 100):
        df = _make_feature_df(n)
        train_df, test_df = split_chronological(df, test_ratio=0.2)
        return (
            train_df[FEATURE_COLS],
            test_df[FEATURE_COLS],
            train_df[[TARGET_COL]],
            test_df[[TARGET_COL]],
        )

    def test_all_four_parquet_files_created(self, tmp_path):
        """After save_parquet, all four split files must exist on disk."""
        X_train, X_test, y_train, y_test = self._prepare_splits()
        save_parquet(X_train, X_test, y_train, y_test, str(tmp_path))

        for name in ("X_train", "X_test", "y_train", "y_test"):
            assert (tmp_path / f"{name}.parquet").exists(), (
                f"Missing file: {name}.parquet"
            )

    def test_loaded_x_train_matches_input(self, tmp_path):
        """X_train.parquet, when loaded, must have identical shape and columns."""
        X_train, X_test, y_train, y_test = self._prepare_splits()
        paths = save_parquet(X_train, X_test, y_train, y_test, str(tmp_path))

        loaded = pd.read_parquet(paths["X_train"])
        assert loaded.shape == X_train.shape
        assert list(loaded.columns) == list(X_train.columns)

    def test_train_larger_than_test(self, tmp_path):
        """Parquet row counts must reflect the 80/20 split."""
        X_train, X_test, y_train, y_test = self._prepare_splits(100)
        paths = save_parquet(X_train, X_test, y_train, y_test, str(tmp_path))

        n_train = len(pd.read_parquet(paths["X_train"]))
        n_test = len(pd.read_parquet(paths["X_test"]))
        assert n_train > n_test


# ─── Test: no nulls in key columns ───────────────────────────────────────────


class TestNoNullsInKeyColumns:
    """Ensure that null-free DataFrames pass and null-containing ones fail."""

    def test_clean_feature_df_has_no_nulls_in_lag_cols(self):
        """A fully populated DataFrame must have zero NaN values in lag columns."""
        df = _make_feature_df(50)
        lag_cols = ["lag_1h", "lag_2h", "lag_24h", "lag_168h"]
        for col in lag_cols:
            assert df[col].isna().sum() == 0, f"Unexpected NaN in '{col}'"

    def test_null_in_lag_column_is_detected(self):
        """Introduce a NaN and confirm it can be found programmatically."""
        df = _make_feature_df(50)
        df.loc[5, "lag_24h"] = float("nan")
        assert df["lag_24h"].isna().sum() == 1

    def test_validate_no_leakage_does_not_modify_dataframe(self):
        """validate_no_leakage must be a pure check and not alter the feature list."""
        original = list(FEATURE_COLS)
        validate_no_leakage(FEATURE_COLS, TARGET_COL)
        assert list(FEATURE_COLS) == original


# ─── Test: Model A – split_by_dates ──────────────────────────────────────────


def _make_load_df() -> pd.DataFrame:
    """Multi-year DataFrame spanning 2021-2025 for split_by_dates tests."""
    times = pd.date_range("2021-01-01", "2025-12-31 23:00", freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "time": times,
            "net_load_kwh": range(len(times)),
            "temperature_2m": 10.0,
        }
    )


class TestSplitByDates:
    """Verify date-based train/val/test split for Model A."""

    def test_train_is_on_or_before_2022(self):
        """All training rows must have time ≤ 2022-12-31."""
        df = _make_load_df()
        train, _, _ = split_by_dates(df)
        assert train["time"].dt.year.max() <= 2022

    def test_val_is_2023(self):
        """All validation rows must be within year 2023."""
        df = _make_load_df()
        _, val, _ = split_by_dates(df)
        assert val["time"].dt.year.min() == 2023
        assert val["time"].dt.year.max() == 2023

    def test_test_is_2024_or_later(self):
        """All test rows must have time ≥ 2024-01-01."""
        df = _make_load_df()
        _, _, test = split_by_dates(df)
        assert test["time"].dt.year.min() >= 2024

    def test_no_overlap_between_partitions(self):
        """No timestamp should appear in more than one partition."""
        df = _make_load_df()
        train, val, test = split_by_dates(df)
        train_times = set(train["time"])
        val_times = set(val["time"])
        test_times = set(test["time"])
        assert len(train_times & val_times) == 0
        assert len(val_times & test_times) == 0
        assert len(train_times & test_times) == 0

    def test_all_rows_accounted_for(self):
        """Sum of partition sizes must equal input DataFrame size."""
        df = _make_load_df()
        train, val, test = split_by_dates(df)
        assert len(train) + len(val) + len(test) == len(df)


# ─── Test: Model A – _add_holiday_flags ──────────────────────────────────────


class TestAddHolidayFlags:
    """Verify that _add_holiday_flags produces a valid binary column."""

    def test_returns_is_holiday_zh_column(self):
        """Output DataFrame must contain the is_holiday_zh column."""
        times = pd.date_range("2023-01-01", periods=48, freq="h", tz="UTC")
        df = pd.DataFrame({"time": times})
        result = _add_holiday_flags(df)
        assert "is_holiday_zh" in result.columns

    def test_column_is_binary(self):
        """is_holiday_zh must contain only 0 or 1 values."""
        times = pd.date_range("2023-01-01", periods=100, freq="h", tz="UTC")
        df = pd.DataFrame({"time": times})
        result = _add_holiday_flags(df)
        unique_vals = set(result["is_holiday_zh"].unique())
        assert unique_vals.issubset({0, 1})

    def test_new_years_day_is_holiday_when_package_available(self):
        """2023-01-01 is Neujahr – flagged as 1 if holidays package present, else 0."""
        try:
            import holidays as _hd  # noqa: F401
            holidays_available = True
        except ImportError:
            holidays_available = False

        times = pd.date_range("2023-01-01", periods=24, freq="h", tz="UTC")
        df = pd.DataFrame({"time": times})
        result = _add_holiday_flags(df)
        if holidays_available:
            assert result["is_holiday_zh"].iloc[0] == 1
        else:
            assert result["is_holiday_zh"].iloc[0] == 0

    def test_does_not_modify_original_df(self):
        """_add_holiday_flags must return a new DataFrame, not mutate the input."""
        times = pd.date_range("2023-06-01", periods=10, freq="h", tz="UTC")
        df = pd.DataFrame({"time": times})
        original_cols = list(df.columns)
        _add_holiday_flags(df)
        assert list(df.columns) == original_cols


# ─── Test: Model A – LOAD_FEATURE_COLS leakage check ─────────────────────────


class TestLoadFeatureColsNoLeakage:
    """Ensure Model A feature list does not contain the load target."""

    def test_load_target_not_in_load_feature_cols(self):
        """net_load_kwh must never appear in LOAD_FEATURE_COLS."""
        assert LOAD_TARGET_COL not in LOAD_FEATURE_COLS

    def test_load_feature_cols_pass_validate(self):
        """LOAD_FEATURE_COLS must pass validate_no_leakage without raising."""
        validate_no_leakage(LOAD_FEATURE_COLS, LOAD_TARGET_COL)

    def test_load_feature_cols_fail_if_target_injected(self):
        """Injecting target into LOAD_FEATURE_COLS must raise ValueError."""
        bad = LOAD_FEATURE_COLS + [LOAD_TARGET_COL]
        with pytest.raises(ValueError, match="leakage"):
            validate_no_leakage(bad, LOAD_TARGET_COL)
