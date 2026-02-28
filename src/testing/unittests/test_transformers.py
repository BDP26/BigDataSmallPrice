"""
Unit tests for data_cleaning.transformers.
"""

from datetime import datetime, timezone

import pandas as pd
import pytest

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[3]))

from src.data_cleaning.transformers import (
    aggregate_to_hourly,
    to_utc,
    validate_ascending_timestamps,
    validate_no_nulls,
)


class TestToUtc:
    def test_naive_timestamps_get_utc_tzinfo(self):
        df = pd.DataFrame({"time": ["2026-02-28 00:00:00", "2026-02-28 01:00:00"]})
        result = to_utc(df)
        assert result["time"].dt.tz is not None
        assert str(result["time"].dt.tz) == "UTC"

    def test_tz_aware_timestamps_convert_to_utc(self):
        df = pd.DataFrame(
            {"time": pd.to_datetime(["2026-02-28 00:00:00+01:00", "2026-02-28 01:00:00+01:00"])}
        )
        result = to_utc(df)
        assert str(result["time"].dt.tz) == "UTC"
        assert result["time"].iloc[0].hour == 23  # 00:00 CET = 23:00 UTC previous day

    def test_original_dataframe_not_mutated(self):
        df = pd.DataFrame({"time": ["2026-02-28 00:00:00"]})
        to_utc(df)
        # Original column should still be a string type (not datetime), not mutated
        assert not hasattr(df["time"].dtype, "tz")


class TestAggregateToHourly:
    def _make_15min_df(self) -> pd.DataFrame:
        times = pd.date_range("2026-02-28", periods=8, freq="15min", tz="UTC")
        values = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0]
        return pd.DataFrame({"time": times, "price": values})

    def test_output_has_hourly_frequency(self):
        df = self._make_15min_df()
        result = aggregate_to_hourly(df, "price")
        assert len(result) == 2

    def test_mean_aggregation_correct(self):
        df = self._make_15min_df()
        result = aggregate_to_hourly(df, "price")
        # First hour: mean(10, 20, 30, 40) = 25
        assert result["price"].iloc[0] == pytest.approx(25.0)

    def test_output_contains_time_and_value_columns(self):
        df = self._make_15min_df()
        result = aggregate_to_hourly(df, "price")
        assert "time" in result.columns
        assert "price" in result.columns


class TestValidateNoNulls:
    def test_passes_when_no_nulls(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        validate_no_nulls(df, ["a", "b"])  # should not raise

    def test_raises_on_null_value(self):
        df = pd.DataFrame({"a": [1, None, 3]})
        with pytest.raises(ValueError, match="a"):
            validate_no_nulls(df, ["a"])

    def test_raises_on_missing_column(self):
        df = pd.DataFrame({"a": [1, 2]})
        with pytest.raises(ValueError, match="nonexistent"):
            validate_no_nulls(df, ["nonexistent"])


class TestValidateAscendingTimestamps:
    def test_passes_for_sorted_timestamps(self):
        df = pd.DataFrame(
            {"time": pd.date_range("2026-02-28", periods=5, freq="h", tz="UTC")}
        )
        validate_ascending_timestamps(df)  # should not raise

    def test_raises_for_unsorted_timestamps(self):
        df = pd.DataFrame(
            {
                "time": [
                    datetime(2026, 2, 28, 2, tzinfo=timezone.utc),
                    datetime(2026, 2, 28, 0, tzinfo=timezone.utc),
                    datetime(2026, 2, 28, 1, tzinfo=timezone.utc),
                ]
            }
        )
        with pytest.raises(ValueError, match="ascending"):
            validate_ascending_timestamps(df)
