"""
Data cleaning and transformation utilities.

Functions:
  to_utc()                   – Ensure all timestamps are UTC
  aggregate_to_hourly()      – Resample a DataFrame to 1h mean
  validate_no_nulls()        – Raise ValueError if specified columns contain NaN
  validate_ascending_timestamps() – Raise ValueError if timestamps are not sorted
"""

import pandas as pd


def to_utc(df: pd.DataFrame, time_col: str = "time") -> pd.DataFrame:
    """
    Convert the time column to UTC-aware timestamps.

    - If the column is already tz-aware, converts to UTC.
    - If it is tz-naive, assumes UTC and localizes.

    Returns a new DataFrame with the converted column.
    """
    df = df.copy()
    col = pd.to_datetime(df[time_col])
    if col.dt.tz is None:
        col = col.dt.tz_localize("UTC")
    else:
        col = col.dt.tz_convert("UTC")
    df[time_col] = col
    return df


def aggregate_to_hourly(
    df: pd.DataFrame,
    value_col: str,
    time_col: str = "time",
) -> pd.DataFrame:
    """
    Resample a DataFrame to 1-hour intervals using mean aggregation.

    The DataFrame must have a UTC-aware `time_col`. The result is indexed
    by the hourly bucket and contains only `value_col` (mean).

    Returns a new DataFrame with columns [time_col, value_col].
    """
    df = df.copy()
    df[time_col] = pd.to_datetime(df[time_col], utc=True)
    df = df.set_index(time_col)
    hourly = df[[value_col]].resample("1h").mean()
    hourly = hourly.reset_index().rename(columns={"index": time_col})
    return hourly


def validate_no_nulls(df: pd.DataFrame, cols: list[str]) -> None:
    """
    Raise ValueError if any of the specified columns contain NaN values.

    Args:
        df:   DataFrame to check.
        cols: Column names to validate.
    """
    for col in cols:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame.")
        null_count = df[col].isna().sum()
        if null_count > 0:
            raise ValueError(
                f"Column '{col}' contains {null_count} null value(s)."
            )


def validate_ascending_timestamps(
    df: pd.DataFrame, time_col: str = "time"
) -> None:
    """
    Raise ValueError if the time column is not strictly ascending.

    Args:
        df:       DataFrame to check.
        time_col: Name of the timestamp column.
    """
    times = pd.to_datetime(df[time_col])
    if not times.is_monotonic_increasing:
        raise ValueError(
            f"Column '{time_col}' is not sorted in ascending order."
        )
