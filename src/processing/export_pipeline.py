"""
Feature export pipeline for BigDataSmallPrice – Phase 2.

Reads the `training_features` view from TimescaleDB, validates for data
leakage, performs a chronological train/test split, and saves parquet files.

Usage (standalone):
    python src/processing/export_pipeline.py

Usage (from Airflow):
    from processing.export_pipeline import run_export
    run_export(output_dir="/opt/airflow/data/")
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    pass

# ─── Column definitions ───────────────────────────────────────────────────────

TARGET_COL = "price_eur_mwh"

FEATURE_COLS: list[str] = [
    # Lag features
    "lag_1h",
    "lag_2h",
    "lag_24h",
    "lag_168h",
    # Rolling averages (price)
    "rolling_avg_24h",
    "rolling_avg_7d",
    # Calendar
    "hour_of_day",
    "day_of_week",
    "month",
    "is_weekend",
    "is_peak_hour",
    # Weather
    "temperature_2m",
    "wind_speed_10m",
    "shortwave_radiation",
    "cloud_cover",
    "precipitation_mm",     # Niederschlag – req.md Phase 1
    "temp_rolling_avg_24h",
    # Hydro
    "discharge_m3s",
    "level_masl",
    # Tariff signals (dynamic providers)
    "tariff_price_chf_kwh_avg",   # Groupe E 'integrated' – primary signal
    "ckw_price_chf_kwh_avg",      # CKW 'integrated'      – secondary signal
]

# ─── Core functions ───────────────────────────────────────────────────────────


def query_features(conn) -> pd.DataFrame:
    """
    Read the `training_features` view from TimescaleDB, ordered by time.

    Args:
        conn: An open psycopg2 connection (from get_conn()).

    Returns:
        DataFrame with all feature and target columns, sorted by time ascending.
    """
    sql = "SELECT * FROM training_features ORDER BY time"
    return pd.read_sql(sql, conn, parse_dates=["time"])


def validate_no_leakage(
    feature_cols: list[str],
    target_col: str = TARGET_COL,
) -> None:
    """
    Raise ValueError if the target column appears in the feature column list.

    This guards against accidentally including the prediction target as a
    model input, which would constitute data leakage.

    Args:
        feature_cols: List of column names used as model inputs.
        target_col:   Name of the prediction target.
    """
    if target_col in feature_cols:
        raise ValueError(
            f"Data leakage detected: target column '{target_col}' is present "
            "in feature_cols. Remove it before training."
        )


def split_chronological(
    df: pd.DataFrame,
    test_ratio: float = 0.2,
    time_col: str = "time",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split a time-ordered DataFrame into train and test sets chronologically.

    The first (1 - test_ratio) fraction becomes the training set; the
    remaining fraction becomes the test set. No shuffling is applied so
    temporal ordering is preserved, preventing look-ahead bias.

    Args:
        df:         DataFrame sorted by time ascending.
        test_ratio: Fraction of rows to use as the test set (default 0.2).
        time_col:   Name of the timestamp column (used only for the docstring;
                    the split is purely index-based).

    Returns:
        (train_df, test_df) – non-overlapping DataFrames in temporal order.

    Raises:
        ValueError: If test_ratio is not in (0, 1) or produces an empty split.
    """
    if not (0 < test_ratio < 1):
        raise ValueError(f"test_ratio must be in (0, 1), got {test_ratio}.")
    split_idx = int(len(df) * (1 - test_ratio))
    if split_idx == 0 or split_idx == len(df):
        raise ValueError(
            f"test_ratio={test_ratio} produces an empty train or test set "
            f"for a dataset of {len(df)} rows."
        )
    return df.iloc[:split_idx].copy(), df.iloc[split_idx:].copy()


def save_parquet(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.DataFrame,
    y_test: pd.DataFrame,
    output_dir: str = "data/",
    timestamp: str | None = None,
) -> dict[str, Path]:
    """
    Write the four splits to parquet files with a timestamp suffix.

    Files are named ``X_train_<timestamp>.parquet``, etc. If *timestamp* is
    None, today's UTC date in YYYYMMDD format is used.

    Args:
        X_train, X_test:  Feature DataFrames.
        y_train, y_test:  Target DataFrames (single column).
        output_dir:       Directory to write files (created if missing).
        timestamp:        Optional YYYYMMDD string for the filename suffix.

    Returns:
        Dict mapping split name (e.g. "X_train") to the written Path.
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    splits = {
        "X_train": X_train,
        "X_test": X_test,
        "y_train": y_train,
        "y_test": y_test,
    }
    paths: dict[str, Path] = {}
    for name, frame in splits.items():
        p = out / f"{name}_{timestamp}.parquet"
        frame.to_parquet(p, index=False)
        paths[name] = p

    return paths


# ─── Orchestration ────────────────────────────────────────────────────────────


def run_export(
    output_dir: str = "data/",
    test_ratio: float = 0.2,
) -> dict[str, Path]:
    """
    End-to-end feature export:

    1. Validate no target leakage in FEATURE_COLS.
    2. Query the ``training_features`` view from TimescaleDB.
    3. Chronological train/test split.
    4. Save four parquet files (X_train, X_test, y_train, y_test).

    Args:
        output_dir: Directory for parquet files (default ``data/``).
        test_ratio: Fraction of rows for the test set (default 0.2).

    Returns:
        Dict mapping split name to written Path.
    """
    # Import here so the module can be imported without a live DB connection
    # (e.g. during unit tests of pure functions).
    from db.timescale_client import get_conn  # noqa: PLC0415

    validate_no_leakage(FEATURE_COLS, TARGET_COL)

    with get_conn() as conn:
        df = query_features(conn)

    if df.empty:
        raise RuntimeError(
            "training_features view returned 0 rows. "
            "Ensure the ETL pipeline has loaded data before running the export."
        )

    train_df, test_df = split_chronological(df, test_ratio)

    X_train = train_df[FEATURE_COLS]
    y_train = train_df[[TARGET_COL]]
    X_test = test_df[FEATURE_COLS]
    y_test = test_df[[TARGET_COL]]

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    paths = save_parquet(X_train, X_test, y_train, y_test, output_dir, timestamp)

    print(f"Export complete – {len(train_df)} train rows, {len(test_df)} test rows.")
    for name, p in paths.items():
        print(f"  {name}: {p}")

    return paths


# ─── Model A: Load feature export ────────────────────────────────────────────

LOAD_TARGET_COL = "net_load_kwh"

LOAD_FEATURE_COLS: list[str] = [
    # Lag features (net load)
    "load_lag_1h",
    "load_lag_1d",
    "load_lag_7d",
    "load_rolling_avg_24h",
    # Calendar
    "hour_of_day",
    "day_of_week",
    "month",
    "quarter",
    "is_weekend",
    # Holiday (computed in Python, not in SQL)
    "is_holiday_zh",
    # Weather
    "temperature_2m",
    "wind_speed_10m",
    "shortwave_radiation",
    "cloud_cover",
    "precipitation_mm",
    # PV feed-in (exogenous)
    "pv_feed_in_kwh",
]


def query_load_features(conn) -> pd.DataFrame:
    """Read the ``winterthur_net_load_features`` view, ordered by time."""
    sql = "SELECT * FROM winterthur_net_load_features ORDER BY time"
    return pd.read_sql(sql, conn, parse_dates=["time"])


def _add_holiday_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute ``is_holiday_zh`` flag in Python (CH public holidays for ZH).

    Uses the ``holidays`` package if available; otherwise defaults to 0.
    """
    try:
        import holidays as hd  # noqa: PLC0415

        ch_zh = hd.country_holidays("CH", subdiv="ZH")
        dates = df["time"].dt.date
        df = df.copy()
        df["is_holiday_zh"] = dates.apply(lambda d: int(d in ch_zh))
    except ImportError:
        df = df.copy()
        df["is_holiday_zh"] = 0
    return df


def split_by_dates(
    df: pd.DataFrame,
    train_end: str = "2022-12-31",
    val_end: str = "2023-12-31",
    time_col: str = "time",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split a time-ordered DataFrame into train / val / test by calendar date.

    Args:
        df:        DataFrame sorted by time ascending.
        train_end: Last date (inclusive) of the training set.
        val_end:   Last date (inclusive) of the validation set.
        time_col:  Name of the UTC-aware timestamp column.

    Returns:
        (train_df, val_df, test_df)
    """
    col = df[time_col]
    train_mask = col.dt.date <= pd.Timestamp(train_end).date()
    val_mask   = (col.dt.date > pd.Timestamp(train_end).date()) & \
                 (col.dt.date <= pd.Timestamp(val_end).date())
    test_mask  = col.dt.date > pd.Timestamp(val_end).date()
    return df[train_mask].copy(), df[val_mask].copy(), df[test_mask].copy()


def run_load_export(
    output_dir: str = "data/",
    train_end: str = "2022-12-31",
    val_end:   str = "2023-12-31",
) -> dict[str, "Path"]:
    """
    End-to-end feature export for Model A (grid-load forecasting):

    1. Validate no target leakage in LOAD_FEATURE_COLS.
    2. Query the ``winterthur_net_load_features`` view from TimescaleDB.
    3. Compute ``is_holiday_zh`` flag.
    4. Date-based train/val/test split (train≤2022, val=2023, test≥2024).
    5. Save parquet files: X_load_train, X_load_val, X_load_test,
                           y_load_train, y_load_val, y_load_test.

    Args:
        output_dir: Directory for parquet files (default ``data/``).
        train_end:  Last date of training set (YYYY-MM-DD).
        val_end:    Last date of validation set (YYYY-MM-DD).

    Returns:
        Dict mapping split name to written Path.
    """
    from db.timescale_client import get_conn  # noqa: PLC0415

    validate_no_leakage(LOAD_FEATURE_COLS, LOAD_TARGET_COL)

    with get_conn() as conn:
        df = query_load_features(conn)

    if df.empty:
        raise RuntimeError(
            "winterthur_net_load_features view returned 0 rows. "
            "Ensure the ETL pipeline has loaded Winterthur OGD data."
        )

    df = _add_holiday_flags(df)

    # Drop rows where target is NaN
    df = df.dropna(subset=[LOAD_TARGET_COL])

    train_df, val_df, test_df = split_by_dates(df, train_end, val_end)

    # Only keep feature cols that exist in the dataframe
    available = [c for c in LOAD_FEATURE_COLS if c in df.columns]

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    paths: dict[str, Path] = {}
    for split_name, split_df in [("train", train_df), ("val", val_df), ("test", test_df)]:
        x_path = out / f"X_load_{split_name}_{timestamp}.parquet"
        y_path = out / f"y_load_{split_name}_{timestamp}.parquet"
        split_df[available].to_parquet(x_path, index=False)
        split_df[[LOAD_TARGET_COL]].to_parquet(y_path, index=False)
        paths[f"X_load_{split_name}"] = x_path
        paths[f"y_load_{split_name}"] = y_path

    total = len(train_df) + len(val_df) + len(test_df)
    print(
        f"Load export complete – {len(train_df)} train, "
        f"{len(val_df)} val, {len(test_df)} test rows (total {total})."
    )
    for name, p in paths.items():
        print(f"  {name}: {p}")

    return paths


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os

    # Allow override via env variable for Docker usage
    out_dir = os.environ.get("BDSP_EXPORT_DIR", "data/")
    ratio = float(os.environ.get("BDSP_TEST_RATIO", "0.2"))
    run_export(output_dir=out_dir, test_ratio=ratio)
