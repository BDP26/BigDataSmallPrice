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

import sys
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
    "temp_rolling_avg_24h",
    # Hydro
    "discharge_m3s",
    "level_masl",
    # EKZ
    "ekz_price_chf_kwh_avg",
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


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os

    # Allow override via env variable for Docker usage
    out_dir = os.environ.get("BDSP_EXPORT_DIR", "data/")
    ratio = float(os.environ.get("BDSP_TEST_RATIO", "0.2"))
    run_export(output_dir=out_dir, test_ratio=ratio)
