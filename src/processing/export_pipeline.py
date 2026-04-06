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

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    pass

_LOG = logging.getLogger(__name__)

# ─── Column definitions ───────────────────────────────────────────────────────

TARGET_COL = "price_eur_mwh"

FEATURE_COLS: list[str] = [
    # EPEX price lags (leakage-safe: always past values)
    "lag_1h",
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
    # Weather – CH (Winterthur)
    "temperature_2m",
    "wind_speed_10m",
    "shortwave_radiation",
    "cloud_cover",
    "precipitation_mm",
    "temp_rolling_avg_24h",
    # Weather – DE proxies (EPEX price drivers)
    "wind_speed_de_nord",        # Hamburg – DE wind proxy
    "solar_de_nord",
    "solar_de_sued",             # Stuttgart – DE solar proxy
    "wind_speed_de_sued",
    # ENTSO-E generation lags (B12 hydro RoR, B16 solar CH, B19 wind DE)
    "hydro_ror_ch_lag_24h",
    "hydro_ror_ch_lag_168h",
    "solar_gen_ch_lag_24h",
    "solar_gen_ch_lag_168h",
    "wind_gen_de_lag_24h",
    "wind_gen_de_lag_168h",
    # ENTSO-E actual load lags
    "actual_load_ch_lag_24h",
    "actual_load_ch_lag_168h",
    # ENTSO-E net position lags (all 4 borders: DE+IT+FR+AT)
    "net_position_ch_lag_24h",
    "net_position_ch_lag_168h",
    # ENTSO-E A65/A01 day-ahead load forecast (published D-1, leakage-safe for D+1)
    "load_forecast_ch",
]

# NOTE: req.md's "epex_t" maps to the target (price_eur_mwh) in this project.
# It is intentionally excluded from FEATURE_COLS to prevent target leakage.

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
    Raise ValueError if the target column appears in the feature column list,
    or if any api_call_log column appears in features (isolation guard).

    ISOLATION GUARANTEE: api_call_log is operational metadata only.
    It MUST NEVER appear in training_features, winterthur_net_load_features,
    FEATURE_COLS, or LOAD_FEATURE_COLS.

    Args:
        feature_cols: List of column names used as model inputs.
        target_col:   Name of the prediction target.
    """
    if target_col in feature_cols:
        raise ValueError(
            f"Data leakage detected: target column '{target_col}' is present "
            "in feature_cols. Remove it before training."
        )
    # Guard: api_call_log columns must never appear in feature lists
    _API_CALL_LOG_COLS = {"id", "source", "called_at", "status_code",
                          "was_rate_limited", "response_ms", "date_fetched"}
    leaked = _API_CALL_LOG_COLS & set(feature_cols)
    if leaked:
        raise ValueError(
            f"Isolation violation: api_call_log column(s) {leaked} found in "
            "feature_cols. api_call_log is operational metadata and must never "
            "be used as an ML feature."
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


def split_chronological_three_way(
    df: pd.DataFrame,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split a time-ordered DataFrame chronologically into train / val / test.

    Ratios:
        train = 1 - val_ratio - test_ratio  (default 0.70)
        val   = val_ratio                   (default 0.15)
        test  = test_ratio                  (default 0.15)

    No shuffling is applied so temporal ordering is preserved.

    Args:
        df:         DataFrame sorted by time ascending.
        val_ratio:  Fraction of rows for the validation set.
        test_ratio: Fraction of rows for the test set.

    Returns:
        (train_df, val_df, test_df) – non-overlapping in temporal order.

    Raises:
        ValueError: If ratios are invalid or any split would be empty.
    """
    train_ratio = 1.0 - val_ratio - test_ratio
    if not (0 < train_ratio < 1) or not (0 < val_ratio < 1) or not (0 < test_ratio < 1):
        raise ValueError(
            f"Invalid ratios: val_ratio={val_ratio}, test_ratio={test_ratio}. "
            "All three splits must be non-empty."
        )
    n = len(df)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))
    if train_end == 0 or val_end == train_end or val_end == n:
        raise ValueError(
            f"Ratios produce an empty split for a dataset of {n} rows."
        )
    return (
        df.iloc[:train_end].copy(),
        df.iloc[train_end:val_end].copy(),
        df.iloc[val_end:].copy(),
    )


def save_parquet(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.DataFrame,
    y_test: pd.DataFrame,
    output_dir: str = "data/energy/",
    X_val: pd.DataFrame | None = None,
    y_val: pd.DataFrame | None = None,
    versioned: bool = False,
    timestamp: str | None = None,
) -> dict[str, Path]:
    """
    Write feature/target splits to parquet files.

    By default files use fixed names (``X_train.parquet``, etc.) and are
    overwritten on each run.  Set *versioned=True* to append a timestamp
    suffix (``YYYYMMDD_HHMM``) so each run's output is preserved.

    Args:
        X_train, X_test:  Feature DataFrames for train and test splits.
        y_train, y_test:  Target DataFrames for train and test splits.
        output_dir:       Directory to write files (created if missing).
        X_val, y_val:     Optional validation-split DataFrames.
        versioned:        If True, append *timestamp* suffix to filenames.
        timestamp:        YYYYMMDD_HHMM string; auto-generated if None and
                          *versioned* is True.

    Returns:
        Dict mapping split name (e.g. "X_train") to the written Path.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    if versioned and timestamp is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")

    splits: dict[str, pd.DataFrame] = {
        "X_train": X_train,
        "X_test": X_test,
        "y_train": y_train,
        "y_test": y_test,
    }
    if X_val is not None:
        splits["X_val"] = X_val
    if y_val is not None:
        splits["y_val"] = y_val

    paths: dict[str, Path] = {}
    for name, frame in splits.items():
        fname = f"{name}_{timestamp}.parquet" if versioned else f"{name}.parquet"
        p = out / fname
        frame.to_parquet(p, index=False)
        paths[name] = p

    return paths


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _check_data_freshness(df: pd.DataFrame, max_age_hours: int = 26) -> None:
    """
    Raise RuntimeError if the newest timestamp in df is older than max_age_hours.

    26h = ETL runs at 06:00 UTC, feature pipeline at 07:00 UTC,
    so newest data should be at most ~25h old. 26h gives 1h buffer.

    Args:
        df:            DataFrame with a UTC-aware ``time`` column.
        max_age_hours: Maximum allowed age of the newest record in hours.
    """
    if df.empty:
        return  # Empty DataFrame is handled separately by the caller
    latest = df["time"].max()
    age_hours = (pd.Timestamp.now(tz="UTC") - latest).total_seconds() / 3600
    if age_hours > max_age_hours:
        raise RuntimeError(
            f"Data freshness check failed: newest record is {age_hours:.1f}h old "
            f"(threshold: {max_age_hours}h). ETL pipeline may have failed. "
            f"Latest timestamp in DB: {latest}"
        )


# ─── Orchestration ────────────────────────────────────────────────────────────


def run_export(
    output_dir: str = "data/energy/",
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
) -> dict[str, Path]:
    """
    End-to-end feature export for Model B (EPEX energy price):

    1. Validate no target leakage in FEATURE_COLS.
    2. Query the ``training_features`` view from TimescaleDB.
    3. Check data freshness (newest record ≤ 26 h old).
    4. Chronological 70 / 15 / 15 train / val / test split.
    5. Save six parquet files (X_train, X_val, X_test, y_train, y_val, y_test)
       with a timestamp suffix for reproducibility.

    Args:
        output_dir: Directory for parquet files (default ``data/energy/``).
        val_ratio:  Fraction of rows for the validation set (default 0.15).
        test_ratio: Fraction of rows for the test set (default 0.15).

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

    _check_data_freshness(df)

    train_df, val_df, test_df = split_chronological_three_way(df, val_ratio, test_ratio)

    missing = [c for c in FEATURE_COLS if c not in df.columns]
    if missing:
        _LOG.warning(
            "run_export: %d feature column(s) missing from training_features view and will be skipped: %s",
            len(missing),
            missing,
        )
    available = [c for c in FEATURE_COLS if c in df.columns]

    X_train = train_df[available]
    y_train = train_df[[TARGET_COL]]
    X_val = val_df[available]
    y_val = val_df[[TARGET_COL]]
    X_test = test_df[available]
    y_test = test_df[[TARGET_COL]]

    paths = save_parquet(
        X_train, X_test, y_train, y_test, output_dir,
        X_val=X_val, y_val=y_val,
    )

    # Save timestamps for the validation set (used by the dashboard validation chart)
    val_df[["time"]].to_parquet(Path(output_dir) / "timestamps_val.parquet", index=False)

    print(
        f"Export complete – {len(train_df)} train, "
        f"{len(val_df)} val, {len(test_df)} test rows."
    )
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
    "hour",
    "weekday",
    "month",
    "quarter",
    "is_weekend",
    # Holiday / school calendar (computed in Python, not in SQL)
    "is_holiday_zh",
    "is_school_holiday",
    # Weather
    "temp_c",
    "wind_speed_ms",
    "ghi_wm2",
    "cloud_cover_pct",
    "precipitation_mm",
    "temp_deviation",
    # PV feed-in (exogenous)
    "pv_feed_in",
]


def query_load_features(conn) -> pd.DataFrame:
    """Read the ``winterthur_net_load_features`` view, ordered by time."""
    sql = "SELECT * FROM winterthur_net_load_features ORDER BY time"
    return pd.read_sql(sql, conn, parse_dates=["time"])


# Schulferien Kanton Zürich 2013–2026 (Volksschulamt Kanton Zürich).
# Each tuple is (start_date_inclusive, end_date_inclusive) as "YYYY-MM-DD".
_ZH_SCHOOL_HOLIDAYS: list[tuple[str, str]] = [
    # 2013
    ("2013-02-18", "2013-03-01"),   # Sportferien
    ("2013-03-28", "2013-04-13"),   # Frühlingsferien / Ostern
    ("2013-07-13", "2013-08-17"),   # Sommerferien
    ("2013-10-05", "2013-10-19"),   # Herbstferien
    ("2013-12-21", "2014-01-04"),   # Weihnachtsferien
    # 2014
    ("2014-02-10", "2014-02-22"),
    ("2014-04-14", "2014-04-26"),
    ("2014-07-12", "2014-08-16"),
    ("2014-10-04", "2014-10-18"),
    ("2014-12-20", "2015-01-03"),
    # 2015
    ("2015-02-09", "2015-02-21"),
    ("2015-03-30", "2015-04-11"),
    ("2015-07-11", "2015-08-15"),
    ("2015-10-03", "2015-10-17"),
    ("2015-12-19", "2016-01-02"),
    # 2016
    ("2016-02-15", "2016-02-27"),
    ("2016-03-28", "2016-04-09"),
    ("2016-07-09", "2016-08-13"),
    ("2016-10-08", "2016-10-22"),
    ("2016-12-24", "2017-01-07"),
    # 2017
    ("2017-02-13", "2017-02-25"),
    ("2017-04-10", "2017-04-22"),
    ("2017-07-08", "2017-08-12"),
    ("2017-10-07", "2017-10-21"),
    ("2017-12-23", "2018-01-06"),
    # 2018
    ("2018-02-12", "2018-02-24"),
    ("2018-03-26", "2018-04-07"),
    ("2018-07-07", "2018-08-11"),
    ("2018-10-06", "2018-10-20"),
    ("2018-12-22", "2019-01-05"),
    # 2019
    ("2019-02-18", "2019-03-02"),
    ("2019-04-15", "2019-04-27"),
    ("2019-07-06", "2019-08-10"),
    ("2019-10-05", "2019-10-19"),
    ("2019-12-21", "2020-01-04"),
    # 2020
    ("2020-02-10", "2020-02-22"),
    ("2020-04-06", "2020-04-18"),
    ("2020-07-04", "2020-08-08"),
    ("2020-10-03", "2020-10-17"),
    ("2020-12-24", "2021-01-02"),
    # 2021
    ("2021-02-01", "2021-02-13"),
    ("2021-04-12", "2021-04-24"),
    ("2021-07-10", "2021-08-14"),
    ("2021-10-02", "2021-10-16"),
    ("2021-12-24", "2022-01-01"),
    # 2022
    ("2022-01-31", "2022-02-12"),
    ("2022-04-11", "2022-04-23"),
    ("2022-07-09", "2022-08-13"),
    ("2022-10-01", "2022-10-15"),
    ("2022-12-24", "2023-01-07"),
    # 2023
    ("2023-02-06", "2023-02-18"),
    ("2023-04-10", "2023-04-22"),
    ("2023-07-08", "2023-08-12"),
    ("2023-10-07", "2023-10-21"),
    ("2023-12-23", "2024-01-06"),
    # 2024
    ("2024-02-05", "2024-02-17"),
    ("2024-04-15", "2024-04-27"),
    ("2024-07-06", "2024-08-10"),
    ("2024-10-05", "2024-10-19"),
    ("2024-12-21", "2025-01-04"),
    # 2025
    ("2025-02-10", "2025-02-22"),
    ("2025-04-14", "2025-04-26"),
    ("2025-07-05", "2025-08-09"),
    ("2025-10-04", "2025-10-18"),
    ("2025-12-20", "2026-01-03"),
    # 2026
    ("2026-02-09", "2026-02-21"),
    ("2026-04-06", "2026-04-18"),
]


def _add_holiday_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute ``is_holiday_zh`` and ``is_school_holiday`` flags in Python.

    ``is_holiday_zh``    – CH public holidays for canton ZH (``holidays`` package).
    ``is_school_holiday`` – Schulferien Kanton Zürich (hardcoded from Volksschulamt ZH).

    Falls back to 0 for both flags if the computation fails.
    """
    df = df.copy()
    dates = df["time"].dt.date

    # ── Public holidays ────────────────────────────────────────────────────────
    try:
        import holidays as hd  # noqa: PLC0415

        ch_zh = hd.country_holidays("CH", subdiv="ZH")
        df["is_holiday_zh"] = dates.apply(lambda d: int(d in ch_zh))
    except ImportError:
        df["is_holiday_zh"] = 0

    # ── School holidays ────────────────────────────────────────────────────────
    try:
        from datetime import date as _date  # noqa: PLC0415

        periods = [
            (_date.fromisoformat(s), _date.fromisoformat(e))
            for s, e in _ZH_SCHOOL_HOLIDAYS
        ]

        def _in_school_holiday(d: "_date") -> int:
            return int(any(start <= d <= end for start, end in periods))

        df["is_school_holiday"] = dates.apply(_in_school_holiday)
    except Exception:  # noqa: BLE001
        _LOG.warning("_add_holiday_flags: is_school_holiday computation failed; defaulting to 0")
        df["is_school_holiday"] = 0

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
    output_dir: str = "data/load/",
    train_end: str | None = None,
    val_end: str | None = None,
    val_days: int = 14,
    test_days: int = 7,
) -> dict[str, "Path"]:
    """
    End-to-end feature export for Model A (grid-load forecasting):

    1. Validate no target leakage in LOAD_FEATURE_COLS.
    2. Query the ``winterthur_net_load_features`` view from TimescaleDB.
    3. Compute ``is_holiday_zh`` flag.
    4. Rolling date split anchored to the latest available data:
         test  = last ``test_days`` days  (default 7)
         val   = ``val_days`` before test  (default 14)
         train = everything before val
       This ensures val/test always sit in the most recent period where
       weather and ENTSO-E features are available.
       Pass explicit ``train_end`` / ``val_end`` strings to override.
    5. Save parquet files: X_train, X_val, X_test, y_train, y_val, y_test
       plus timestamps_val.parquet and timestamps_test.parquet.

    Args:
        output_dir: Directory for parquet files (default ``data/load/``).
        train_end:  Last date of training set (YYYY-MM-DD). If None, computed
                    from the data as max_date − test_days − val_days.
        val_end:    Last date of validation set (YYYY-MM-DD). If None, computed
                    as max_date − test_days.
        val_days:   Days to hold out for validation (default 14).
        test_days:  Days to hold out for test (default 7).

    Returns:
        Dict mapping split name to written Path.
    """
    from datetime import timedelta  # noqa: PLC0415

    from db.timescale_client import get_conn  # noqa: PLC0415

    validate_no_leakage(LOAD_FEATURE_COLS, LOAD_TARGET_COL)

    with get_conn() as conn:
        df = query_load_features(conn)

    if df.empty:
        raise RuntimeError(
            "winterthur_net_load_features view returned 0 rows. "
            "Ensure the ETL pipeline has loaded Winterthur OGD data."
        )

    # OGD Bruttolastgang (Kanton ZH) has a ~1–2 day publication lag,
    # so we allow up to 72h before raising a freshness error.
    _check_data_freshness(df, max_age_hours=72)

    df = _add_holiday_flags(df)

    # Compute temp_deviation (temperature_2m − daily mean)
    daily_avg = df.groupby(df["time"].dt.date)["temperature_2m"].transform("mean")
    df["temp_deviation"] = df["temperature_2m"] - daily_avg

    # Drop rows where target is NaN
    df = df.dropna(subset=[LOAD_TARGET_COL])

    # Compute dynamic split dates if not explicitly provided
    if train_end is None or val_end is None:
        max_date = df["time"].max().date()
        _val_end   = max_date - timedelta(days=test_days)
        _train_end = _val_end  - timedelta(days=val_days)
        if val_end   is None:
            val_end   = str(_val_end)
        if train_end is None:
            train_end = str(_train_end)

    print(
        f"Load export split: train ≤ {train_end}, "
        f"val = {train_end}+1 … {val_end}, "
        f"test = {val_end}+1 … latest"
    )

    train_df, val_df, test_df = split_by_dates(df, train_end, val_end)

    # Only keep feature cols that exist in the dataframe
    missing = [c for c in LOAD_FEATURE_COLS if c not in df.columns]
    if missing:
        _LOG.warning(
            "run_load_export: %d feature column(s) missing from view and will be skipped: %s",
            len(missing),
            missing,
        )
    available = [c for c in LOAD_FEATURE_COLS if c in df.columns]

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    paths: dict[str, Path] = {}
    for split_name, split_df in [("train", train_df), ("val", val_df), ("test", test_df)]:
        x_path = out / f"X_{split_name}.parquet"
        y_path = out / f"y_{split_name}.parquet"
        split_df[available].to_parquet(x_path, index=False)
        split_df[[LOAD_TARGET_COL]].to_parquet(y_path, index=False)
        paths[f"X_{split_name}"] = x_path
        paths[f"y_{split_name}"] = y_path

    # Save timestamps for val/test (used by the dashboard validation chart)
    val_df[["time"]].to_parquet(out / "timestamps_val.parquet", index=False)
    test_df[["time"]].to_parquet(out / "timestamps_test.parquet", index=False)

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
    out_dir = os.environ.get("BDSP_ENERGY_EXPORT_DIR", "data/energy/")
    run_export(output_dir=out_dir)
