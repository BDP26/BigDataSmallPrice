"""
Stadtwerk Winterthur OGD collectors.

Two collectors for open government data published by Kanton Zürich:

  BruttolastgangCollector  – hourly grid load (kWh) for Winterthur network
  NetzEinspeisungCollector – hourly PV feed-in (kWh) injected into the grid

Data sources:
  https://daten.statistik.zh.ch (Kanton Zürich OGD portal)

CSV format (both files):
  zeitpunkt,<value_col>
  2022-01-01T00:15:00+0100,1234.5
  ...

Timestamps are ISO 8601 with UTC+01:00 offset; converted to UTC on parse.
Files can be large (multi-year, 15-min resolution) so fetch() uses httpx
streaming to avoid loading the full response body into memory.
"""

import csv
import io
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import httpx

from .base_collector import BaseCollector

_LOG = logging.getLogger(__name__)

# Directory for raw CSV snapshots – override with env var BDSP_RAW_DIR
_RAW_DIR = Path(os.environ.get("BDSP_RAW_DIR", "/opt/airflow/data/raw"))


def _save_raw(name: str, text: str) -> Path:
    """
    Persist *text* to ``<_RAW_DIR>/<name>_<YYYYMMDD>.csv``.

    Creates the directory if it doesn't exist.
    Returns the path written.
    """
    _RAW_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = _RAW_DIR / f"{name}_{ts}.csv"
    path.write_text(text, encoding="utf-8")
    _LOG.info("Raw CSV saved → %s (%d bytes)", path, len(text))
    return path


# ─── OGD resource URLs ────────────────────────────────────────────────────────

_BRUTTOLAST_URLS = [
    # 2013–2015
    "https://daten.statistik.zh.ch/ogd/daten/ressourcen/KTZH_00001863_00003561.csv",
    # 2016–2018
    "https://daten.statistik.zh.ch/ogd/daten/ressourcen/KTZH_00001863_00003563.csv",
    # 2019–2021
    "https://daten.statistik.zh.ch/ogd/daten/ressourcen/KTZH_00001863_00003564.csv",
    # 2022–current
    "https://daten.statistik.zh.ch/ogd/daten/ressourcen/KTZH_00001863_00003562.csv",
]

_NETZEINSPEISUNG_URL = (
    "https://daten.statistik.zh.ch/ogd/daten/ressourcen/KTZH_00003122_00006583.csv"
)

# ─── Shared streaming helper ──────────────────────────────────────────────────


def _stream_csv_text(url: str, timeout: int = 120) -> str:
    """Download *url* with httpx streaming and return the decoded text."""
    chunks: list[str] = []
    with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_text():
            chunks.append(chunk)
    return "".join(chunks)


def _parse_timestamp(raw: str) -> datetime:
    """Parse ISO 8601 timestamp (with any UTC offset) and return UTC-aware datetime."""
    # fromisoformat handles '+01:00', '+0100', 'Z' (Python 3.11+)
    # For older Python, normalise '+0100' → '+01:00'
    cleaned = raw.strip()
    if len(cleaned) > 5 and cleaned[-5] in ("+", "-") and ":" not in cleaned[-5:]:
        cleaned = cleaned[:-2] + ":" + cleaned[-2:]
    dt = datetime.fromisoformat(cleaned)
    return dt.astimezone(timezone.utc)


# ─── BruttolastgangCollector ──────────────────────────────────────────────────


class BruttolastgangCollector(BaseCollector):
    """
    Fetches Stadtwerk Winterthur hourly grid load (Bruttolastgang) from OGD.

    For daily ETL runs only the most-recent CSV file is needed; set
    ``all_files=True`` to fetch and merge all four historical files.

    Args:
        all_files: If True, fetch all 4 CSV files (2013–current).
                   If False (default), fetch only the current file (2022–current).
    """

    #: Column name containing the load value (kWh).
    _VALUE_COL = "bruttolastgang_kwh"

    def __init__(self, all_files: bool = False) -> None:
        self._urls = _BRUTTOLAST_URLS if all_files else [_BRUTTOLAST_URLS[-1]]

    def fetch(self) -> str:
        """Download CSV file(s), save raw snapshot, and return concatenated text."""
        parts: list[str] = []
        header_written = False
        for url in self._urls:
            _LOG.info("Fetching Bruttolastgang from %s", url)
            text = _stream_csv_text(url)
            lines = text.splitlines()
            if not lines:
                continue
            if not header_written:
                parts.extend(lines)
                header_written = True
            else:
                parts.extend(lines[1:])
        combined = "\n".join(parts)
        _save_raw("bruttolastgang", combined)
        return combined

    def parse(self, raw: bytes | str) -> list[dict]:
        """Parse CSV text into ``[{time, load_kwh}]`` records sorted by time."""
        if isinstance(raw, bytes):
            raw = raw.decode()
        if not raw.strip():
            return []

        reader = csv.DictReader(io.StringIO(raw))
        records: list[dict] = []

        # Accept flexible column names (strip whitespace, case-insensitive check)
        value_col: str | None = None

        for row in reader:
            # Detect value column on first data row
            if value_col is None:
                for col in row:
                    if col.strip().lower() == self._VALUE_COL:
                        value_col = col
                        break
                if value_col is None:
                    # Fall back: use the second column (after zeitpunkt)
                    cols = list(row.keys())
                    value_col = cols[1] if len(cols) > 1 else cols[0]
                    _LOG.warning(
                        "BruttolastgangCollector: column '%s' not found; "
                        "using '%s' as fallback.",
                        self._VALUE_COL,
                        value_col,
                    )

            raw_ts = row.get("zeitpunkt", "").strip()
            raw_val = row.get(value_col, "").strip()
            if not raw_ts or not raw_val:
                continue
            try:
                ts_utc = _parse_timestamp(raw_ts)
                load_kwh = float(raw_val)
            except (ValueError, TypeError):
                continue
            records.append({"time": ts_utc, "load_kwh": load_kwh})

        records.sort(key=lambda r: r["time"])
        return records


# ─── NetzEinspeisungCollector ─────────────────────────────────────────────────


class NetzEinspeisungCollector(BaseCollector):
    """
    Fetches Stadtwerk Winterthur PV feed-in into the grid (Netzeinspeisung) from OGD.

    The CSV has format: ``zeitpunkt, energietraeger, lastgang_kwh``
    with multiple rows per timestamp (photovoltaik, thermisch, wind, wasser).
    Only rows where ``energietraeger == 'photovoltaik'`` are used.

    Returns ``[{time, pv_kwh}]`` sorted ascending.
    """

    _FILTER_TYPE = "photovoltaik"

    def fetch(self) -> str:
        _LOG.info("Fetching Netzeinspeisung from %s", _NETZEINSPEISUNG_URL)
        text = _stream_csv_text(_NETZEINSPEISUNG_URL)
        _save_raw("netzeinspeisung", text)
        return text

    def parse(self, raw: bytes | str) -> list[dict]:
        """Parse CSV text into ``[{time, pv_kwh}]`` records sorted by time."""
        if isinstance(raw, bytes):
            raw = raw.decode()
        if not raw.strip():
            return []

        reader = csv.DictReader(io.StringIO(raw))
        records: list[dict] = []

        # Detect value column name (lastgang_kwh or fallback)
        value_col: str | None = None

        for row in reader:
            if value_col is None:
                lower_cols = {c.strip().lower(): c for c in row}
                value_col = lower_cols.get("lastgang_kwh") or lower_cols.get("einspeisung_kwh")
                if value_col is None:
                    cols = list(row.keys())
                    value_col = cols[2] if len(cols) > 2 else cols[-1]
                    _LOG.warning(
                        "NetzEinspeisungCollector: 'lastgang_kwh' not found; "
                        "using '%s' as fallback.",
                        value_col,
                    )

            # Filter: only photovoltaik rows
            energietraeger = row.get("energietraeger", "").strip().lower()
            if energietraeger != self._FILTER_TYPE:
                continue

            raw_ts = row.get("zeitpunkt", "").strip()
            raw_val = row.get(value_col, "").strip()
            if not raw_ts or not raw_val:
                continue
            try:
                ts_utc = _parse_timestamp(raw_ts)
                pv_kwh = float(raw_val)
            except (ValueError, TypeError):
                continue
            records.append({"time": ts_utc, "pv_kwh": pv_kwh})

        records.sort(key=lambda r: r["time"])
        return records
