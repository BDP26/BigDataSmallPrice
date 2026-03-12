"""
Open-Meteo historical weather backfill script.

Uses the Open-Meteo Archive API (https://archive.open-meteo.com/v1/archive)
to fetch hourly weather data for Winterthur from 2022-01-01 to yesterday
and upserts it into TimescaleDB.

No API token required.

Usage:
    BDSP_DB_PASSWORD=xxx python scripts/backfill_weather.py

Optional env vars:
    BACKFILL_START  – override start date (YYYY-MM-DD), default 2022-01-01
    BACKFILL_END    – override end date   (YYYY-MM-DD), default yesterday
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from db.timescale_client import upsert_weather

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s – %(message)s",
)
_LOG = logging.getLogger("backfill_weather")

_ARCHIVE_URL = "https://archive.open-meteo.com/v1/archive"
_LATITUDE = 47.5001
_LONGITUDE = 8.7502
_HOURLY_VARS = "temperature_2m,wind_speed_10m,shortwave_radiation,cloud_cover,precipitation"

# Chunk size: fetch 3 months at a time to stay within API limits
_CHUNK_DAYS = 90


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _date_chunks(start: datetime, end: datetime, chunk_days: int):
    """Yield (chunk_start, chunk_end) date pairs."""
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        yield current, chunk_end
        current = chunk_end


def _safe_float(lst: list, idx: int):
    try:
        v = lst[idx]
        return float(v) if v is not None else None
    except (IndexError, TypeError, ValueError):
        return None


def fetch_archive(start: datetime, end: datetime) -> list[dict]:
    """Fetch historical weather from Open-Meteo archive API."""
    params = {
        "latitude": _LATITUDE,
        "longitude": _LONGITUDE,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": (end - timedelta(days=1)).strftime("%Y-%m-%d"),  # end is exclusive
        "hourly": _HOURLY_VARS,
        "timezone": "UTC",
    }
    response = httpx.get(_ARCHIVE_URL, params=params, timeout=60)
    response.raise_for_status()
    data = json.loads(response.text)

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    winds = hourly.get("wind_speed_10m", [])
    radiations = hourly.get("shortwave_radiation", [])
    clouds = hourly.get("cloud_cover", [])
    precip = hourly.get("precipitation", [])

    records = []
    for i, t_str in enumerate(times):
        ts_utc = datetime.fromisoformat(t_str).replace(tzinfo=timezone.utc)
        records.append(
            {
                "time": ts_utc,
                "latitude": _LATITUDE,
                "longitude": _LONGITUDE,
                "temperature_2m": _safe_float(temps, i),
                "wind_speed_10m": _safe_float(winds, i),
                "shortwave_radiation": _safe_float(radiations, i),
                "cloud_cover": _safe_float(clouds, i),
                "precipitation_mm": _safe_float(precip, i),
            }
        )
    return records


def main() -> None:
    start_str = os.environ.get("BACKFILL_START", "2022-01-01")
    yesterday = datetime.now(tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    end_str = os.environ.get("BACKFILL_END", yesterday.strftime("%Y-%m-%d"))

    start = _parse_date(start_str)
    end = _parse_date(end_str) + timedelta(days=1)  # make end inclusive

    _LOG.info("Weather backfill %s → %s", start_str, end_str)

    total_records = 0
    chunks = list(_date_chunks(start, end, _CHUNK_DAYS))
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        _LOG.info(
            "[%d/%d] Fetching %s → %s",
            i, len(chunks),
            chunk_start.strftime("%Y-%m-%d"),
            (chunk_end - timedelta(days=1)).strftime("%Y-%m-%d"),
        )
        try:
            records = fetch_archive(chunk_start, chunk_end)
            if records:
                upsert_weather(records)
                total_records += len(records)
                _LOG.info("  Upserted %d records", len(records))
            else:
                _LOG.warning("  No records returned for this chunk")
        except Exception as exc:  # noqa: BLE001
            _LOG.error("  Failed: %s", exc)

    _LOG.info("Done. Total records upserted: %d", total_records)


if __name__ == "__main__":
    main()
