"""
BAFU (existenz.ch) hydrological data backfill script.

Fetches Rhein-Rekingen (station 2018) flow + height data from 2022-01-01
to yesterday in monthly chunks and upserts into TimescaleDB.

Usage:
    BDSP_DB_PASSWORD=xxx python scripts/backfill_bafu.py

Optional env vars:
    BACKFILL_START  – override start date (YYYY-MM-DD), default 2022-01-01
    BACKFILL_END    – override end date   (YYYY-MM-DD), default yesterday
    BAFU_STATION_ID – station ID, default 2018 (Rhein-Rekingen)
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

from db.timescale_client import upsert_bafu

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s – %(message)s",
)
_LOG = logging.getLogger("backfill_bafu")

_BASE_URL = "https://api.existenz.ch/apiv1/hydro/daterange"
_STATION_ID = "2018"

# API seems to handle ~30 days per request reliably
_CHUNK_DAYS = 30


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _date_chunks(start: datetime, end: datetime, chunk_days: int):
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        yield current, chunk_end
        current = chunk_end


def fetch_bafu(station_id: str, start: datetime, end: datetime) -> list[dict]:
    """Fetch BAFU hydro data for a date range."""
    params = {
        "locations": station_id,
        "parameters": "flow,height",
        "startdate": start.strftime("%Y-%m-%d"),
        "enddate": end.strftime("%Y-%m-%d"),
        "app": "bdsp",
        "version": "0.1",
    }
    response = httpx.get(_BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    data = json.loads(response.text)

    payload = data.get("payload", [])
    if not payload:
        return []

    by_ts: dict[int, dict] = {}
    for entry in payload:
        ts_unix = entry.get("timestamp")
        par = entry.get("par")
        val = entry.get("val")
        if ts_unix is None or par is None:
            continue
        if ts_unix not in by_ts:
            by_ts[ts_unix] = {"ts_unix": ts_unix}
        if par == "flow":
            by_ts[ts_unix]["discharge_m3s"] = float(val) if val is not None else None
        elif par == "height":
            by_ts[ts_unix]["level_masl"] = float(val) if val is not None else None

    records = []
    for ts_unix, fields in by_ts.items():
        ts_utc = datetime.fromtimestamp(fields["ts_unix"], tz=timezone.utc)
        records.append(
            {
                "time": ts_utc,
                "station_id": station_id,
                "discharge_m3s": fields.get("discharge_m3s"),
                "level_masl": fields.get("level_masl"),
            }
        )

    records.sort(key=lambda r: r["time"])
    return records


def main() -> None:
    station_id = os.environ.get("BAFU_STATION_ID", _STATION_ID)
    start_str = os.environ.get("BACKFILL_START", "2022-01-01")
    yesterday = datetime.now(tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    end_str = os.environ.get("BACKFILL_END", yesterday.strftime("%Y-%m-%d"))

    start = _parse_date(start_str)
    end = _parse_date(end_str)

    _LOG.info("BAFU backfill station=%s %s → %s", station_id, start_str, end_str)

    total_records = 0
    chunks = list(_date_chunks(start, end, _CHUNK_DAYS))
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        _LOG.info(
            "[%d/%d] Fetching %s → %s",
            i, len(chunks),
            chunk_start.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d"),
        )
        try:
            records = fetch_bafu(station_id, chunk_start, chunk_end)
            if records:
                upsert_bafu(records)
                total_records += len(records)
                _LOG.info("  Upserted %d records", len(records))
            else:
                _LOG.warning("  No records returned for this chunk")
        except Exception as exc:  # noqa: BLE001
            _LOG.error("  Failed: %s", exc)

    _LOG.info("Done. Total records upserted: %d", total_records)


if __name__ == "__main__":
    main()
