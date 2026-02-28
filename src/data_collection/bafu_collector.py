"""
BAFU (Bundesamt für Umwelt) – Hydro data collector.

REST API: https://ogd.bafu.admin.ch/hydro/
Station: Rhein-Rekingen, ID 2018
Data: discharge (m³/s) and water level (m.a.s.l.)
"""

from datetime import datetime, timedelta, timezone

import httpx

from .base_collector import BaseCollector

_BASE_URL = "https://ogd.bafu.admin.ch/hydro"
_STATION_ID = "2018"  # Rhein-Rekingen


class BafuCollector(BaseCollector):
    """
    Fetches hydrological data from the BAFU REST API.

    Args:
        station_id:  BAFU station ID. Defaults to Rhein-Rekingen (2018).
        days_back:   Number of days of history to fetch. Defaults to 2.
    """

    def __init__(
        self,
        station_id: str = _STATION_ID,
        days_back: int = 2,
    ) -> None:
        self.station_id = station_id
        self.days_back = days_back

    def fetch(self) -> str:
        now_utc = datetime.now(tz=timezone.utc)
        date_from = (now_utc - timedelta(days=self.days_back)).strftime("%Y-%m-%d")
        date_to = now_utc.strftime("%Y-%m-%d")

        url = f"{_BASE_URL}/stations/{self.station_id}/measurements"
        params = {
            "from": date_from,
            "to": date_to,
            "format": "json",
        }
        response = httpx.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.text

    def parse(self, raw: bytes | str) -> list[dict]:
        import json

        if isinstance(raw, bytes):
            raw = raw.decode()
        data = json.loads(raw)

        records: list[dict] = []
        measurements = data if isinstance(data, list) else data.get("measurements", data.get("data", []))

        for entry in measurements:
            ts_raw = entry.get("timestamp") or entry.get("time") or entry.get("date")
            if ts_raw is None:
                continue

            ts_utc = _parse_timestamp(ts_raw)
            records.append(
                {
                    "time": ts_utc,
                    "station_id": self.station_id,
                    "discharge_m3s": _safe_float(entry, "discharge") or _safe_float(entry, "q") or _safe_float(entry, "abfluss"),
                    "level_masl": _safe_float(entry, "level") or _safe_float(entry, "w") or _safe_float(entry, "pegel"),
                }
            )

        return records


def _parse_timestamp(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _safe_float(d: dict, key: str) -> float | None:
    v = d.get(key)
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None
