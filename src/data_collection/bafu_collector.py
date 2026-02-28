"""
BAFU (Bundesamt für Umwelt) – Hydro data collector.

API: https://api.existenz.ch/apiv1/hydro/daterange
Station: Rhein-Rekingen, ID 2018
Parameters: flow = Abfluss (m³/s), height = Pegel (m.ü.M.)
"""

import json
import logging
from datetime import datetime, timedelta, timezone

from .base_collector import BaseCollector

_LOG = logging.getLogger(__name__)

_BASE_URL = "https://api.existenz.ch/apiv1/hydro/daterange"
_STATION_ID = "2018"  # Rhein-Rekingen


class BafuCollector(BaseCollector):
    """
    Fetches hydrological data from the existenz.ch hydro API (BAFU data).

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

        params = {
            "locations": self.station_id,
            "parameters": "flow,height",
            "startdate": date_from,
            "enddate": date_to,
            "app": "bdsp",
            "version": "0.1",
        }
        response = self._fetch_with_retry(_BASE_URL, params=params)
        return response.text

    def parse(self, raw: bytes | str) -> list[dict]:
        if isinstance(raw, bytes):
            raw = raw.decode()
        data = json.loads(raw)

        payload = data.get("payload", [])
        if not payload:
            _LOG.warning("BAFU parse: empty payload. Response keys: %s", list(data.keys()))
            return []

        # Merge flow and height entries that share the same timestamp into one record.
        # Timestamps are Unix epoch integers.
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

        records: list[dict] = []
        for ts_unix, fields in by_ts.items():
            ts_utc = datetime.fromtimestamp(fields["ts_unix"], tz=timezone.utc)
            records.append(
                {
                    "time": ts_utc,
                    "station_id": self.station_id,
                    "discharge_m3s": fields.get("discharge_m3s"),
                    "level_masl": fields.get("level_masl"),
                }
            )

        records.sort(key=lambda r: r["time"])
        return records
