"""
EKZ Tariff collector – refactored from src/utils/ekzTariffs.py.

API: https://api.tariffs.ekz.ch/v1/tariffs
Returns 15-minute dynamic tariff intervals.
"""

from datetime import datetime, timezone

import httpx

from .base_collector import BaseCollector

_API_URL = "https://api.tariffs.ekz.ch/v1/tariffs"
_TARIFF_TYPE = "dynamic"


class EkzCollector(BaseCollector):
    """
    Fetches EKZ dynamic electricity tariffs (15-min intervals).

    Args:
        date:        Date string "YYYY-MM-DD". Defaults to today (UTC).
        tariff_type: Tariff type string. Defaults to "dynamic".
    """

    def __init__(
        self,
        date: str | None = None,
        tariff_type: str = _TARIFF_TYPE,
    ) -> None:
        self.date = date or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        self.tariff_type = tariff_type

    def fetch(self) -> str:
        params = {
            "date": self.date,
            "tariffType": self.tariff_type,
        }
        response = httpx.get(_API_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.text

    def parse(self, raw: bytes | str) -> list[dict]:
        import json

        if isinstance(raw, bytes):
            raw = raw.decode()
        data = json.loads(raw)

        records: list[dict] = []
        intervals = data.get("intervals", data.get("data", []))

        for interval in intervals:
            ts_raw = interval.get("startTime") or interval.get("timestamp") or interval.get("time")
            price_raw = interval.get("price") or interval.get("value")

            if ts_raw is None or price_raw is None:
                continue

            ts_utc = _parse_timestamp(ts_raw)
            records.append(
                {
                    "time": ts_utc,
                    "price_chf_kwh": float(price_raw),
                    "tariff_type": self.tariff_type,
                }
            )

        return records


def _parse_timestamp(ts: str) -> datetime:
    """Parse ISO-8601 timestamp and ensure UTC-awareness."""
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
