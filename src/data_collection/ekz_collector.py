"""
EKZ Tariff collector – refactored from src/utils/ekzTariffs.py.

API: https://api.tariffs.ekz.ch/v1/tariffs
Returns 15-minute dynamic tariff intervals.
"""

import logging
from datetime import datetime, timezone

from .base_collector import BaseCollector

_LOG = logging.getLogger(__name__)

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
        response = self._fetch_with_retry(_API_URL, params=params)
        return response.text

    def parse(self, raw: bytes | str) -> list[dict]:
        import json

        if isinstance(raw, bytes):
            raw = raw.decode()
        data = json.loads(raw)

        records: list[dict] = []
        prices = data.get("prices", [])

        if not prices and isinstance(data, dict):
            _LOG.warning(
                "EKZ parse: 0 records found. Response keys: %s. "
                "Top-level structure: %s",
                list(data.keys()),
                {k: type(v).__name__ for k, v in data.items()},
            )

        for entry in prices:
            ts_raw = entry.get("start_timestamp")
            if ts_raw is None:
                continue

            # Extract CHF_kWh from the electricity component
            electricity = {e["unit"]: e["value"] for e in entry.get("electricity", [])}
            price_raw = electricity.get("CHF_kWh")
            if price_raw is None:
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
