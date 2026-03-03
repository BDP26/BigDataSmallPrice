"""
EKZ dynamic tariff collector.

API: https://api.tariffs.ekz.ch/v1/tariffs
Returns 15-minute dynamic tariff intervals.

Two tariffs are collected per day:
  electricity_dynamic → component 'electricity'  (pure energy price)
  integrated_400D     → component 'integrated'   (all-in price, highest variability)

Correct parameters (confirmed 2026-03):
  tariff_type, tariff_name, start_timestamp, end_timestamp (ISO with +01:00)
  — NOT the old date/tariffType params which returned a flat rate.
"""

import json
import logging
from datetime import datetime, timezone

from .base_collector import BaseCollector

_LOG = logging.getLogger(__name__)

_API_URL = "https://api.tariffs.ekz.ch/v1/tariffs"

# (tariff_type param, tariff_name param) → component key in response
_TARIFFS = (
    ("electricity", "electricity_dynamic"),
    ("integrated",  "integrated_400D"),
)


class EkzCollector(BaseCollector):
    """
    Fetches EKZ dynamic electricity tariffs (15-min intervals).

    Makes two API calls per day (electricity_dynamic + integrated_400D)
    and stores each as a separate record keyed by tariff_type.

    Args:
        date: Date string "YYYY-MM-DD". Defaults to today (UTC).
    """

    def __init__(self, date: str | None = None) -> None:
        self.date = date or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    def fetch(self) -> str:
        start = f"{self.date}T00:00:00+01:00"
        end   = f"{self.date}T23:59:59+01:00"

        combined: list[dict] = []
        for tariff_type, tariff_name in _TARIFFS:
            params = {
                "tariff_type":      tariff_type,
                "tariff_name":      tariff_name,
                "start_timestamp":  start,
                "end_timestamp":    end,
            }
            resp = self._fetch_with_retry(_API_URL, params=params)
            data = json.loads(resp.text)
            combined.extend(data.get("prices", []))

        if not combined:
            _LOG.warning("EKZ fetch: 0 price entries for date %s", self.date)

        return json.dumps({"prices": combined})

    def parse(self, raw: bytes | str) -> list[dict]:
        if isinstance(raw, bytes):
            raw = raw.decode()
        data = json.loads(raw)

        records: list[dict] = []
        for entry in data.get("prices", []):
            ts_raw = entry.get("start_timestamp")
            if ts_raw is None:
                continue
            ts_utc = datetime.fromisoformat(ts_raw).astimezone(timezone.utc)
            for tariff_type, _ in _TARIFFS:
                for item in entry.get(tariff_type, []):
                    if item.get("unit") == "CHF_kWh":
                        records.append({
                            "time":         ts_utc,
                            "tariff_type":  tariff_type,
                            "price_chf_kwh": float(item["value"]),
                        })

        return records
