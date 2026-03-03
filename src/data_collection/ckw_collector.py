"""
CKW dynamic tariff collector.

API: https://e-ckw-public-data.de-c1.eu1.cloudhub.io/api/v1/netzinformationen/energie/dynamische-preise
Returns 15-minute dynamic tariff intervals for 4 components:
  grid_usage, grid, electricity, integrated
"""

import json
import logging
from datetime import datetime, timezone

from .base_collector import BaseCollector

_LOG = logging.getLogger(__name__)

_API_URL = (
    "https://e-ckw-public-data.de-c1.eu1.cloudhub.io"
    "/api/v1/netzinformationen/energie/dynamische-preise"
)


class CKWCollector(BaseCollector):
    """
    Fetches CKW dynamic electricity tariffs (15-min intervals).

    Stores all four components (grid_usage, grid, electricity, integrated)
    as separate records keyed by tariff_type.

    Args:
        date:        Date string "YYYY-MM-DD". Defaults to today (UTC).
        tariff_name: Tariff plan name. Defaults to "home_dynamic".
    """

    COMPONENTS = ("grid_usage", "grid", "electricity", "integrated")

    def __init__(
        self,
        date: str | None = None,
        tariff_name: str = "home_dynamic",
    ) -> None:
        self.date = date or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        self.tariff_name = tariff_name

    def fetch(self) -> str:
        start = f"{self.date}T00:00:00+01:00"
        end = f"{self.date}T23:59:59+01:00"
        params = {
            "tariff_name": self.tariff_name,
            "start_timestamp": start,
            "end_timestamp": end,
        }
        response = self._fetch_with_retry(_API_URL, params=params)
        return response.text

    def parse(self, raw: bytes | str) -> list[dict]:
        if isinstance(raw, bytes):
            raw = raw.decode()
        data = json.loads(raw)

        prices = data.get("prices", [])
        if not prices:
            _LOG.warning(
                "CKW parse: 0 records found. Response keys: %s",
                list(data.keys()) if isinstance(data, dict) else type(data).__name__,
            )

        records: list[dict] = []
        for entry in prices:
            ts_raw = entry.get("start_timestamp")
            if ts_raw is None:
                continue
            ts_utc = datetime.fromisoformat(ts_raw).astimezone(timezone.utc)
            for component in self.COMPONENTS:
                for item in entry.get(component, []):
                    if item.get("unit") == "CHF_kWh":
                        records.append({
                            "time": ts_utc,
                            "tariff_type": component,
                            "price_chf_kwh": float(item["value"]),
                        })

        return records
