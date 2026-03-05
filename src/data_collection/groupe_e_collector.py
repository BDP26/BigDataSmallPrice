"""
Groupe E dynamic tariff collector.

API: https://api.tariffs.groupe-e.ch/v2/tariffs
Returns 15-minute dynamic tariff intervals for 2 components:
  grid, integrated
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .base_collector import BaseCollector

_LOG = logging.getLogger(__name__)

_API_URL = "https://api.tariffs.groupe-e.ch/v2/tariffs"
_CH_TZ = ZoneInfo("Europe/Zurich")


class GroupeECollector(BaseCollector):
    """
    Fetches Groupe E dynamic electricity tariffs (15-min intervals).

    Stores both components (grid, integrated) as separate records
    keyed by tariff_type.

    Args:
        date: Date string "YYYY-MM-DD". Defaults to today (UTC).
    """

    _source_name = "groupe_e"
    COMPONENTS = ("grid", "integrated")

    def __init__(self, date: str | None = None) -> None:
        self.date = date or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    def fetch(self) -> str:
        # Groupe E v2 supports timestamp range params reliably for historical data.
        day_start_local = datetime.fromisoformat(self.date).replace(tzinfo=_CH_TZ)
        day_end_local = day_start_local + timedelta(days=1)
        params = {
            "start_timestamp": day_start_local.isoformat(timespec="seconds"),
            "end_timestamp": day_end_local.isoformat(timespec="seconds"),
        }
        response = self._fetch_with_retry(
            _API_URL, params=params,
            source=self._source_name, date_fetched=self.date,
        )
        return response.text

    def parse(self, raw: bytes | str) -> list[dict]:
        if isinstance(raw, bytes):
            raw = raw.decode()
        data = json.loads(raw)

        prices = data.get("prices", [])
        if not prices:
            _LOG.warning(
                "Groupe E parse: 0 records found. Response keys: %s",
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
