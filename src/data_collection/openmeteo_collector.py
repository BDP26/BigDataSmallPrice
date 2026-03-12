"""
open-meteo.com – Hourly weather forecast/archive collector.

No API token required.
Coordinates: Winterthur, Switzerland (lat=47.5001, lon=8.7502)

Auto-selects endpoint based on date age:
  - Recent/future (≤5 days ago): api.open-meteo.com/v1/forecast
  - Historical (>5 days ago):    archive-api.open-meteo.com/v1/archive
"""

from datetime import datetime, timezone

from .base_collector import BaseCollector

_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
_LATITUDE = 47.5001
_LONGITUDE = 8.7502
_HOURLY_VARS = "temperature_2m,wind_speed_10m,shortwave_radiation,cloud_cover,precipitation"


def _is_historical(date_str: str) -> bool:
    """Return True if *date_str* ("YYYY-MM-DD") is more than 5 days in the past (UTC)."""
    today = datetime.now(tz=timezone.utc).date()
    target = datetime.fromisoformat(date_str).date()
    return (today - target).days >= 5


class OpenMeteoCollector(BaseCollector):
    """
    Fetches hourly weather data for Winterthur from open-meteo.com.

    When *date* is None or a recent date (≤5 days ago), uses the forecast
    endpoint (``/v1/forecast``).  For historical dates (>5 days ago), uses
    the archive endpoint (``archive-api.open-meteo.com/v1/archive``).

    Args:
        latitude:      Defaults to Winterthur lat.
        longitude:     Defaults to Winterthur lon.
        forecast_days: Number of days to fetch in forecast mode (1-16). Default 2.
        date:          Date string "YYYY-MM-DD". If given and historical, uses
                       archive endpoint; if recent/None, uses forecast endpoint.
    """

    _source_name = "openmeteo"

    def __init__(
        self,
        latitude: float = _LATITUDE,
        longitude: float = _LONGITUDE,
        forecast_days: int = 2,
        date: str | None = None,
    ) -> None:
        self.latitude = latitude
        self.longitude = longitude
        self.forecast_days = forecast_days
        self.date = date

    def fetch(self) -> str:
        if self.date is not None and _is_historical(self.date):
            params = {
                "latitude": self.latitude,
                "longitude": self.longitude,
                "start_date": self.date,
                "end_date": self.date,
                "hourly": _HOURLY_VARS,
                "timezone": "UTC",
            }
            response = self._fetch_with_retry(
                _ARCHIVE_URL,
                params=params,
                source=self._source_name,
                date_fetched=self.date,
            )
        else:
            params = {
                "latitude": self.latitude,
                "longitude": self.longitude,
                "hourly": _HOURLY_VARS,
                "forecast_days": self.forecast_days,
                "timezone": "UTC",
            }
            response = self._fetch_with_retry(
                _FORECAST_URL,
                params=params,
                source=self._source_name,
                date_fetched=self.date,
            )
        return response.text

    def parse(self, raw: bytes | str) -> list[dict]:
        import json

        if isinstance(raw, bytes):
            raw = raw.decode()
        data = json.loads(raw)

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        winds = hourly.get("wind_speed_10m", [])
        radiations = hourly.get("shortwave_radiation", [])
        clouds = hourly.get("cloud_cover", [])
        precip = hourly.get("precipitation", [])

        records: list[dict] = []
        for i, t_str in enumerate(times):
            ts_utc = datetime.fromisoformat(t_str).replace(tzinfo=timezone.utc)
            records.append(
                {
                    "time": ts_utc,
                    "latitude": self.latitude,
                    "longitude": self.longitude,
                    "temperature_2m": _safe_float(temps, i),
                    "wind_speed_10m": _safe_float(winds, i),
                    "shortwave_radiation": _safe_float(radiations, i),
                    "cloud_cover": _safe_float(clouds, i),
                    "precipitation_mm": _safe_float(precip, i),
                }
            )

        return records


def _safe_float(lst: list, idx: int) -> float | None:
    try:
        v = lst[idx]
        return float(v) if v is not None else None
    except (IndexError, TypeError, ValueError):
        return None
