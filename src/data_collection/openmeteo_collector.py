"""
open-meteo.com – Hourly weather forecast collector.

No API token required.
Coordinates: Winterthur, Switzerland (lat=47.5001, lon=8.7502)
"""

from datetime import datetime, timezone

from .base_collector import BaseCollector

_API_URL = "https://api.open-meteo.com/v1/forecast"
_LATITUDE = 47.5001
_LONGITUDE = 8.7502
_HOURLY_VARS = "temperature_2m,wind_speed_10m,shortwave_radiation,cloud_cover"


class OpenMeteoCollector(BaseCollector):
    """
    Fetches hourly weather data for Winterthur from open-meteo.com.

    Args:
        latitude:  Defaults to Winterthur lat.
        longitude: Defaults to Winterthur lon.
        forecast_days: Number of days to fetch (1-16). Defaults to 2.
    """

    def __init__(
        self,
        latitude: float = _LATITUDE,
        longitude: float = _LONGITUDE,
        forecast_days: int = 2,
    ) -> None:
        self.latitude = latitude
        self.longitude = longitude
        self.forecast_days = forecast_days

    def fetch(self) -> str:
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "hourly": _HOURLY_VARS,
            "forecast_days": self.forecast_days,
            "timezone": "UTC",
        }
        response = self._fetch_with_retry(_API_URL, params=params)
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
                }
            )

        return records


def _safe_float(lst: list, idx: int) -> float | None:
    try:
        v = lst[idx]
        return float(v) if v is not None else None
    except (IndexError, TypeError, ValueError):
        return None
