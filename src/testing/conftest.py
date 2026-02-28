"""
Shared pytest fixtures for BigDataSmallPrice tests.
"""

import pytest
from datetime import datetime, timezone


@pytest.fixture
def utc_now() -> datetime:
    """Return a fixed UTC-aware datetime for reproducible tests."""
    return datetime(2026, 2, 28, 6, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def sample_entsoe_xml() -> bytes:
    """Minimal valid ENTSO-E Day-Ahead XML response with 2 price points."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument
    xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
  <TimeSeries>
    <currency_Unit.name>EUR</currency_Unit.name>
    <Period>
      <timeInterval>
        <start>2026-02-28T00:00Z</start>
        <end>2026-02-28T02:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <price.amount>85.50</price.amount>
      </Point>
      <Point>
        <position>2</position>
        <price.amount>92.10</price.amount>
      </Point>
    </Period>
  </TimeSeries>
</Publication_MarketDocument>"""


@pytest.fixture
def sample_openmeteo_json() -> str:
    """Minimal open-meteo JSON response with 2 hourly entries."""
    return """{
  "latitude": 47.5001,
  "longitude": 8.7502,
  "hourly": {
    "time": ["2026-02-28T00:00", "2026-02-28T01:00"],
    "temperature_2m": [3.5, 4.1],
    "wind_speed_10m": [12.3, 14.0],
    "shortwave_radiation": [0.0, 0.0],
    "cloud_cover": [80, 75]
  }
}"""


@pytest.fixture
def sample_ekz_json() -> str:
    """Minimal EKZ tariff JSON response with 3 intervals (15-min)."""
    return """{
  "intervals": [
    {"startTime": "2026-02-28T00:00:00Z", "price": 0.1234},
    {"startTime": "2026-02-28T00:15:00Z", "price": 0.1250},
    {"startTime": "2026-02-28T00:30:00Z", "price": 0.1199}
  ]
}"""


@pytest.fixture
def sample_bafu_json() -> str:
    """Minimal BAFU hydro JSON response."""
    return """{
  "measurements": [
    {"timestamp": "2026-02-28T00:00:00Z", "discharge": 245.3, "level": 322.1},
    {"timestamp": "2026-02-28T01:00:00Z", "discharge": 243.8, "level": 321.9}
  ]
}"""
