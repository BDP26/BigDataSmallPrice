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
    """Minimal EKZ tariff JSON response with 3 intervals (15-min), matching real API structure."""
    return """{
  "publication_timestamp": "2025-12-18T14:12:57+01:00",
  "prices": [
    {
      "start_timestamp": "2026-02-28T00:00:00+00:00",
      "end_timestamp": "2026-02-28T00:15:00+00:00",
      "electricity": [{"unit": "CHF_m", "value": 3.0}, {"unit": "CHF_kWh", "value": 0.1234}]
    },
    {
      "start_timestamp": "2026-02-28T00:15:00+00:00",
      "end_timestamp": "2026-02-28T00:30:00+00:00",
      "electricity": [{"unit": "CHF_m", "value": 3.0}, {"unit": "CHF_kWh", "value": 0.1250}]
    },
    {
      "start_timestamp": "2026-02-28T00:30:00+00:00",
      "end_timestamp": "2026-02-28T00:45:00+00:00",
      "electricity": [{"unit": "CHF_m", "value": 3.0}, {"unit": "CHF_kWh", "value": 0.1199}]
    }
  ]
}"""


@pytest.fixture
def sample_bafu_json() -> str:
    """Minimal existenz.ch hydro JSON response (Unix timestamps, flow+height per slot)."""
    # 1772236800 = 2026-02-28T00:00:00Z, 1772240400 = 2026-02-28T01:00:00Z
    return """{
  "payload": [
    {"timestamp": 1772236800, "loc": "2018", "par": "flow",   "val": 245.3},
    {"timestamp": 1772236800, "loc": "2018", "par": "height", "val": 322.1},
    {"timestamp": 1772240400, "loc": "2018", "par": "flow",   "val": 243.8},
    {"timestamp": 1772240400, "loc": "2018", "par": "height", "val": 321.9}
  ]
}"""
