"""
Unit tests for OpenMeteoCollector archive/forecast endpoint auto-switching.

Tests verify:
  - _is_historical() flag logic
  - Correct URL selection based on date age
  - Parse method works identically for both endpoints
"""

import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parents[3]))

from src.data_collection.openmeteo_collector import (
    OpenMeteoCollector,
    _is_historical,
)

# ─── Fixtures ─────────────────────────────────────────────────────────────────

_ARCHIVE_RESPONSE = {
    "hourly": {
        "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
        "temperature_2m": [5.2, 4.8],
        "wind_speed_10m": [10.1, 9.5],
        "shortwave_radiation": [0.0, 0.0],
        "cloud_cover": [80.0, 75.0],
        "precipitation": [0.1, 0.0],
    }
}


def _today_utc():
    return datetime.now(tz=timezone.utc).date()


def _date_str(delta_days: int) -> str:
    """Return an ISO date string offset by *delta_days* from today."""
    d = _today_utc() + timedelta(days=delta_days)
    return d.isoformat()


# ─── _is_historical flag ──────────────────────────────────────────────────────


class TestIsHistoricalFlag:
    def test_historical_flag_true(self):
        """Date 10 days ago → historical."""
        assert _is_historical(_date_str(-10)) is True

    def test_historical_flag_false(self):
        """Tomorrow → not historical (forecast)."""
        assert _is_historical(_date_str(1)) is False

    def test_historical_flag_boundary(self):
        """Exactly 5 days ago → boundary → treated as historical."""
        assert _is_historical(_date_str(-5)) is True

    def test_historical_flag_four_days_ago(self):
        """4 days ago → not historical (within rolling window)."""
        assert _is_historical(_date_str(-4)) is False

    def test_historical_flag_today(self):
        """Today → not historical."""
        assert _is_historical(_date_str(0)) is False


# ─── URL selection ────────────────────────────────────────────────────────────


class TestUrlSelection:
    def _mock_response(self):
        mock = MagicMock()
        mock.text = json.dumps(_ARCHIVE_RESPONSE)
        return mock

    def test_forecast_collector_uses_forecast_url(self):
        """No date given → forecast endpoint."""
        collector = OpenMeteoCollector()
        with patch(
            "src.data_collection.base_collector.httpx.get",
            return_value=self._mock_response(),
        ) as mock_get:
            collector.fetch()
        called_url = mock_get.call_args[0][0]
        assert "open-meteo.com/v1/forecast" in called_url

    def test_archive_collector_uses_archive_url(self):
        """Date 30 days ago → archive endpoint."""
        collector = OpenMeteoCollector(date=_date_str(-30))
        with patch(
            "src.data_collection.base_collector.httpx.get",
            return_value=self._mock_response(),
        ) as mock_get:
            collector.fetch()
        called_url = mock_get.call_args[0][0]
        assert "archive-api.open-meteo.com/v1/archive" in called_url

    def test_recent_date_uses_forecast_url(self):
        """Date 2 days ago → forecast endpoint."""
        collector = OpenMeteoCollector(date=_date_str(-2))
        with patch(
            "src.data_collection.base_collector.httpx.get",
            return_value=self._mock_response(),
        ) as mock_get:
            collector.fetch()
        called_url = mock_get.call_args[0][0]
        assert "open-meteo.com/v1/forecast" in called_url


# ─── Parse ────────────────────────────────────────────────────────────────────


class TestParseArchiveResponse:
    def test_parse_archive_response(self):
        """parse() returns correctly structured records from archive fixture."""
        collector = OpenMeteoCollector()
        records = collector.parse(json.dumps(_ARCHIVE_RESPONSE))

        assert len(records) == 2
        r = records[0]
        assert r["time"] == datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        assert r["temperature_2m"] == pytest.approx(5.2)
        assert r["wind_speed_10m"] == pytest.approx(10.1)
        assert r["shortwave_radiation"] == pytest.approx(0.0)
        assert r["cloud_cover"] == pytest.approx(80.0)
        assert r["precipitation_mm"] == pytest.approx(0.1)
        assert r["latitude"] == pytest.approx(47.5001)
        assert r["longitude"] == pytest.approx(8.7502)
