"""
Unit tests for OpenMeteoCollector.

HTTP is mocked – no real API calls are made.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[3]))

from src.data_collection.openmeteo_collector import OpenMeteoCollector


class TestOpenMeteoCollector:
    def test_parse_returns_correct_number_of_records(self, sample_openmeteo_json):
        collector = OpenMeteoCollector()
        records = collector.parse(sample_openmeteo_json)
        assert len(records) == 2

    def test_parse_first_record_timestamp(self, sample_openmeteo_json):
        collector = OpenMeteoCollector()
        records = collector.parse(sample_openmeteo_json)
        expected = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
        assert records[0]["time"] == expected

    def test_parse_all_four_fields_present(self, sample_openmeteo_json):
        collector = OpenMeteoCollector()
        records = collector.parse(sample_openmeteo_json)
        required_fields = {"temperature_2m", "wind_speed_10m", "shortwave_radiation", "cloud_cover"}
        for rec in records:
            assert required_fields.issubset(rec.keys())

    def test_parse_temperature_values(self, sample_openmeteo_json):
        collector = OpenMeteoCollector()
        records = collector.parse(sample_openmeteo_json)
        assert records[0]["temperature_2m"] == pytest.approx(3.5)
        assert records[1]["temperature_2m"] == pytest.approx(4.1)

    def test_parse_wind_speed_values(self, sample_openmeteo_json):
        collector = OpenMeteoCollector()
        records = collector.parse(sample_openmeteo_json)
        assert records[0]["wind_speed_10m"] == pytest.approx(12.3)

    def test_parse_coordinates_attached(self, sample_openmeteo_json):
        collector = OpenMeteoCollector(latitude=47.5001, longitude=8.7502)
        records = collector.parse(sample_openmeteo_json)
        for rec in records:
            assert rec["latitude"] == pytest.approx(47.5001)
            assert rec["longitude"] == pytest.approx(8.7502)

    def test_all_timestamps_are_utc_aware(self, sample_openmeteo_json):
        collector = OpenMeteoCollector()
        records = collector.parse(sample_openmeteo_json)
        for rec in records:
            assert rec["time"].tzinfo is not None

    def test_fetch_calls_correct_url(self):
        mock_response = MagicMock()
        mock_response.text = '{"hourly": {"time": [], "temperature_2m": [], "wind_speed_10m": [], "shortwave_radiation": [], "cloud_cover": []}}'
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_response) as mock_get:
            collector = OpenMeteoCollector()
            collector.fetch()
            url = mock_get.call_args.args[0]
            assert "open-meteo.com" in url
