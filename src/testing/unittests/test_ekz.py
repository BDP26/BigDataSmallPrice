"""
Unit tests for EkzCollector.

HTTP is mocked – no real API calls are made.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[3]))

from src.data_collection.ekz_collector import EkzCollector


class TestEkzCollector:
    def test_parse_returns_correct_number_of_records(self, sample_ekz_json):
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        assert len(records) == 3

    def test_parse_first_record_timestamp(self, sample_ekz_json):
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        expected = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
        assert records[0]["time"] == expected

    def test_parse_15min_intervals(self, sample_ekz_json):
        """Records should be 15 minutes apart."""
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        delta = records[1]["time"] - records[0]["time"]
        assert delta.total_seconds() == 15 * 60

    def test_parse_price_values(self, sample_ekz_json):
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        assert records[0]["price_chf_kwh"] == pytest.approx(0.1234)
        assert records[1]["price_chf_kwh"] == pytest.approx(0.1250)

    def test_parse_tariff_type_attached(self, sample_ekz_json):
        collector = EkzCollector(tariff_type="dynamic")
        records = collector.parse(sample_ekz_json)
        for rec in records:
            assert rec["tariff_type"] == "dynamic"

    def test_all_timestamps_are_utc_aware(self, sample_ekz_json):
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        for rec in records:
            assert rec["time"].tzinfo is not None

    def test_fetch_passes_date_param(self):
        mock_response = MagicMock()
        mock_response.text = '{"prices": []}'
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_response) as mock_get:
            collector = EkzCollector(date="2026-02-28")
            collector.fetch()
            params = mock_get.call_args.kwargs.get("params", {})
            assert params.get("date") == "2026-02-28"
            assert params.get("tariffType") == "dynamic"
