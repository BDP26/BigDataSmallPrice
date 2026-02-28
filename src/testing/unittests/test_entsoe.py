"""
Unit tests for EntsoeCollector.

HTTP is mocked – no real API calls are made.
"""

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

# Ensure src is importable when running pytest from repo root
import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[3]))

from src.data_collection.entsoe_collector import EntsoeCollector


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("ENTSOE_API_TOKEN", "test-token-123")


class TestEntsoeCollector:
    def test_parse_returns_correct_number_of_records(self, sample_entsoe_xml):
        collector = EntsoeCollector()
        records = collector.parse(sample_entsoe_xml)
        assert len(records) == 2

    def test_parse_first_record_timestamp(self, sample_entsoe_xml):
        collector = EntsoeCollector()
        records = collector.parse(sample_entsoe_xml)
        expected = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
        assert records[0]["time"] == expected

    def test_parse_second_record_timestamp(self, sample_entsoe_xml):
        collector = EntsoeCollector()
        records = collector.parse(sample_entsoe_xml)
        expected = datetime(2026, 2, 28, 1, 0, 0, tzinfo=timezone.utc)
        assert records[1]["time"] == expected

    def test_parse_price_values(self, sample_entsoe_xml):
        collector = EntsoeCollector()
        records = collector.parse(sample_entsoe_xml)
        assert records[0]["price_eur_mwh"] == pytest.approx(85.50)
        assert records[1]["price_eur_mwh"] == pytest.approx(92.10)

    def test_parse_currency_and_domain(self, sample_entsoe_xml):
        collector = EntsoeCollector()
        records = collector.parse(sample_entsoe_xml)
        for rec in records:
            assert rec["currency"] == "EUR"
            assert rec["domain"] == "10YCH-SWISSGRIDZ"

    def test_all_timestamps_are_utc_aware(self, sample_entsoe_xml):
        collector = EntsoeCollector()
        records = collector.parse(sample_entsoe_xml)
        for rec in records:
            assert rec["time"].tzinfo is not None

    def test_fetch_calls_api_with_correct_params(self):
        mock_response = MagicMock()
        mock_response.content = b"<root/>"
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_response) as mock_get:
            collector = EntsoeCollector(token="my-token")
            collector.fetch()
            call_kwargs = mock_get.call_args
            params = call_kwargs.kwargs.get("params", call_kwargs.args[1] if len(call_kwargs.args) > 1 else {})
            assert params["securityToken"] == "my-token"
            assert params["documentType"] == "A44"
            assert params["in_Domain"] == "10YCH-SWISSGRIDZ"

    def test_run_validates_utc_aware_timestamps(self, sample_entsoe_xml):
        collector = EntsoeCollector()
        with patch.object(collector, "fetch", return_value=sample_entsoe_xml):
            records = collector.run()
        assert all(r["time"].tzinfo is not None for r in records)
