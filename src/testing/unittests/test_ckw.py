"""
Unit tests for CKWCollector.

HTTP is mocked – no real API calls are made.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[3]))

from src.data_collection.ckw_collector import CKWCollector


class TestCKWCollectorParse:
    def test_parse_returns_correct_number_of_records(self, sample_ckw_json):
        """3 intervals × 4 components = 12 records."""
        collector = CKWCollector()
        records = collector.parse(sample_ckw_json)
        assert len(records) == 12

    def test_parse_first_record_timestamp_converted_to_utc(self, sample_ckw_json):
        """2026-02-28T00:00:00+01:00 → 2026-02-27T23:00:00Z."""
        collector = CKWCollector()
        records = collector.parse(sample_ckw_json)
        expected = datetime(2026, 2, 27, 23, 0, 0, tzinfo=timezone.utc)
        first_ts = records[0]["time"]
        assert first_ts == expected

    def test_parse_all_components_present(self, sample_ckw_json):
        collector = CKWCollector()
        records = collector.parse(sample_ckw_json)
        found_types = {r["tariff_type"] for r in records}
        assert found_types == {"grid_usage", "grid", "electricity", "integrated"}

    def test_parse_integrated_price_value(self, sample_ckw_json):
        collector = CKWCollector()
        records = collector.parse(sample_ckw_json)
        integrated = [r for r in records if r["tariff_type"] == "integrated"]
        assert integrated[0]["price_chf_kwh"] == pytest.approx(0.1600)

    def test_parse_grid_usage_price_value(self, sample_ckw_json):
        collector = CKWCollector()
        records = collector.parse(sample_ckw_json)
        grid_usage = [r for r in records if r["tariff_type"] == "grid_usage"]
        assert grid_usage[0]["price_chf_kwh"] == pytest.approx(0.0500)

    def test_all_timestamps_are_utc_aware(self, sample_ckw_json):
        collector = CKWCollector()
        records = collector.parse(sample_ckw_json)
        for rec in records:
            assert rec["time"].tzinfo is not None

    def test_all_records_have_required_keys(self, sample_ckw_json):
        collector = CKWCollector()
        records = collector.parse(sample_ckw_json)
        for rec in records:
            assert "time" in rec
            assert "tariff_type" in rec
            assert "price_chf_kwh" in rec

    def test_parse_empty_prices_returns_empty_list(self):
        collector = CKWCollector()
        records = collector.parse('{"prices": []}')
        assert records == []

    def test_parse_ignores_non_chf_kwh_units(self):
        """Only CHF_kWh entries should be included."""
        raw = """{
            "prices": [{
                "start_timestamp": "2026-02-28T00:00:00+01:00",
                "grid_usage": [{"unit": "CHF_m", "value": 0.99}]
            }]
        }"""
        collector = CKWCollector()
        records = collector.parse(raw)
        assert records == []

    def test_parse_skips_entry_without_start_timestamp(self):
        raw = """{
            "prices": [{"grid_usage": [{"unit": "CHF_kWh", "value": 0.05}]}]
        }"""
        collector = CKWCollector()
        records = collector.parse(raw)
        assert records == []

    def test_15min_interval_between_records(self, sample_ckw_json):
        collector = CKWCollector()
        records = collector.parse(sample_ckw_json)
        # Records for the same component should be 15 min apart
        grid_usage = sorted(
            [r for r in records if r["tariff_type"] == "grid_usage"],
            key=lambda r: r["time"],
        )
        delta = grid_usage[1]["time"] - grid_usage[0]["time"]
        assert delta.total_seconds() == 15 * 60


class TestCKWCollectorFetch:
    def test_fetch_passes_correct_params(self):
        mock_response = MagicMock()
        mock_response.text = '{"prices": []}'
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_response) as mock_get:
            collector = CKWCollector(date="2026-02-28", tariff_name="home_dynamic")
            collector.fetch()
            params = mock_get.call_args.kwargs.get("params", {})
            assert params.get("tariff_name") == "home_dynamic"
            assert params.get("start_timestamp") == "2026-02-28T00:00:00+01:00"
            assert params.get("end_timestamp") == "2026-02-28T23:59:59+01:00"

    def test_default_date_is_today_utc(self):
        collector = CKWCollector()
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        assert collector.date == today
