"""
Unit tests for EkzCollector (updated API: tariff_type/tariff_name params).

HTTP is mocked – no real API calls are made.
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[3]))

from src.data_collection.ekz_collector import EkzCollector


class TestEkzCollectorParse:
    def test_parse_returns_correct_number_of_records(self, sample_ekz_json):
        """3 electricity + 3 integrated entries = 6 records."""
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        assert len(records) == 6

    def test_parse_both_components_present(self, sample_ekz_json):
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        found_types = {r["tariff_type"] for r in records}
        assert found_types == {"electricity", "integrated"}

    def test_parse_first_electricity_timestamp_utc(self, sample_ekz_json):
        """2026-02-28T00:00:00+01:00 → 2026-02-27T23:00:00Z."""
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        electricity = [r for r in records if r["tariff_type"] == "electricity"]
        expected = datetime(2026, 2, 27, 23, 0, 0, tzinfo=timezone.utc)
        assert electricity[0]["time"] == expected

    def test_parse_electricity_price_value(self, sample_ekz_json):
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        electricity = [r for r in records if r["tariff_type"] == "electricity"]
        assert electricity[0]["price_chf_kwh"] == pytest.approx(0.1192)

    def test_parse_integrated_price_value(self, sample_ekz_json):
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        integrated = [r for r in records if r["tariff_type"] == "integrated"]
        assert integrated[0]["price_chf_kwh"] == pytest.approx(0.2352)

    def test_all_timestamps_are_utc_aware(self, sample_ekz_json):
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        for rec in records:
            assert rec["time"].tzinfo is not None

    def test_all_records_have_required_keys(self, sample_ekz_json):
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        for rec in records:
            assert "time" in rec
            assert "tariff_type" in rec
            assert "price_chf_kwh" in rec

    def test_parse_empty_prices_returns_empty_list(self):
        collector = EkzCollector()
        records = collector.parse('{"prices": []}')
        assert records == []

    def test_parse_ignores_non_chf_kwh_units(self):
        raw = """{
            "prices": [{
                "start_timestamp": "2026-02-28T00:00:00+01:00",
                "electricity": [{"unit": "CHF_m", "value": 3.0}]
            }]
        }"""
        collector = EkzCollector()
        records = collector.parse(raw)
        assert records == []

    def test_parse_skips_entry_without_start_timestamp(self):
        raw = '{"prices": [{"electricity": [{"unit": "CHF_kWh", "value": 0.12}]}]}'
        collector = EkzCollector()
        records = collector.parse(raw)
        assert records == []

    def test_15min_interval_between_records(self, sample_ekz_json):
        collector = EkzCollector()
        records = collector.parse(sample_ekz_json)
        electricity = sorted(
            [r for r in records if r["tariff_type"] == "electricity"],
            key=lambda r: r["time"],
        )
        delta = electricity[1]["time"] - electricity[0]["time"]
        assert delta.total_seconds() == 15 * 60


class TestEkzCollectorFetch:
    def _make_mock_response(self, prices: list) -> MagicMock:
        mock = MagicMock()
        mock.text = json.dumps({"prices": prices})
        mock.status_code = 200
        mock.raise_for_status = MagicMock()
        return mock

    def test_fetch_makes_two_api_calls(self):
        """fetch() must call the API twice: electricity_dynamic + integrated_400D."""
        mock_resp = self._make_mock_response([])
        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_resp) as mock_get:
            EkzCollector(date="2026-02-28").fetch()
            assert mock_get.call_count == 2

    def test_fetch_calls_electricity_dynamic(self):
        mock_resp = self._make_mock_response([])
        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_resp) as mock_get:
            EkzCollector(date="2026-02-28").fetch()
            first_params = mock_get.call_args_list[0].kwargs["params"]
            assert first_params["tariff_type"] == "electricity"
            assert first_params["tariff_name"] == "electricity_dynamic"

    def test_fetch_calls_integrated_400d(self):
        mock_resp = self._make_mock_response([])
        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_resp) as mock_get:
            EkzCollector(date="2026-02-28").fetch()
            second_params = mock_get.call_args_list[1].kwargs["params"]
            assert second_params["tariff_type"] == "integrated"
            assert second_params["tariff_name"] == "integrated_400D"

    def test_fetch_uses_start_end_timestamp_params(self):
        mock_resp = self._make_mock_response([])
        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_resp) as mock_get:
            EkzCollector(date="2026-02-28").fetch()
            params = mock_get.call_args_list[0].kwargs["params"]
            assert params["start_timestamp"] == "2026-02-28T00:00:00+01:00"
            assert params["end_timestamp"]   == "2026-02-28T23:59:59+01:00"

    def test_fetch_combines_both_responses(self):
        """Combined prices list contains entries from both API calls."""
        electricity_entry = {"start_timestamp": "2026-02-28T00:00:00+01:00",
                             "electricity": [{"unit": "CHF_kWh", "value": 0.12}]}
        integrated_entry  = {"start_timestamp": "2026-02-28T00:00:00+01:00",
                             "integrated":  [{"unit": "CHF_kWh", "value": 0.24}]}

        responses = [
            self._make_mock_response([electricity_entry]),
            self._make_mock_response([integrated_entry]),
        ]
        with patch("src.data_collection.base_collector.httpx.get", side_effect=responses):
            result = json.loads(EkzCollector(date="2026-02-28").fetch())
            assert len(result["prices"]) == 2

    def test_default_date_is_today_utc(self):
        collector = EkzCollector()
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        assert collector.date == today
