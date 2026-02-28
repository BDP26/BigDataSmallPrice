"""
Unit tests for BafuCollector (existenz.ch API).

HTTP is mocked – no real API calls are made.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[3]))

from src.data_collection.bafu_collector import BafuCollector

# 1772236800 = 2026-02-28T00:00:00Z
_T0 = datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
# 1772240400 = 2026-02-28T01:00:00Z
_T1 = datetime(2026, 2, 28, 1, 0, 0, tzinfo=timezone.utc)


class TestBafuCollector:
    def test_parse_merges_flow_and_height_into_records(self, sample_bafu_json):
        collector = BafuCollector()
        records = collector.parse(sample_bafu_json)
        # 4 payload entries (2 timestamps × 2 params) → 2 merged records
        assert len(records) == 2

    def test_parse_first_record_timestamp(self, sample_bafu_json):
        collector = BafuCollector()
        records = collector.parse(sample_bafu_json)
        assert records[0]["time"] == _T0

    def test_parse_discharge_and_level_values(self, sample_bafu_json):
        collector = BafuCollector()
        records = collector.parse(sample_bafu_json)
        assert records[0]["discharge_m3s"] == pytest.approx(245.3)
        assert records[0]["level_masl"] == pytest.approx(322.1)

    def test_parse_second_record_values(self, sample_bafu_json):
        collector = BafuCollector()
        records = collector.parse(sample_bafu_json)
        assert records[1]["time"] == _T1
        assert records[1]["discharge_m3s"] == pytest.approx(243.8)
        assert records[1]["level_masl"] == pytest.approx(321.9)

    def test_parse_station_id_attached(self, sample_bafu_json):
        collector = BafuCollector(station_id="2018")
        records = collector.parse(sample_bafu_json)
        for rec in records:
            assert rec["station_id"] == "2018"

    def test_all_timestamps_are_utc_aware(self, sample_bafu_json):
        collector = BafuCollector()
        records = collector.parse(sample_bafu_json)
        for rec in records:
            assert rec["time"].tzinfo is not None

    def test_records_sorted_ascending(self, sample_bafu_json):
        collector = BafuCollector()
        records = collector.parse(sample_bafu_json)
        times = [r["time"] for r in records]
        assert times == sorted(times)

    def test_parse_empty_payload_returns_empty_list(self):
        collector = BafuCollector()
        records = collector.parse('{"payload": []}')
        assert records == []

    def test_fetch_calls_existenz_daterange_endpoint(self):
        mock_response = MagicMock()
        mock_response.text = '{"payload": []}'
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_response) as mock_get:
            collector = BafuCollector(station_id="2018", days_back=2)
            collector.fetch()
            call_args = mock_get.call_args
            url = call_args.args[0]
            assert "existenz.ch" in url
            assert "daterange" in url
            params = call_args.kwargs.get("params", {})
            assert params.get("locations") == "2018"
            assert "flow" in params.get("parameters", "")
            assert "height" in params.get("parameters", "")
