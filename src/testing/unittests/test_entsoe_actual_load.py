"""
Unit tests for EntsoeActualLoadCollector (ENTSO-E A65 Actual Total Load CH).

HTTP is mocked – no real API calls are made.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[3]))

from src.data_collection.entsoe_collector import EntsoeActualLoadCollector


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("ENTSOE_API_TOKEN", "test-token-123")


@pytest.fixture
def sample_a65_xml() -> bytes:
    """Minimal valid ENTSO-E A65 Actual Total Load XML response with 2 points."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<GL_MarketDocument
    xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
  <TimeSeries>
    <Period>
      <timeInterval>
        <start>2026-02-27T00:00Z</start>
        <end>2026-02-27T02:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <quantity>7850.0</quantity>
      </Point>
      <Point>
        <position>2</position>
        <quantity>7620.5</quantity>
      </Point>
    </Period>
  </TimeSeries>
</GL_MarketDocument>"""


class TestEntsoeActualLoadCollector:
    def test_parse_returns_correct_number_of_records(self, sample_a65_xml):
        collector = EntsoeActualLoadCollector()
        records = collector.parse(sample_a65_xml)
        assert len(records) == 2

    def test_parse_first_record_timestamp(self, sample_a65_xml):
        collector = EntsoeActualLoadCollector()
        records = collector.parse(sample_a65_xml)
        expected = datetime(2026, 2, 27, 0, 0, 0, tzinfo=timezone.utc)
        assert records[0]["time"] == expected

    def test_parse_second_record_timestamp(self, sample_a65_xml):
        collector = EntsoeActualLoadCollector()
        records = collector.parse(sample_a65_xml)
        expected = datetime(2026, 2, 27, 1, 0, 0, tzinfo=timezone.utc)
        assert records[1]["time"] == expected

    def test_parse_load_values(self, sample_a65_xml):
        collector = EntsoeActualLoadCollector()
        records = collector.parse(sample_a65_xml)
        assert records[0]["load_mwh"] == pytest.approx(7850.0)
        assert records[1]["load_mwh"] == pytest.approx(7620.5)

    def test_parse_domain(self, sample_a65_xml):
        collector = EntsoeActualLoadCollector()
        records = collector.parse(sample_a65_xml)
        for rec in records:
            assert rec["domain"] == "10YCH-SWISSGRIDZ"

    def test_all_timestamps_are_utc_aware(self, sample_a65_xml):
        collector = EntsoeActualLoadCollector()
        records = collector.parse(sample_a65_xml)
        for rec in records:
            assert rec["time"].tzinfo is not None

    def test_parse_string_input(self, sample_a65_xml):
        collector = EntsoeActualLoadCollector()
        records = collector.parse(sample_a65_xml.decode())
        assert len(records) == 2

    def test_parse_empty_xml_returns_empty_list(self):
        collector = EntsoeActualLoadCollector()
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<GL_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"/>"""
        records = collector.parse(xml)
        assert records == []

    def test_fetch_calls_api_with_correct_params(self):
        mock_response = MagicMock()
        mock_response.content = b"<root/>"
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_response) as mock_get:
            collector = EntsoeActualLoadCollector(token="my-token")
            collector.fetch()
            call_kwargs = mock_get.call_args
            params = call_kwargs.kwargs.get("params", call_kwargs.args[1] if len(call_kwargs.args) > 1 else {})
            assert params["securityToken"] == "my-token"
            assert params["documentType"] == "A65"
            assert params["processType"] == "A16"
            assert params["outBiddingZone_Domain"] == "10YCH-SWISSGRIDZ"
            assert "in_Domain" not in params
            assert "out_Domain" not in params

    def test_run_validates_utc_aware_timestamps(self, sample_a65_xml):
        collector = EntsoeActualLoadCollector()
        with patch.object(collector, "fetch", return_value=sample_a65_xml):
            records = collector.run()
        assert all(r["time"].tzinfo is not None for r in records)

    def test_records_have_required_keys(self, sample_a65_xml):
        collector = EntsoeActualLoadCollector()
        records = collector.parse(sample_a65_xml)
        for rec in records:
            assert set(rec.keys()) == {"time", "load_mwh", "domain"}
