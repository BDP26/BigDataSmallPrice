"""
Unit tests for EntsoeGenerationCollector, EntsoeCrossBorderFlowCollector,
and EntsoeLoadForecastCollector.

HTTP is mocked – no real API calls are made.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[3]))

from src.data_collection.entsoe_collector import (
    EntsoeGenerationCollector,
    EntsoeCrossBorderFlowCollector,
    EntsoeLoadForecastCollector,
)


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("ENTSOE_API_TOKEN", "test-token-456")


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_a75_xml() -> bytes:
    """Minimal valid ENTSO-E A75 Generation Per Type XML with 2 points (B16 solar CH)."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<GL_MarketDocument
    xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
  <TimeSeries>
    <Period>
      <timeInterval>
        <start>2026-03-11T00:00Z</start>
        <end>2026-03-11T02:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <quantity>120.5</quantity>
      </Point>
      <Point>
        <position>2</position>
        <quantity>245.0</quantity>
      </Point>
    </Period>
  </TimeSeries>
</GL_MarketDocument>"""


@pytest.fixture
def sample_a11_xml() -> bytes:
    """Minimal valid ENTSO-E A11 Cross-Border Flows XML with 2 points."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<GL_MarketDocument
    xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
  <TimeSeries>
    <Period>
      <timeInterval>
        <start>2026-03-11T00:00Z</start>
        <end>2026-03-11T02:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <quantity>350.0</quantity>
      </Point>
      <Point>
        <position>2</position>
        <quantity>410.5</quantity>
      </Point>
    </Period>
  </TimeSeries>
</GL_MarketDocument>"""


@pytest.fixture
def sample_a65_a01_xml() -> bytes:
    """Minimal valid ENTSO-E A65/A01 Day-Ahead Load Forecast XML with 2 points."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<GL_MarketDocument
    xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
  <TimeSeries>
    <Period>
      <timeInterval>
        <start>2026-03-12T00:00Z</start>
        <end>2026-03-12T02:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <quantity>8100.0</quantity>
      </Point>
      <Point>
        <position>2</position>
        <quantity>7950.5</quantity>
      </Point>
    </Period>
  </TimeSeries>
</GL_MarketDocument>"""


@pytest.fixture
def empty_xml() -> bytes:
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<GL_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"/>"""


# ─── EntsoeGenerationCollector tests ──────────────────────────────────────────

class TestEntsoeGenerationCollector:
    def test_parse_returns_correct_number_of_records(self, sample_a75_xml):
        collector = EntsoeGenerationCollector(domain="10YCH-SWISSGRIDZ", psr_type="B16")
        records = collector.parse(sample_a75_xml)
        assert len(records) == 2

    def test_parse_first_record_timestamp(self, sample_a75_xml):
        collector = EntsoeGenerationCollector(domain="10YCH-SWISSGRIDZ", psr_type="B16")
        records = collector.parse(sample_a75_xml)
        expected = datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc)
        assert records[0]["time"] == expected

    def test_parse_second_record_timestamp(self, sample_a75_xml):
        collector = EntsoeGenerationCollector(domain="10YCH-SWISSGRIDZ", psr_type="B16")
        records = collector.parse(sample_a75_xml)
        expected = datetime(2026, 3, 11, 1, 0, 0, tzinfo=timezone.utc)
        assert records[1]["time"] == expected

    def test_parse_quantity_values(self, sample_a75_xml):
        collector = EntsoeGenerationCollector(domain="10YCH-SWISSGRIDZ", psr_type="B16")
        records = collector.parse(sample_a75_xml)
        assert records[0]["quantity_mwh"] == pytest.approx(120.5)
        assert records[1]["quantity_mwh"] == pytest.approx(245.0)

    def test_parse_domain_field(self, sample_a75_xml):
        collector = EntsoeGenerationCollector(domain="10YCH-SWISSGRIDZ", psr_type="B16")
        records = collector.parse(sample_a75_xml)
        for rec in records:
            assert rec["domain"] == "10YCH-SWISSGRIDZ"

    def test_parse_psr_type_field(self, sample_a75_xml):
        collector = EntsoeGenerationCollector(domain="10YCH-SWISSGRIDZ", psr_type="B16")
        records = collector.parse(sample_a75_xml)
        for rec in records:
            assert rec["psr_type"] == "B16"

    def test_all_timestamps_are_utc_aware(self, sample_a75_xml):
        collector = EntsoeGenerationCollector(domain="10YCH-SWISSGRIDZ", psr_type="B16")
        records = collector.parse(sample_a75_xml)
        for rec in records:
            assert rec["time"].tzinfo is not None

    def test_records_have_required_keys(self, sample_a75_xml):
        collector = EntsoeGenerationCollector(domain="10YCH-SWISSGRIDZ", psr_type="B16")
        records = collector.parse(sample_a75_xml)
        for rec in records:
            assert set(rec.keys()) == {"time", "domain", "psr_type", "quantity_mwh"}

    def test_parse_string_input(self, sample_a75_xml):
        collector = EntsoeGenerationCollector(domain="10YCH-SWISSGRIDZ", psr_type="B16")
        records = collector.parse(sample_a75_xml.decode())
        assert len(records) == 2

    def test_parse_empty_xml_returns_empty_list(self, empty_xml):
        collector = EntsoeGenerationCollector(domain="10YCH-SWISSGRIDZ", psr_type="B16")
        records = collector.parse(empty_xml)
        assert records == []

    def test_parse_de_domain_b19(self, sample_a75_xml):
        collector = EntsoeGenerationCollector(domain="10Y1001A1001A83F", psr_type="B19")
        records = collector.parse(sample_a75_xml)
        for rec in records:
            assert rec["domain"] == "10Y1001A1001A83F"
            assert rec["psr_type"] == "B19"

    def test_fetch_calls_api_with_correct_params(self):
        mock_response = MagicMock()
        mock_response.content = b"<root/>"
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_response) as mock_get:
            collector = EntsoeGenerationCollector(
                domain="10YCH-SWISSGRIDZ",
                psr_type="B16",
                token="my-token",
            )
            collector.fetch()
            call_kwargs = mock_get.call_args
            params = call_kwargs.kwargs.get("params", call_kwargs.args[1] if len(call_kwargs.args) > 1 else {})
            assert params["securityToken"] == "my-token"
            assert params["documentType"] == "A75"
            assert params["processType"] == "A16"
            assert params["in_Domain"] == "10YCH-SWISSGRIDZ"
            assert params["psrType"] == "B16"

    def test_fetch_uses_de_domain(self):
        mock_response = MagicMock()
        mock_response.content = b"<root/>"
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_response) as mock_get:
            collector = EntsoeGenerationCollector(
                domain="10Y1001A1001A83F",
                psr_type="B19",
                token="my-token",
            )
            collector.fetch()
            call_kwargs = mock_get.call_args
            params = call_kwargs.kwargs.get("params", call_kwargs.args[1] if len(call_kwargs.args) > 1 else {})
            assert params["in_Domain"] == "10Y1001A1001A83F"
            assert params["psrType"] == "B19"


# ─── EntsoeCrossBorderFlowCollector tests ─────────────────────────────────────

class TestEntsoeCrossBorderFlowCollector:
    def test_parse_returns_correct_number_of_records(self, sample_a11_xml):
        collector = EntsoeCrossBorderFlowCollector(
            in_domain="10YCH-SWISSGRIDZ",
            out_domain="10Y1001A1001A83F",
        )
        records = collector.parse(sample_a11_xml)
        assert len(records) == 2

    def test_parse_first_record_timestamp(self, sample_a11_xml):
        collector = EntsoeCrossBorderFlowCollector(
            in_domain="10YCH-SWISSGRIDZ",
            out_domain="10Y1001A1001A83F",
        )
        records = collector.parse(sample_a11_xml)
        expected = datetime(2026, 3, 11, 0, 0, 0, tzinfo=timezone.utc)
        assert records[0]["time"] == expected

    def test_parse_second_record_timestamp(self, sample_a11_xml):
        collector = EntsoeCrossBorderFlowCollector(
            in_domain="10YCH-SWISSGRIDZ",
            out_domain="10Y1001A1001A83F",
        )
        records = collector.parse(sample_a11_xml)
        expected = datetime(2026, 3, 11, 1, 0, 0, tzinfo=timezone.utc)
        assert records[1]["time"] == expected

    def test_parse_flow_values(self, sample_a11_xml):
        collector = EntsoeCrossBorderFlowCollector(
            in_domain="10YCH-SWISSGRIDZ",
            out_domain="10Y1001A1001A83F",
        )
        records = collector.parse(sample_a11_xml)
        assert records[0]["flow_mwh"] == pytest.approx(350.0)
        assert records[1]["flow_mwh"] == pytest.approx(410.5)

    def test_parse_in_domain_field(self, sample_a11_xml):
        collector = EntsoeCrossBorderFlowCollector(
            in_domain="10YCH-SWISSGRIDZ",
            out_domain="10Y1001A1001A83F",
        )
        records = collector.parse(sample_a11_xml)
        for rec in records:
            assert rec["in_domain"] == "10YCH-SWISSGRIDZ"

    def test_parse_out_domain_field(self, sample_a11_xml):
        collector = EntsoeCrossBorderFlowCollector(
            in_domain="10YCH-SWISSGRIDZ",
            out_domain="10Y1001A1001A83F",
        )
        records = collector.parse(sample_a11_xml)
        for rec in records:
            assert rec["out_domain"] == "10Y1001A1001A83F"

    def test_all_timestamps_are_utc_aware(self, sample_a11_xml):
        collector = EntsoeCrossBorderFlowCollector(
            in_domain="10YCH-SWISSGRIDZ",
            out_domain="10Y1001A1001A83F",
        )
        records = collector.parse(sample_a11_xml)
        for rec in records:
            assert rec["time"].tzinfo is not None

    def test_records_have_required_keys(self, sample_a11_xml):
        collector = EntsoeCrossBorderFlowCollector(
            in_domain="10YCH-SWISSGRIDZ",
            out_domain="10Y1001A1001A83F",
        )
        records = collector.parse(sample_a11_xml)
        for rec in records:
            assert set(rec.keys()) == {"time", "in_domain", "out_domain", "flow_mwh"}

    def test_parse_string_input(self, sample_a11_xml):
        collector = EntsoeCrossBorderFlowCollector(
            in_domain="10YCH-SWISSGRIDZ",
            out_domain="10Y1001A1001A83F",
        )
        records = collector.parse(sample_a11_xml.decode())
        assert len(records) == 2

    def test_parse_empty_xml_returns_empty_list(self, empty_xml):
        collector = EntsoeCrossBorderFlowCollector(
            in_domain="10YCH-SWISSGRIDZ",
            out_domain="10Y1001A1001A83F",
        )
        records = collector.parse(empty_xml)
        assert records == []

    def test_fetch_calls_api_with_correct_params(self):
        mock_response = MagicMock()
        mock_response.content = b"<root/>"
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_response) as mock_get:
            collector = EntsoeCrossBorderFlowCollector(
                in_domain="10YCH-SWISSGRIDZ",
                out_domain="10Y1001A1001A83F",
                token="my-token",
            )
            collector.fetch()
            call_kwargs = mock_get.call_args
            params = call_kwargs.kwargs.get("params", call_kwargs.args[1] if len(call_kwargs.args) > 1 else {})
            assert params["securityToken"] == "my-token"
            assert params["documentType"] == "A11"
            assert params["in_Domain"] == "10YCH-SWISSGRIDZ"
            assert params["out_Domain"] == "10Y1001A1001A83F"

    def test_fetch_reverse_direction(self):
        mock_response = MagicMock()
        mock_response.content = b"<root/>"
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_response) as mock_get:
            collector = EntsoeCrossBorderFlowCollector(
                in_domain="10Y1001A1001A83F",
                out_domain="10YCH-SWISSGRIDZ",
                token="my-token",
            )
            collector.fetch()
            call_kwargs = mock_get.call_args
            params = call_kwargs.kwargs.get("params", call_kwargs.args[1] if len(call_kwargs.args) > 1 else {})
            assert params["in_Domain"] == "10Y1001A1001A83F"
            assert params["out_Domain"] == "10YCH-SWISSGRIDZ"


# ─── EntsoeLoadForecastCollector tests ────────────────────────────────────────

class TestEntsoeLoadForecastCollector:
    def test_parse_returns_correct_number_of_records(self, sample_a65_a01_xml):
        collector = EntsoeLoadForecastCollector()
        records = collector.parse(sample_a65_a01_xml)
        assert len(records) == 2

    def test_parse_first_record_timestamp(self, sample_a65_a01_xml):
        collector = EntsoeLoadForecastCollector()
        records = collector.parse(sample_a65_a01_xml)
        expected = datetime(2026, 3, 12, 0, 0, 0, tzinfo=timezone.utc)
        assert records[0]["time"] == expected

    def test_parse_second_record_timestamp(self, sample_a65_a01_xml):
        collector = EntsoeLoadForecastCollector()
        records = collector.parse(sample_a65_a01_xml)
        expected = datetime(2026, 3, 12, 1, 0, 0, tzinfo=timezone.utc)
        assert records[1]["time"] == expected

    def test_parse_load_values(self, sample_a65_a01_xml):
        collector = EntsoeLoadForecastCollector()
        records = collector.parse(sample_a65_a01_xml)
        assert records[0]["load_mwh"] == pytest.approx(8100.0)
        assert records[1]["load_mwh"] == pytest.approx(7950.5)

    def test_parse_domain_field(self, sample_a65_a01_xml):
        collector = EntsoeLoadForecastCollector()
        records = collector.parse(sample_a65_a01_xml)
        for rec in records:
            assert rec["domain"] == "10YCH-SWISSGRIDZ"

    def test_all_timestamps_are_utc_aware(self, sample_a65_a01_xml):
        collector = EntsoeLoadForecastCollector()
        records = collector.parse(sample_a65_a01_xml)
        for rec in records:
            assert rec["time"].tzinfo is not None

    def test_records_have_required_keys(self, sample_a65_a01_xml):
        collector = EntsoeLoadForecastCollector()
        records = collector.parse(sample_a65_a01_xml)
        for rec in records:
            assert set(rec.keys()) == {"time", "domain", "load_mwh"}

    def test_parse_string_input(self, sample_a65_a01_xml):
        collector = EntsoeLoadForecastCollector()
        records = collector.parse(sample_a65_a01_xml.decode())
        assert len(records) == 2

    def test_parse_empty_xml_returns_empty_list(self, empty_xml):
        collector = EntsoeLoadForecastCollector()
        records = collector.parse(empty_xml)
        assert records == []

    def test_fetch_calls_api_with_correct_params(self):
        mock_response = MagicMock()
        mock_response.content = b"<root/>"
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("src.data_collection.base_collector.httpx.get", return_value=mock_response) as mock_get:
            collector = EntsoeLoadForecastCollector(token="my-token")
            collector.fetch()
            call_kwargs = mock_get.call_args
            params = call_kwargs.kwargs.get("params", call_kwargs.args[1] if len(call_kwargs.args) > 1 else {})
            assert params["securityToken"] == "my-token"
            assert params["documentType"] == "A65"
            assert params["processType"] == "A01"
            assert params["outBiddingZone_Domain"] == "10YCH-SWISSGRIDZ"
            assert "in_Domain" not in params
            assert "out_Domain" not in params
            assert "psrType" not in params

    def test_run_validates_utc_aware_timestamps(self, sample_a65_a01_xml):
        collector = EntsoeLoadForecastCollector()
        with patch.object(collector, "fetch", return_value=sample_a65_a01_xml):
            records = collector.run()
        assert all(r["time"].tzinfo is not None for r in records)

    def test_default_domain_is_ch(self):
        collector = EntsoeLoadForecastCollector()
        assert collector.domain == "10YCH-SWISSGRIDZ"
