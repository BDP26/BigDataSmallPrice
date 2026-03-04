"""
Unit tests for stadtwerk_winterthur_collector.py.

All tests work on synthetic CSV strings – no HTTP calls are made.
"""

from datetime import datetime, timezone

import pytest

from data_collection.stadtwerk_winterthur_collector import (
    BruttolastgangCollector,
    NetzEinspeisungCollector,
    _parse_timestamp,
)

# ─── Fixtures ─────────────────────────────────────────────────────────────────

BRUTTO_CSV = """\
zeitpunkt,bruttolastgang_kwh
2022-01-01T00:15:00+0100,1100.5
2022-01-01T00:30:00+0100,1050.0
2022-01-01T00:45:00+0100,980.25
"""

EINSPEISUNG_CSV = """\
zeitpunkt,energietraeger,lastgang_kwh
2022-06-15T10:00:00+0200,photovoltaik,250.0
2022-06-15T10:00:00+0200,thermisch,180.0
2022-06-15T10:00:00+0200,wind,12.5
2022-06-15T11:00:00+0200,photovoltaik,310.5
2022-06-15T11:00:00+0200,thermisch,195.0
"""

EMPTY_CSV = ""

CSV_WITH_COLON_OFFSET = """\
zeitpunkt,bruttolastgang_kwh
2022-03-01T08:00:00+01:00,500.0
"""

# ─── _parse_timestamp tests ───────────────────────────────────────────────────


def test_parse_timestamp_with_compact_offset():
    dt = _parse_timestamp("2022-01-01T00:15:00+0100")
    assert dt.tzinfo is not None
    assert dt == datetime(2021, 12, 31, 23, 15, tzinfo=timezone.utc)


def test_parse_timestamp_with_colon_offset():
    dt = _parse_timestamp("2022-03-01T08:00:00+01:00")
    assert dt == datetime(2022, 3, 1, 7, 0, tzinfo=timezone.utc)


def test_parse_timestamp_returns_utc():
    dt = _parse_timestamp("2022-06-15T10:00:00+0200")
    assert dt.tzinfo == timezone.utc
    assert dt == datetime(2022, 6, 15, 8, 0, tzinfo=timezone.utc)


# ─── BruttolastgangCollector.parse tests ─────────────────────────────────────


def test_parse_bruttolastgang_converts_timestamps_to_utc():
    col = BruttolastgangCollector()
    records = col.parse(BRUTTO_CSV)
    assert len(records) == 3
    for rec in records:
        assert rec["time"].tzinfo == timezone.utc


def test_parse_bruttolastgang_extracts_load_kwh():
    col = BruttolastgangCollector()
    records = col.parse(BRUTTO_CSV)
    assert records[0]["load_kwh"] == pytest.approx(1100.5)
    assert records[1]["load_kwh"] == pytest.approx(1050.0)
    assert records[2]["load_kwh"] == pytest.approx(980.25)


def test_parse_bruttolastgang_first_record_utc_correct():
    col = BruttolastgangCollector()
    records = col.parse(BRUTTO_CSV)
    # 00:15 CET (+01:00) → 23:15 UTC previous day
    assert records[0]["time"] == datetime(2021, 12, 31, 23, 15, tzinfo=timezone.utc)


def test_parse_bruttolastgang_colon_offset():
    col = BruttolastgangCollector()
    records = col.parse(CSV_WITH_COLON_OFFSET)
    assert len(records) == 1
    assert records[0]["time"] == datetime(2022, 3, 1, 7, 0, tzinfo=timezone.utc)


# ─── NetzEinspeisungCollector.parse tests ─────────────────────────────────────


def test_parse_netzeinspeisung_extracts_pv_kwh():
    col = NetzEinspeisungCollector()
    records = col.parse(EINSPEISUNG_CSV)
    # Only photovoltaik rows are returned (2 out of 5)
    assert len(records) == 2
    assert records[0]["pv_kwh"] == pytest.approx(250.0)
    assert records[1]["pv_kwh"] == pytest.approx(310.5)


def test_parse_netzeinspeisung_filters_non_pv_rows():
    col = NetzEinspeisungCollector()
    records = col.parse(EINSPEISUNG_CSV)
    # thermisch and wind rows must be excluded
    assert all("pv_kwh" in r for r in records)
    assert len(records) == 2  # only the 2 photovoltaik rows


def test_parse_netzeinspeisung_converts_timestamps_to_utc():
    col = NetzEinspeisungCollector()
    records = col.parse(EINSPEISUNG_CSV)
    for rec in records:
        assert rec["time"].tzinfo == timezone.utc
    # 10:00 CEST (+02:00) → 08:00 UTC
    assert records[0]["time"] == datetime(2022, 6, 15, 8, 0, tzinfo=timezone.utc)


# ─── Shared edge-case tests ───────────────────────────────────────────────────


def test_parse_empty_csv_returns_empty_list_brutto():
    col = BruttolastgangCollector()
    assert col.parse(EMPTY_CSV) == []


def test_parse_empty_csv_returns_empty_list_einspeisung():
    col = NetzEinspeisungCollector()
    assert col.parse(EMPTY_CSV) == []


def test_parse_records_sorted_ascending():
    unsorted_csv = """\
zeitpunkt,bruttolastgang_kwh
2022-01-01T01:00:00+0100,200.0
2022-01-01T00:00:00+0100,100.0
2022-01-01T00:30:00+0100,150.0
"""
    col = BruttolastgangCollector()
    records = col.parse(unsorted_csv)
    times = [r["time"] for r in records]
    assert times == sorted(times)


def test_all_timestamps_utc_aware():
    col = BruttolastgangCollector()
    records = col.parse(BRUTTO_CSV)
    for rec in records:
        assert rec["time"].tzinfo is not None
        # UTC offset must be zero
        assert rec["time"].utcoffset().total_seconds() == 0
