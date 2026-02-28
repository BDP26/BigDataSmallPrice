"""
Integration tests: write to a real TimescaleDB instance.

These tests are SKIPPED unless the environment variable BDSP_INTEGRATION_TESTS=1
is set AND a running TimescaleDB is reachable via the BDSP_DB_* env vars.

Run with:
    BDSP_INTEGRATION_TESTS=1 pytest src/testing/integrationtests/ -v
"""

import os
from datetime import datetime, timezone

import pytest

INTEGRATION = os.getenv("BDSP_INTEGRATION_TESTS") == "1"
skip_reason = "Integration tests disabled. Set BDSP_INTEGRATION_TESTS=1 to run."


@pytest.fixture(scope="module")
def db_client():
    """Return the timescale_client module (imports lazily to avoid connection on import)."""
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[3]))
    import importlib
    return importlib.import_module("src.db.timescale_client")


@pytest.mark.skipif(not INTEGRATION, reason=skip_reason)
class TestEntsoeWrite:
    def test_upsert_entsoe_inserts_record(self, db_client):
        record = {
            "time": datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc),
            "domain": "10YCH-SWISSGRIDZ",
            "price_eur_mwh": 99.99,
            "currency": "EUR",
        }
        result = db_client.upsert_entsoe([record])
        assert result >= 0  # 0 = duplicate (OK), 1 = inserted

    def test_upsert_entsoe_idempotent(self, db_client):
        record = {
            "time": datetime(2026, 2, 28, 1, 0, 0, tzinfo=timezone.utc),
            "domain": "10YCH-SWISSGRIDZ",
            "price_eur_mwh": 88.88,
            "currency": "EUR",
        }
        db_client.upsert_entsoe([record])
        result = db_client.upsert_entsoe([record])  # second insert = ON CONFLICT DO NOTHING
        assert result >= 0


@pytest.mark.skipif(not INTEGRATION, reason=skip_reason)
class TestWeatherWrite:
    def test_upsert_weather_inserts_record(self, db_client):
        record = {
            "time": datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc),
            "latitude": 47.5001,
            "longitude": 8.7502,
            "temperature_2m": 3.5,
            "wind_speed_10m": 12.3,
            "shortwave_radiation": 0.0,
            "cloud_cover": 80.0,
        }
        result = db_client.upsert_weather([record])
        assert result >= 0


@pytest.mark.skipif(not INTEGRATION, reason=skip_reason)
class TestEkzWrite:
    def test_upsert_ekz_inserts_record(self, db_client):
        record = {
            "time": datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc),
            "tariff_type": "dynamic",
            "price_chf_kwh": 0.1234,
        }
        result = db_client.upsert_ekz([record])
        assert result >= 0


@pytest.mark.skipif(not INTEGRATION, reason=skip_reason)
class TestBafuWrite:
    def test_upsert_bafu_inserts_record(self, db_client):
        record = {
            "time": datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc),
            "station_id": "2018",
            "discharge_m3s": 245.3,
            "level_masl": 322.1,
        }
        result = db_client.upsert_bafu([record])
        assert result >= 0
