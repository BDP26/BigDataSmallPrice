"""
Shared ETL fetch logic used by both etl_pipeline_dag and backfill_dag.

Each function accepts a list of date strings (YYYY-MM-DD) and an optional
sleep interval between iterations (useful for backfilling to avoid rate limits).
"""

import time
from datetime import datetime, timedelta

import httpx
import pendulum

_TABLES = [
    "entsoe_day_ahead_prices",
    "entsoe_actual_load",
    "entsoe_generation",
    "entsoe_crossborder_flows",
    "entsoe_load_forecast",
    "weather_hourly",
    "bafu_hydro",
    "ekz_tariffs_raw",
    "ckw_tariffs_raw",
    "groupe_e_tariffs_raw",
    "winterthur_load",
    "winterthur_pv",
]


def _entsoe_skip_404(date_str: str, exc: Exception) -> bool:
    """Return True and log a warning if *exc* is an ENTSOE 404 (no data published)."""
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 404:
        print(f"ENTSO-E {date_str}: no data published (404) – skipping.")
        return True
    return False


def fetch_entsoe(dates: list[str], sleep_s: float = 0) -> None:
    from data_collection.entsoe_collector import EntsoeCollector
    from db.timescale_client import upsert_entsoe

    for date_str in dates:
        period_start = datetime.fromisoformat(date_str).replace(tzinfo=pendulum.UTC)
        period_end = period_start + timedelta(days=1)
        try:
            records = EntsoeCollector(period_start=period_start, period_end=period_end).run()
        except Exception as exc:
            if _entsoe_skip_404(date_str, exc):
                continue
            raise
        inserted = upsert_entsoe(records)
        print(f"ENTSO-E {date_str}: {len(records)} fetched, {inserted} inserted.")
        if sleep_s:
            time.sleep(sleep_s)


def fetch_entsoe_actual_load(dates: list[str], sleep_s: float = 0) -> None:
    from data_collection.entsoe_collector import EntsoeActualLoadCollector
    from db.timescale_client import upsert_entsoe_actual_load

    for date_str in dates:
        period_start = datetime.fromisoformat(date_str).replace(tzinfo=pendulum.UTC)
        period_end = period_start + timedelta(days=1)
        try:
            records = EntsoeActualLoadCollector(period_start=period_start, period_end=period_end).run()
        except Exception as exc:
            if _entsoe_skip_404(date_str, exc):
                continue
            raise
        inserted = upsert_entsoe_actual_load(records)
        print(f"ENTSO-E ActualLoad {date_str}: {len(records)} fetched, {inserted} inserted.")
        if sleep_s:
            time.sleep(sleep_s)


def fetch_entsoe_generation(dates: list[str], domain: str, psr_type: str, sleep_s: float = 0) -> None:
    from data_collection.entsoe_collector import EntsoeGenerationCollector
    from db.timescale_client import upsert_entsoe_generation
    for date_str in dates:
        period_start = datetime.fromisoformat(date_str).replace(tzinfo=pendulum.UTC)
        period_end = period_start + timedelta(days=1)
        try:
            records = EntsoeGenerationCollector(domain=domain, psr_type=psr_type, period_start=period_start, period_end=period_end).run()
        except Exception as exc:
            if _entsoe_skip_404(date_str, exc):
                continue
            raise
        inserted = upsert_entsoe_generation(records)
        print(f"ENTSO-E Generation {domain}/{psr_type} {date_str}: {len(records)} fetched, {inserted} inserted.")
        if sleep_s:
            time.sleep(sleep_s)


def fetch_entsoe_crossborder(dates: list[str], in_domain: str, out_domain: str, sleep_s: float = 0) -> None:
    from data_collection.entsoe_collector import EntsoeCrossBorderFlowCollector
    from db.timescale_client import upsert_entsoe_crossborder_flows
    for date_str in dates:
        period_start = datetime.fromisoformat(date_str).replace(tzinfo=pendulum.UTC)
        period_end = period_start + timedelta(days=1)
        try:
            records = EntsoeCrossBorderFlowCollector(in_domain=in_domain, out_domain=out_domain, period_start=period_start, period_end=period_end).run()
        except Exception as exc:
            if _entsoe_skip_404(date_str, exc):
                continue
            raise
        inserted = upsert_entsoe_crossborder_flows(records)
        print(f"ENTSO-E CrossBorder {in_domain}→{out_domain} {date_str}: {len(records)} fetched, {inserted} inserted.")
        if sleep_s:
            time.sleep(sleep_s)


def fetch_entsoe_load_forecast(dates: list[str], sleep_s: float = 0) -> None:
    from data_collection.entsoe_collector import EntsoeLoadForecastCollector
    from db.timescale_client import upsert_entsoe_load_forecast
    for date_str in dates:
        period_start = datetime.fromisoformat(date_str).replace(tzinfo=pendulum.UTC)
        period_end = period_start + timedelta(days=1)
        try:
            records = EntsoeLoadForecastCollector(period_start=period_start, period_end=period_end).run()
        except Exception as exc:
            if _entsoe_skip_404(date_str, exc):
                continue
            raise
        inserted = upsert_entsoe_load_forecast(records)
        print(f"ENTSO-E LoadForecast CH {date_str}: {len(records)} fetched, {inserted} inserted.")
        if sleep_s:
            time.sleep(sleep_s)


# Weather locations: CH baseline + DE proxies for Model B (EPEX price drivers)
_WEATHER_LOCATIONS = [
    {"latitude": 47.5001, "longitude": 8.7502, "label": "CH-Winterthur"},
    {"latitude": 53.5,    "longitude": 10.0,   "label": "DE-Nord"},   # Hamburg – DE wind proxy
    {"latitude": 48.5,    "longitude": 9.0,    "label": "DE-Sued"},   # Stuttgart – DE solar proxy
]


def fetch_weather(dates: list[str], sleep_s: float = 0) -> None:
    from data_collection.openmeteo_collector import OpenMeteoCollector
    from db.timescale_client import upsert_weather

    for date_str in dates:
        total_inserted = 0
        for loc in _WEATHER_LOCATIONS:
            records = OpenMeteoCollector(
                latitude=loc["latitude"],
                longitude=loc["longitude"],
                date=date_str,
            ).run()
            total_inserted += upsert_weather(records)
        print(f"Weather {date_str}: {total_inserted} inserted ({len(_WEATHER_LOCATIONS)} locations).")
        if sleep_s:
            time.sleep(sleep_s)


def fetch_ekz(dates: list[str], sleep_s: float = 0) -> None:
    from data_collection.ekz_collector import EkzCollector
    from db.timescale_client import upsert_ekz

    for date_str in dates:
        records = EkzCollector(date=date_str).run()
        inserted = upsert_ekz(records)
        print(f"EKZ {date_str}: {len(records)} fetched, {inserted} inserted.")
        if sleep_s:
            time.sleep(sleep_s)


def fetch_ckw(dates: list[str], sleep_s: float = 0) -> None:
    from data_collection.ckw_collector import CKWCollector
    from db.timescale_client import upsert_ckw

    for date_str in dates:
        records = CKWCollector(date=date_str).run()
        inserted = upsert_ckw(records)
        print(f"CKW {date_str}: {len(records)} fetched, {inserted} inserted.")
        if sleep_s:
            time.sleep(sleep_s)


def fetch_groupe_e(dates: list[str], sleep_s: float = 0) -> None:
    from data_collection.groupe_e_collector import GroupeECollector
    from db.timescale_client import upsert_groupe_e

    for date_str in dates:
        records = GroupeECollector(date=date_str).run()
        inserted = upsert_groupe_e(records)
        print(f"Groupe E {date_str}: {len(records)} fetched, {inserted} inserted.")
        if sleep_s:
            time.sleep(sleep_s)


def fetch_bafu(dates: list[str], sleep_s: float = 0) -> None:
    from data_collection.bafu_collector import BafuCollector
    from db.timescale_client import upsert_bafu

    for date_str in dates:
        records = BafuCollector(date=date_str).run()
        inserted = upsert_bafu(records)
        print(f"BAFU {date_str}: {len(records)} fetched, {inserted} inserted.")
        if sleep_s:
            time.sleep(sleep_s)


def fetch_winterthur_load(all_files: bool = False) -> None:
    from data_collection.stadtwerk_winterthur_collector import BruttolastgangCollector
    from db.timescale_client import upsert_winterthur_load

    records = BruttolastgangCollector(all_files=all_files).run()
    inserted = upsert_winterthur_load(records)
    print(f"Winterthur Load: {len(records)} fetched, {inserted} inserted.")


def fetch_winterthur_pv() -> None:
    from data_collection.stadtwerk_winterthur_collector import NetzEinspeisungCollector
    from db.timescale_client import upsert_winterthur_pv

    records = NetzEinspeisungCollector().run()
    inserted = upsert_winterthur_pv(records)
    print(f"Winterthur PV: {len(records)} fetched, {inserted} inserted.")


def log_row_counts(date_str: str | None = None) -> None:
    """Print row counts for all tables. If date_str is given, filter by that date."""
    import os
    import psycopg2

    try:
        conn = psycopg2.connect(
            host=os.getenv("BDSP_DB_HOST", "timescaledb"),
            port=int(os.getenv("BDSP_DB_PORT", 5432)),
            dbname=os.getenv("BDSP_DB_NAME", "bdsp"),
            user=os.getenv("BDSP_DB_USER", "bdsp"),
            password=os.getenv("BDSP_DB_PASSWORD", ""),
        )
        header = f"Row counts for {date_str}:" if date_str else "Total row counts:"
        lines = [header]
        with conn.cursor() as cur:
            for table in _TABLES:
                if date_str:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE time::date = %s",
                        (date_str,),
                    )
                else:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                lines.append(f"  {table}: {cur.fetchone()[0]}")
        conn.close()
        print("\n".join(lines))
    except Exception as exc:
        print(f"Row count query failed (non-critical): {exc}")
