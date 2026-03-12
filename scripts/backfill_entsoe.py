"""
ENTSO-E historical backfill script.

Fetches Day-Ahead prices in monthly chunks from 2022-01-01 to yesterday
and upserts them into TimescaleDB.

Usage:
    BDSP_DB_PASSWORD=xxx ENTSOE_API_TOKEN=xxx python scripts/backfill_entsoe.py

Optional env vars:
    BACKFILL_START  – override start date (YYYY-MM-DD), default 2022-01-01
    BACKFILL_END    – override end date   (YYYY-MM-DD), default yesterday
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_collection.entsoe_collector import EntsoeCollector
from db.timescale_client import upsert_entsoe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s – %(message)s",
)
_LOG = logging.getLogger("backfill_entsoe")


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _month_chunks(start: datetime, end: datetime):
    """Yield (chunk_start, chunk_end) pairs of roughly 1-month each."""
    current = start
    while current < end:
        # Move to first day of next month
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
        chunk_end = min(next_month, end)
        yield current, chunk_end
        current = chunk_end


def main() -> None:
    start_str = os.environ.get("BACKFILL_START", "2022-01-01")
    yesterday = datetime.now(tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    end_str = os.environ.get("BACKFILL_END", yesterday.strftime("%Y-%m-%d"))

    start = _parse_date(start_str)
    end = _parse_date(end_str)

    _LOG.info("ENTSO-E backfill %s → %s", start_str, end_str)

    total_records = 0
    chunks = list(_month_chunks(start, end))
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        _LOG.info(
            "[%d/%d] Fetching %s → %s",
            i, len(chunks),
            chunk_start.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d"),
        )
        try:
            collector = EntsoeCollector(
                period_start=chunk_start,
                period_end=chunk_end,
            )
            records = collector.run()
            if records:
                upsert_entsoe(records)
                total_records += len(records)
                _LOG.info("  Upserted %d records", len(records))
            else:
                _LOG.warning("  No records returned for this chunk")
        except Exception as exc:  # noqa: BLE001
            _LOG.error("  Failed: %s", exc)

    _LOG.info("Done. Total records upserted: %d", total_records)


if __name__ == "__main__":
    main()
