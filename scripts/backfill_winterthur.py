"""
Winterthur OGD (Stadtwerk) full historical backfill script.

Fetches all 4 Bruttolastgang CSV files (2013–current) and the
Netzeinspeisung CSV, then upserts both into TimescaleDB.

Usage:
    BDSP_DB_PASSWORD=xxx python scripts/backfill_winterthur.py
"""

import logging
import sys
from pathlib import Path

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_collection.stadtwerk_winterthur_collector import (
    BruttolastgangCollector,
    NetzEinspeisungCollector,
)
from db.timescale_client import upsert_winterthur_load, upsert_winterthur_pv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s – %(message)s",
)
_LOG = logging.getLogger("backfill_winterthur")


def main() -> None:
    _LOG.info("Fetching all Bruttolastgang CSVs (2013–current)…")
    load_records = BruttolastgangCollector(all_files=True).run()
    _LOG.info("Fetched %d load records", len(load_records))
    if load_records:
        upsert_winterthur_load(load_records)
        _LOG.info("Upserted %d load records into winterthur_load", len(load_records))

    _LOG.info("Fetching Netzeinspeisung (PV) CSV…")
    pv_records = NetzEinspeisungCollector().run()
    _LOG.info("Fetched %d PV records", len(pv_records))
    if pv_records:
        upsert_winterthur_pv(pv_records)
        _LOG.info("Upserted %d PV records into winterthur_pv", len(pv_records))

    _LOG.info(
        "Done. Load: %d rows, PV: %d rows",
        len(load_records),
        len(pv_records),
    )


if __name__ == "__main__":
    main()
