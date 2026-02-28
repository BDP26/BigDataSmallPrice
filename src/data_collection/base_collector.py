"""Abstract base class for all data collectors."""

from abc import ABC, abstractmethod
from datetime import datetime


class BaseCollector(ABC):
    """
    Every collector must implement fetch() and parse().
    run() orchestrates both and returns validated records.

    Each record dict MUST contain:
        time: datetime  – UTC-aware timestamp
    """

    @abstractmethod
    def fetch(self) -> bytes | str:
        """Retrieve raw data from the source (HTTP call, file read, etc.)."""

    @abstractmethod
    def parse(self, raw: bytes | str) -> list[dict]:
        """
        Parse raw data into a list of record dicts.
        Every dict must include a UTC-aware ``time`` key.
        """

    def run(self) -> list[dict]:
        """Fetch, parse, and validate records."""
        raw = self.fetch()
        records = self.parse(raw)
        self._validate(records)
        return records

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _validate(records: list[dict]) -> None:
        for i, rec in enumerate(records):
            if "time" not in rec:
                raise ValueError(f"Record {i} is missing required 'time' key: {rec}")
            t = rec["time"]
            if not isinstance(t, datetime):
                raise TypeError(f"Record {i} 'time' must be a datetime, got {type(t)}")
            if t.tzinfo is None:
                raise ValueError(f"Record {i} 'time' must be UTC-aware (tzinfo is None)")
