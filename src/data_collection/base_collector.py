"""Abstract base class for all data collectors."""

import time
from abc import ABC, abstractmethod
from datetime import datetime

import httpx


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
    def _fetch_with_retry(
        url: str,
        params: dict | None = None,
        timeout: int = 30,
        max_retries: int = 3,
    ) -> httpx.Response:
        """
        GET *url* with automatic retry on 429 / 5xx responses.

        - 429 Too Many Requests: waits Retry-After seconds (default 60).
        - 500/502/503: exponential back-off (1s, 2s, 4s).
        - All other errors raise immediately via raise_for_status().
        """
        response: httpx.Response | None = None
        for attempt in range(max_retries):
            response = httpx.get(url, params=params, timeout=timeout)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                time.sleep(retry_after)
                continue
            if response.status_code in (500, 502, 503):
                time.sleep(2 ** attempt)
                continue
            response.raise_for_status()
            return response
        # Last attempt exhausted – raise whatever we got
        assert response is not None
        response.raise_for_status()
        return response  # unreachable, but satisfies type checkers

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
