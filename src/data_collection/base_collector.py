"""Abstract base class for all data collectors."""

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime

import httpx

_LOG = logging.getLogger(__name__)


def _log_api_call(
    source: str,
    status_code: int,
    response_ms: int,
    date_fetched: str | None,
) -> None:
    """
    Insert a row into api_call_log. Fails silently if DB is unavailable.

    Uses a standalone psycopg2 connection (not the connection pool) to avoid
    circular imports with timescale_client.py.
    """
    import os  # noqa: PLC0415

    try:
        import psycopg2  # noqa: PLC0415

        conn = psycopg2.connect(
            host=os.getenv("BDSP_DB_HOST", "timescaledb"),
            port=int(os.getenv("BDSP_DB_PORT", 5432)),
            dbname=os.getenv("BDSP_DB_NAME", "bdsp"),
            user=os.getenv("BDSP_DB_USER", "bdsp"),
            password=os.getenv("BDSP_DB_PASSWORD", ""),
        )
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_call_log
                        (source, status_code, was_rate_limited, response_ms, date_fetched)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (source, status_code, status_code == 429, response_ms, date_fetched),
                )
        conn.close()
    except Exception as exc:  # noqa: BLE001
        _LOG.warning("api_call_log insert failed (non-critical): %s", exc)


class BaseCollector(ABC):
    """
    Every collector must implement fetch() and parse().
    run() orchestrates both and returns validated records.

    Each record dict MUST contain:
        time: datetime  – UTC-aware timestamp

    Subclasses should define:
        _source_name: str  – identifier for API call logging (e.g. "entsoe")
    """

    _source_name: str | None = None

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
        source: str | None = None,
        date_fetched: str | None = None,
    ) -> httpx.Response:
        """
        GET *url* with automatic retry on 429 / 5xx responses and network errors.

        - 429 Too Many Requests: waits Retry-After seconds (default 60).
        - 500/502/503: exponential back-off (1s, 2s, 4s).
        - ConnectTimeout/ReadTimeout/ConnectError: exponential back-off.
        - All other errors raise immediately via raise_for_status().

        If *source* is provided, each response is logged to api_call_log.
        """
        response: httpx.Response | None = None
        last_exc: BaseException | None = None
        for attempt in range(max_retries):
            t0 = time.monotonic()
            try:
                response = httpx.get(url, params=params, timeout=timeout)
            except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError) as exc:
                last_exc = exc
                wait = 2 ** attempt
                _LOG.warning(
                    "Network error on attempt %d/%d for %s: %s – retrying in %ds",
                    attempt + 1, max_retries, url, exc, wait,
                )
                time.sleep(wait)
                continue
            response_ms = int((time.monotonic() - t0) * 1000)

            if source:
                _log_api_call(source, response.status_code, response_ms, date_fetched)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                time.sleep(retry_after)
                continue
            if response.status_code in (500, 502, 503):
                time.sleep(2 ** attempt)
                continue
            response.raise_for_status()
            return response
        # Last attempt exhausted
        if response is None:
            raise last_exc  # type: ignore[misc]
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
