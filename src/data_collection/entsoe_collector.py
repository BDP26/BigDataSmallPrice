"""
ENTSO-E Transparency Platform – Day-Ahead Prices collector.

API docs: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
Document type A44 = Day-ahead Prices
Domain: 10YCH-SWISSGRIDZ (Switzerland / Swissgrid)
"""

import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

from .base_collector import BaseCollector

_API_URL = "https://web-api.tp.entsoe.eu/api"
_DOMAIN = "10YCH-SWISSGRIDZ"
_DE_DOMAIN = "10Y1001A1001A83F"  # Germany BZN
_IT_DOMAIN = "10YIT-GRTN-----B"  # Italy BZN
_NS = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}


def _ns_from_root(root: ET.Element) -> dict:
    """Extract namespace from root element tag so parsing works across all ENTSO-E document types."""
    tag = root.tag
    if tag.startswith("{"):
        return {"ns": tag[1: tag.index("}")]}
    return _NS


class EntsoeCollector(BaseCollector):
    """
    Fetches ENTSO-E Day-Ahead prices for Switzerland.

    Args:
        token:       API token (defaults to ENTSOE_API_TOKEN env var)
        period_start: Start datetime (UTC). Defaults to today 00:00 UTC.
        period_end:   End datetime (UTC). Defaults to tomorrow 00:00 UTC.
    """

    _source_name = "entsoe"

    def __init__(
        self,
        token: str | None = None,
        period_start: datetime | None = None,
        period_end: datetime | None = None,
    ) -> None:
        self.token = token or os.environ["ENTSOE_API_TOKEN"]
        now_utc = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        self.period_start = period_start or now_utc
        self.period_end = period_end or (now_utc + timedelta(days=1))

    def fetch(self) -> bytes:
        params = {
            "securityToken": self.token,
            "documentType": "A44",
            "in_Domain": _DOMAIN,
            "out_Domain": _DOMAIN,
            "periodStart": self.period_start.strftime("%Y%m%d%H%M"),
            "periodEnd": self.period_end.strftime("%Y%m%d%H%M"),
        }
        response = self._fetch_with_retry(
            _API_URL,
            params=params,
            source=self._source_name,
            date_fetched=self.period_start.strftime("%Y-%m-%d"),
        )
        return response.content

    def parse(self, raw: bytes | str) -> list[dict]:
        if isinstance(raw, str):
            raw = raw.encode()
        root = ET.fromstring(raw)

        records: list[dict] = []
        for ts in root.findall(".//ns:TimeSeries", _NS):
            currency = _text(ts, "ns:currency_Unit.name", _NS) or "EUR"
            period = ts.find("ns:Period", _NS)
            if period is None:
                continue

            start_str = _text(period, "ns:timeInterval/ns:start", _NS)
            if start_str is None:
                continue
            interval_start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))

            resolution = _text(period, "ns:resolution", _NS) or "PT60M"
            interval_minutes = _resolution_to_minutes(resolution)

            for point in period.findall("ns:Point", _NS):
                pos_str = _text(point, "ns:position", _NS)
                price_str = _text(point, "ns:price.amount", _NS)
                if pos_str is None or price_str is None:
                    continue
                position = int(pos_str) - 1  # 1-based → 0-based
                ts_utc = interval_start + timedelta(minutes=position * interval_minutes)
                records.append(
                    {
                        "time": ts_utc,
                        "price_eur_mwh": float(price_str),
                        "currency": currency,
                        "domain": _DOMAIN,
                    }
                )

        return records


class EntsoeActualLoadCollector(BaseCollector):
    """
    Fetches ENTSO-E Actual Total Load for Switzerland (Document Type A65).

    Args:
        token:        API token (defaults to ENTSOE_API_TOKEN env var)
        period_start: Start datetime (UTC). Defaults to yesterday 00:00 UTC.
        period_end:   End datetime (UTC). Defaults to today 00:00 UTC.
    """

    _source_name = "entsoe_actual_load"

    def __init__(
        self,
        token: str | None = None,
        period_start: datetime | None = None,
        period_end: datetime | None = None,
    ) -> None:
        self.token = token or os.environ["ENTSOE_API_TOKEN"]
        now_utc = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        self.period_start = period_start or (now_utc - timedelta(days=1))
        self.period_end = period_end or now_utc

    def fetch(self) -> bytes:
        params = {
            "securityToken": self.token,
            "documentType": "A65",
            "processType": "A16",
            "outBiddingZone_Domain": _DOMAIN,
            "periodStart": self.period_start.strftime("%Y%m%d%H%M"),
            "periodEnd": self.period_end.strftime("%Y%m%d%H%M"),
        }
        response = self._fetch_with_retry(
            _API_URL,
            params=params,
            source=self._source_name,
            date_fetched=self.period_start.strftime("%Y-%m-%d"),
        )
        return response.content

    def parse(self, raw: bytes | str) -> list[dict]:
        if isinstance(raw, str):
            raw = raw.encode()
        root = ET.fromstring(raw)
        ns = _ns_from_root(root)

        records: list[dict] = []
        for ts in root.findall(".//ns:TimeSeries", ns):
            period = ts.find("ns:Period", ns)
            if period is None:
                continue

            start_str = _text(period, "ns:timeInterval/ns:start", ns)
            if start_str is None:
                continue
            interval_start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))

            resolution = _text(period, "ns:resolution", ns) or "PT60M"
            interval_minutes = _resolution_to_minutes(resolution)

            for point in period.findall("ns:Point", ns):
                pos_str = _text(point, "ns:position", ns)
                qty_str = _text(point, "ns:quantity", ns)
                if pos_str is None or qty_str is None:
                    continue
                position = int(pos_str) - 1  # 1-based → 0-based
                ts_utc = interval_start + timedelta(minutes=position * interval_minutes)
                records.append(
                    {
                        "time": ts_utc,
                        "load_mwh": float(qty_str),
                        "domain": _DOMAIN,
                    }
                )

        return records


class EntsoeGenerationCollector(BaseCollector):
    """
    Fetches ENTSO-E Actual Generation Per Type (A75/A16) for a given domain and PSR type.

    Args:
        domain:       EIC domain code (e.g. _DOMAIN for CH, _DE_DOMAIN for DE).
        psr_type:     Power System Resource type (e.g. 'B12' hydro run-of-river, 'B16' solar).
        token:        API token (defaults to ENTSOE_API_TOKEN env var).
        period_start: Start datetime (UTC). Defaults to yesterday 00:00 UTC.
        period_end:   End datetime (UTC). Defaults to today 00:00 UTC.
    """

    _source_name = "entsoe_generation"

    def __init__(
        self,
        domain: str,
        psr_type: str,
        token: str | None = None,
        period_start: datetime | None = None,
        period_end: datetime | None = None,
    ) -> None:
        self.domain = domain
        self.psr_type = psr_type
        self.token = token or os.environ["ENTSOE_API_TOKEN"]
        now_utc = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        self.period_start = period_start or (now_utc - timedelta(days=1))
        self.period_end = period_end or now_utc

    def fetch(self) -> bytes:
        params = {
            "securityToken": self.token,
            "documentType": "A75",
            "processType": "A16",
            "in_Domain": self.domain,
            "psrType": self.psr_type,
            "periodStart": self.period_start.strftime("%Y%m%d%H%M"),
            "periodEnd": self.period_end.strftime("%Y%m%d%H%M"),
        }
        response = self._fetch_with_retry(
            _API_URL,
            params=params,
            source=self._source_name,
            date_fetched=self.period_start.strftime("%Y-%m-%d"),
        )
        return response.content

    def parse(self, raw: bytes | str) -> list[dict]:
        if isinstance(raw, str):
            raw = raw.encode()
        root = ET.fromstring(raw)
        ns = _ns_from_root(root)

        records: list[dict] = []
        for ts in root.findall(".//ns:TimeSeries", ns):
            period = ts.find("ns:Period", ns)
            if period is None:
                continue

            start_str = _text(period, "ns:timeInterval/ns:start", ns)
            if start_str is None:
                continue
            interval_start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))

            resolution = _text(period, "ns:resolution", ns) or "PT60M"
            interval_minutes = _resolution_to_minutes(resolution)

            for point in period.findall("ns:Point", ns):
                pos_str = _text(point, "ns:position", ns)
                qty_str = _text(point, "ns:quantity", ns)
                if pos_str is None or qty_str is None:
                    continue
                position = int(pos_str) - 1  # 1-based → 0-based
                ts_utc = interval_start + timedelta(minutes=position * interval_minutes)
                records.append(
                    {
                        "time": ts_utc,
                        "domain": self.domain,
                        "psr_type": self.psr_type,
                        "quantity_mwh": float(qty_str),
                    }
                )

        return records


class EntsoeCrossBorderFlowCollector(BaseCollector):
    """
    Fetches ENTSO-E Cross-Border Physical Flows (A11) between two domains.

    Args:
        in_domain:    EIC code for the importing domain.
        out_domain:   EIC code for the exporting domain.
        token:        API token (defaults to ENTSOE_API_TOKEN env var).
        period_start: Start datetime (UTC). Defaults to yesterday 00:00 UTC.
        period_end:   End datetime (UTC). Defaults to today 00:00 UTC.
    """

    _source_name = "entsoe_crossborder_flows"

    def __init__(
        self,
        in_domain: str,
        out_domain: str,
        token: str | None = None,
        period_start: datetime | None = None,
        period_end: datetime | None = None,
    ) -> None:
        self.in_domain = in_domain
        self.out_domain = out_domain
        self.token = token or os.environ["ENTSOE_API_TOKEN"]
        now_utc = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        self.period_start = period_start or (now_utc - timedelta(days=1))
        self.period_end = period_end or now_utc

    def fetch(self) -> bytes:
        params = {
            "securityToken": self.token,
            "documentType": "A11",
            "in_Domain": self.in_domain,
            "out_Domain": self.out_domain,
            "periodStart": self.period_start.strftime("%Y%m%d%H%M"),
            "periodEnd": self.period_end.strftime("%Y%m%d%H%M"),
        }
        response = self._fetch_with_retry(
            _API_URL,
            params=params,
            source=self._source_name,
            date_fetched=self.period_start.strftime("%Y-%m-%d"),
        )
        return response.content

    def parse(self, raw: bytes | str) -> list[dict]:
        if isinstance(raw, str):
            raw = raw.encode()
        root = ET.fromstring(raw)
        ns = _ns_from_root(root)

        records: list[dict] = []
        for ts in root.findall(".//ns:TimeSeries", ns):
            period = ts.find("ns:Period", ns)
            if period is None:
                continue

            start_str = _text(period, "ns:timeInterval/ns:start", ns)
            if start_str is None:
                continue
            interval_start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))

            resolution = _text(period, "ns:resolution", ns) or "PT60M"
            interval_minutes = _resolution_to_minutes(resolution)

            for point in period.findall("ns:Point", ns):
                pos_str = _text(point, "ns:position", ns)
                qty_str = _text(point, "ns:quantity", ns)
                if pos_str is None or qty_str is None:
                    continue
                position = int(pos_str) - 1  # 1-based → 0-based
                ts_utc = interval_start + timedelta(minutes=position * interval_minutes)
                records.append(
                    {
                        "time": ts_utc,
                        "in_domain": self.in_domain,
                        "out_domain": self.out_domain,
                        "flow_mwh": float(qty_str),
                    }
                )

        return records


class EntsoeLoadForecastCollector(BaseCollector):
    """
    Fetches ENTSO-E Day-Ahead Total Load Forecast (A65/A01) for Switzerland.

    Args:
        domain:       EIC domain code (defaults to CH Swissgrid).
        token:        API token (defaults to ENTSOE_API_TOKEN env var).
        period_start: Start datetime (UTC). Defaults to today 00:00 UTC.
        period_end:   End datetime (UTC). Defaults to tomorrow 00:00 UTC (forward-looking).
    """

    _source_name = "entsoe_load_forecast"

    def __init__(
        self,
        domain: str = _DOMAIN,
        token: str | None = None,
        period_start: datetime | None = None,
        period_end: datetime | None = None,
    ) -> None:
        self.domain = domain
        self.token = token or os.environ["ENTSOE_API_TOKEN"]
        now_utc = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        self.period_start = period_start or now_utc
        self.period_end = period_end or (now_utc + timedelta(days=1))

    def fetch(self) -> bytes:
        params = {
            "securityToken": self.token,
            "documentType": "A65",
            "processType": "A01",
            "outBiddingZone_Domain": self.domain,
            "periodStart": self.period_start.strftime("%Y%m%d%H%M"),
            "periodEnd": self.period_end.strftime("%Y%m%d%H%M"),
        }
        response = self._fetch_with_retry(
            _API_URL,
            params=params,
            source=self._source_name,
            date_fetched=self.period_start.strftime("%Y-%m-%d"),
        )
        return response.content

    def parse(self, raw: bytes | str) -> list[dict]:
        if isinstance(raw, str):
            raw = raw.encode()
        root = ET.fromstring(raw)
        ns = _ns_from_root(root)

        records: list[dict] = []
        for ts in root.findall(".//ns:TimeSeries", ns):
            period = ts.find("ns:Period", ns)
            if period is None:
                continue

            start_str = _text(period, "ns:timeInterval/ns:start", ns)
            if start_str is None:
                continue
            interval_start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))

            resolution = _text(period, "ns:resolution", ns) or "PT60M"
            interval_minutes = _resolution_to_minutes(resolution)

            for point in period.findall("ns:Point", ns):
                pos_str = _text(point, "ns:position", ns)
                qty_str = _text(point, "ns:quantity", ns)
                if pos_str is None or qty_str is None:
                    continue
                position = int(pos_str) - 1  # 1-based → 0-based
                ts_utc = interval_start + timedelta(minutes=position * interval_minutes)
                records.append(
                    {
                        "time": ts_utc,
                        "domain": self.domain,
                        "load_mwh": float(qty_str),
                    }
                )

        return records


# ── Helpers ───────────────────────────────────────────────────────────────────

def _text(element: ET.Element, path: str, ns: dict) -> str | None:
    el = element.find(path, ns)
    return el.text if el is not None else None


def _resolution_to_minutes(resolution: str) -> int:
    mapping = {"PT15M": 15, "PT30M": 30, "PT60M": 60, "P1D": 1440}
    return mapping.get(resolution, 60)
