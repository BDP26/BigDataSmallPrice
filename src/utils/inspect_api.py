"""
API Inspection Tool – standalone script to verify data from any HTTP JSON endpoint.

Usage:
    python src/utils/inspect_api.py                     # runs EKZ scenarios
    # Or import and use functions directly:
    #   from utils.inspect_api import fetch_json, print_summary
"""

import statistics
from datetime import datetime, timedelta, timezone

import httpx

API_EKZ = "https://api.tariffs.ekz.ch/v1/tariffs"


def fetch_json(url: str, params: dict | None = None, headers: dict | None = None) -> dict | list:
    """Fetch JSON from a URL and return parsed response. Prints status info."""
    print(f"  GET {url}")
    if params:
        print(f"  params: {params}")
    response = httpx.get(url, params=params, headers=headers, timeout=15)
    print(f"  status: {response.status_code}")
    response.raise_for_status()
    return response.json()


def extract_values(data: dict | list, component: str) -> list[float]:
    """
    Extract float values from EKZ price entries.

    For EKZ responses: data["prices"] is a list of entries, each with a
    component key (e.g. "electricity") containing a list of {unit, value} dicts.
    Extracts CHF_kWh from each entry for the given component.
    """
    prices = data.get("prices", []) if isinstance(data, dict) else data
    values: list[float] = []
    for entry in prices:
        component_list = entry.get(component, [])
        unit_map = {item["unit"]: item["value"] for item in component_list}
        val = unit_map.get("CHF_kWh")
        if val is not None:
            values.append(float(val))
    return values


def print_summary(label: str, values: list[float]) -> None:
    """Print count, min, max, mean, stdev, and first 5 values."""
    if not values:
        print(f"  [{label}] no values found")
        return
    count = len(values)
    mn = min(values)
    mx = max(values)
    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if count > 1 else 0.0
    print(f"  [{label}] count={count}  min={mn:.6f}  max={mx:.6f}  "
          f"mean={mean:.6f}  stdev={stdev:.6f}")
    print(f"           first 5: {[round(v, 6) for v in values[:5]]}")


def inspect_ekz(date: str, tariff_type: str = "dynamic") -> None:
    """Query EKZ tariff API and print per-component price summaries."""
    params = {"date": date, "tariffType": tariff_type}
    data = fetch_json(API_EKZ, params=params)

    prices = data.get("prices", []) if isinstance(data, dict) else []
    print(f"  total intervals: {len(prices)}")

    for component in ("electricity", "grid", "integrated", "regional_fees"):
        values = extract_values(data, component)
        print_summary(component, values)


def main() -> None:
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    yesterday = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    day_before_yesterday = (datetime.now(tz=timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
    day_before_yesterday1 = (datetime.now(tz=timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")
    day_before_yesterday2 = (datetime.now(tz=timezone.utc) - timedelta(days=4)).strftime("%Y-%m-%d")
    day_before_yesterday3 = (datetime.now(tz=timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")
    day_before_yesterday4 = (datetime.now(tz=timezone.utc) - timedelta(days=6)).strftime("%Y-%m-%d")
    day_before_yesterday5 = (datetime.now(tz=timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    day_before_yesterday6 = (datetime.now(tz=timezone.utc) - timedelta(days=8)).strftime("%Y-%m-%d")
    day_before_yesterday7 = (datetime.now(tz=timezone.utc) - timedelta(days=9)).strftime("%Y-%m-%d")
    day_before_yesterday8 = (datetime.now(tz=timezone.utc) - timedelta(days=10)).strftime("%Y-%m-%d")

    print("=== EKZ: dynamic tariff, today ===")
    inspect_ekz(today, "dynamic")

    print("\n=== EKZ: standard tariff, today (comparison) ===")
    inspect_ekz(today, "standard")

    print(f"\n=== EKZ: dynamic tariff, yesterday ({yesterday}) ===")
    inspect_ekz(yesterday, "dynamic")

    print(f"\n=== EKZ: dynamic tariff, day before yesterday ({day_before_yesterday}) ===")
    inspect_ekz(day_before_yesterday, "dynamic")

    print(f"\n=== EKZ: dynamic tariff, day before yesterday ({day_before_yesterday1}) ===")
    inspect_ekz(day_before_yesterday1, "dynamic")

    print(f"\n=== EKZ: dynamic tariff, day before yesterday ({day_before_yesterday2}) ===")
    inspect_ekz(day_before_yesterday2, "dynamic")

    print(f"\n=== EKZ: dynamic tariff, day before yesterday ({day_before_yesterday3}) ===")
    inspect_ekz(day_before_yesterday3, "dynamic")

    print(f"\n=== EKZ: dynamic tariff, day before yesterday ({day_before_yesterday4}) ===")
    inspect_ekz(day_before_yesterday4, "dynamic")

    print(f"\n=== EKZ: dynamic tariff, day before yesterday ({day_before_yesterday5}) ===")
    inspect_ekz(day_before_yesterday5, "dynamic")

    print(f"\n=== EKZ: dynamic tariff, day before yesterday ({day_before_yesterday6}) ===")
    inspect_ekz(day_before_yesterday6, "dynamic")

    print(f"\n=== EKZ: dynamic tariff, day before yesterday ({day_before_yesterday7}) ===")
    inspect_ekz(day_before_yesterday7, "dynamic")

    print(f"\n=== EKZ: dynamic tariff, day before yesterday ({day_before_yesterday8}) ===")
    inspect_ekz(day_before_yesterday8, "dynamic")


if __name__ == "__main__":
    main()
