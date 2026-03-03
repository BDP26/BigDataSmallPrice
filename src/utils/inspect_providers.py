"""
Multi-Provider Tariff Comparison – standalone script.

Queries EKZ, CKW, and Groupe E for a given date and compares price variability
to identify which providers offer truly dynamic pricing.

Usage:
    python src/utils/inspect_providers.py
"""

import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

# Allow running from project root without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.inspect_api import extract_values, fetch_json, print_summary

API_EKZ = "https://api.tariffs.ekz.ch/v1/tariffs"
API_CKW = "https://e-ckw-public-data.de-c1.eu1.cloudhub.io/api/v1/netzinformationen/energie/dynamische-preise"
API_GROUPE_E = "https://api.tariffs.groupe-e.ch/v2/tariffs"

# All three APIs share the same response shape:
# { "prices": [ { "start_timestamp": ..., "<component>": [{unit, value}, ...] } ] }
# So extract_values() from inspect_api works unchanged for CKW and Groupe E.


# ---------------------------------------------------------------------------
# EKZ
# ---------------------------------------------------------------------------

def inspect_ekz(date: str, tariff_type: str = "dynamic") -> list[float]:
    """Query EKZ tariff API. Returns electricity CHF_kWh values."""
    params = {"date": date, "tariffType": tariff_type}
    try:
        data = fetch_json(API_EKZ, params=params)
    except httpx.HTTPError as exc:
        print(f"  EKZ fetch error: {exc}")
        return []

    prices = data.get("prices", []) if isinstance(data, dict) else []
    print(f"  total intervals: {len(prices)}")

    electricity = extract_values(data, "electricity")
    print_summary("electricity", electricity)
    return electricity


# ---------------------------------------------------------------------------
# CKW
# ---------------------------------------------------------------------------

def inspect_ckw(date: str, tariff_name: str = "home_dynamic") -> list[float]:
    """Query CKW dynamic tariff API for a single day."""
    start = f"{date}T00:00:00+01:00"
    end = f"{date}T23:59:59+01:00"
    params = {
        "tariff_name": tariff_name,
        "start_timestamp": start,
        "end_timestamp": end,
    }
    try:
        data = fetch_json(API_CKW, params=params)
    except httpx.HTTPError as exc:
        print(f"  CKW fetch error: {exc}")
        return []

    prices = data.get("prices", []) if isinstance(data, dict) else []
    print(f"  total intervals: {len(prices)}")

    # Try component keys in priority order
    for key in ("grid_usage", "grid", "electricity", "integrated"):
        values = extract_values(data, key)
        if values:
            print_summary(key, values)
            return values

    print(f"  CKW: no values found in any component")
    return []


# ---------------------------------------------------------------------------
# Groupe E
# ---------------------------------------------------------------------------

def inspect_groupe_e(date: str) -> dict[str, list[float]]:
    """Query Groupe E tariff API. Returns dict of component → values."""
    results: dict[str, list[float]] = {}

    # First attempt: with date param
    try:
        data = fetch_json(API_GROUPE_E, params={"date": date})
    except httpx.HTTPError as exc:
        print(f"  Groupe E fetch error (with date param): {exc}")
        data = None

    # Check if response is empty / unhelpful → retry without params
    is_empty = data is None or (isinstance(data, (dict, list)) and not data)
    if is_empty:
        print("  Groupe E: date param returned empty response – retrying without params")
        try:
            data = fetch_json(API_GROUPE_E)
        except httpx.HTTPError as exc:
            print(f"  Groupe E fetch error (no params): {exc}")
            return results

    prices = data.get("prices", []) if isinstance(data, dict) else []
    print(f"  total intervals: {len(prices)}")

    for component in ("grid", "integrated"):
        values = extract_values(data, component)
        print_summary(component, values)
        if values:
            results[component] = values

    return results


# ---------------------------------------------------------------------------
# Side-by-side comparison
# ---------------------------------------------------------------------------

def _stdev_or_zero(values: list[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def compare_providers(date: str) -> dict[str, float]:
    """
    Run all three providers for *date* and print a comparison table.
    Returns a mapping of label → stdev for callers to inspect.
    """
    print(f"\n{'='*60}")
    print(f"  COMPARISON for {date}")
    print(f"{'='*60}")

    stdevs: dict[str, float] = {}

    print(f"\n--- EKZ (dynamic) ---")
    ekz_vals = inspect_ekz(date, "dynamic")
    stdevs["EKZ/electricity"] = _stdev_or_zero(ekz_vals)

    print(f"\n--- CKW (home_dynamic) ---")
    ckw_vals = inspect_ckw(date, "home_dynamic")
    stdevs["CKW/grid_usage"] = _stdev_or_zero(ckw_vals)

    print(f"\n--- Groupe E ---")
    ge_results = inspect_groupe_e(date)
    for comp, vals in ge_results.items():
        stdevs[f"GroupeE/{comp}"] = _stdev_or_zero(vals)

    # Summary table
    print(f"\n{'─'*60}")
    print(f"  {'Provider/Component':<28} {'stdev':>10}  {'dynamic?'}")
    print(f"{'─'*60}")
    for label, sd in stdevs.items():
        flag = "YES" if sd > 0 else "NO (flat rate)"
        print(f"  {label:<28} {sd:>10.6f}  {flag}")
    print(f"{'─'*60}")

    return stdevs


# ---------------------------------------------------------------------------
# Recommendation
# ---------------------------------------------------------------------------

def print_recommendation(all_stdevs: dict[str, dict[str, float]]) -> None:
    """Aggregate stdevs across days and print a final recommendation."""
    # Collect all labels
    labels: set[str] = set()
    for day_stdevs in all_stdevs.values():
        labels.update(day_stdevs.keys())

    print(f"\n{'='*60}")
    print("  FINAL RECOMMENDATION")
    print(f"{'='*60}")
    for label in sorted(labels):
        day_vals = [all_stdevs[day].get(label, 0.0) for day in all_stdevs]
        avg_sd = statistics.mean(day_vals)
        is_dynamic = avg_sd > 0
        print(f"  {label:<28}  avg stdev={avg_sd:.6f}  → {'DYNAMIC' if is_dynamic else 'FLAT RATE'}")

    print()
    dynamic_providers = [
        label for label in sorted(labels)
        if statistics.mean(all_stdevs[day].get(label, 0.0) for day in all_stdevs) > 0
    ]
    if dynamic_providers:
        print("  Recommended for forecasting model (dynamic pricing confirmed):")
        for p in dynamic_providers:
            if not p.startswith("EKZ"):
                print(f"    -> {p}")
    else:
        print("  No dynamic providers found across tested dates.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    yesterday = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    all_stdevs: dict[str, dict[str, float]] = {}

    print(f"Tariff comparison script – {today}")
    print("Querying EKZ, CKW, Groupe E ...\n")

    for date in (today, yesterday):
        stdevs = compare_providers(date)
        all_stdevs[date] = stdevs

    print_recommendation(all_stdevs)


if __name__ == "__main__":
    main()
