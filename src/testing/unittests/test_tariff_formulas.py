"""
Unit tests for processing/tariff_formulas.py.

Tests cover:
- Quadratic scaling of netzpreis
- Clip behaviour at extremes
- EUR/MWh → Rp./kWh unit conversion in energiepreis
- gesamttarif summation
- compute_tariff convenience wrapper
"""

import pytest

from processing.tariff_formulas import (
    DEFAULT_ALPHA,
    DEFAULT_LOAD_MAX,
    DEFAULT_LOAD_MIN,
    DEFAULT_NETZ_STANDARD,
    ENERGIE_CLIP_DOWN,
    ENERGIE_CLIP_UP,
    NETZ_CLIP_DOWN,
    NETZ_CLIP_UP,
    compute_tariff,
    energiepreis,
    gesamttarif,
    netzpreis,
)


# ─── netzpreis tests ──────────────────────────────────────────────────────────


def test_netzpreis_at_minimum_load():
    """L_norm=0 → raw=0 → clipped to standardtarif - clip_down."""
    result = netzpreis(DEFAULT_LOAD_MIN)
    expected_floor = DEFAULT_NETZ_STANDARD - NETZ_CLIP_DOWN
    assert result == pytest.approx(expected_floor)


def test_netzpreis_at_maximum_load():
    """L_norm=1 → raw=alpha → clipped within [floor, ceiling]."""
    result = netzpreis(DEFAULT_LOAD_MAX)
    raw = DEFAULT_ALPHA  # alpha * 1² = alpha
    lo = DEFAULT_NETZ_STANDARD - NETZ_CLIP_DOWN
    hi = DEFAULT_NETZ_STANDARD + NETZ_CLIP_UP
    expected = float(max(lo, min(hi, raw)))
    assert result == pytest.approx(expected)


def test_netzpreis_quadratic_midpoint():
    """L_norm=0.5 → raw = alpha * 0.25."""
    mid = (DEFAULT_LOAD_MIN + DEFAULT_LOAD_MAX) / 2
    result = netzpreis(mid)
    raw = DEFAULT_ALPHA * 0.25
    lo = DEFAULT_NETZ_STANDARD - NETZ_CLIP_DOWN
    hi = DEFAULT_NETZ_STANDARD + NETZ_CLIP_UP
    expected = max(lo, min(hi, raw))
    assert result == pytest.approx(expected)


def test_netzpreis_exceeds_clip_up():
    """Extreme load above max should still be clipped to standardtarif + clip_up."""
    result = netzpreis(1_000_000, load_min=0, load_max=100, standardtarif=10, alpha=1000)
    assert result == pytest.approx(10 + NETZ_CLIP_UP)


def test_netzpreis_below_minimum_clamped():
    """Load below load_min → L_norm clipped to 0 → result = floor."""
    result = netzpreis(-999, load_min=200, load_max=800, standardtarif=10)
    assert result == pytest.approx(10 - NETZ_CLIP_DOWN)


def test_netzpreis_zero_range_returns_standard():
    """When load_max == load_min, return standardtarif without division by zero."""
    result = netzpreis(500, load_min=500, load_max=500, standardtarif=10)
    assert result == pytest.approx(10.0)


# ─── energiepreis tests ───────────────────────────────────────────────────────


def test_energiepreis_unit_conversion():
    """80 EUR/MWh with k_pe=0.15, k_le=2.0 → 0.15 * 8 + 2 = 3.2 Rp./kWh."""
    result = energiepreis(
        epex_eur_mwh=80,
        k_pe=0.15,
        k_le=2.0,
        standardtarif=8.0,
    )
    expected = 0.15 * (80 / 10) + 2.0  # = 3.2
    lo = 8.0 - ENERGIE_CLIP_DOWN
    assert result == pytest.approx(max(lo, expected))


def test_energiepreis_clipped_high():
    """Very high EPEX price should be clipped at standardtarif + clip_up."""
    result = energiepreis(10_000, k_pe=1.0, k_le=0.0, standardtarif=8.0)
    assert result == pytest.approx(8.0 + ENERGIE_CLIP_UP)


def test_energiepreis_clipped_low():
    """Negative EPEX price (e.g. curtailment) should be clipped at floor."""
    result = energiepreis(-500, k_pe=0.15, k_le=2.0, standardtarif=8.0)
    assert result == pytest.approx(8.0 - ENERGIE_CLIP_DOWN)


def test_energiepreis_zero_epex():
    """0 EUR/MWh → k_le only."""
    result = energiepreis(0, k_pe=0.15, k_le=2.0, standardtarif=8.0)
    expected = 0.15 * 0 + 2.0  # = 2.0
    lo = 8.0 - ENERGIE_CLIP_DOWN
    assert result == pytest.approx(max(lo, expected))


# ─── gesamttarif tests ────────────────────────────────────────────────────────


def test_gesamttarif_sum():
    assert gesamttarif(10.0, 8.0) == pytest.approx(18.0)


def test_gesamttarif_with_zero():
    assert gesamttarif(0.0, 5.0) == pytest.approx(5.0)


def test_gesamttarif_negative_component():
    """Negative component (e.g. negative spot price passed through) is allowed."""
    assert gesamttarif(10.0, -2.0) == pytest.approx(8.0)


# ─── compute_tariff convenience wrapper ───────────────────────────────────────


def test_compute_tariff_returns_dict_keys():
    result = compute_tariff(net_load=400, epex_eur_mwh=80)
    assert "netzpreis_rp_kwh" in result
    assert "energiepreis_rp_kwh" in result
    assert "gesamttarif_rp_kwh" in result


def test_compute_tariff_gesamttarif_is_sum():
    result = compute_tariff(net_load=400, epex_eur_mwh=80)
    expected_sum = result["netzpreis_rp_kwh"] + result["energiepreis_rp_kwh"]
    assert result["gesamttarif_rp_kwh"] == pytest.approx(expected_sum, abs=0.01)


def test_compute_tariff_returns_floats():
    result = compute_tariff(net_load=400, epex_eur_mwh=80)
    for v in result.values():
        assert isinstance(v, float)
