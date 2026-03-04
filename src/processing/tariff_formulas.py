"""
Tariff formulas for BigDataSmallPrice – Phase 3 (Tarifformeln).

Converts model outputs (net-load forecast, EPEX price forecast) into
end-customer tariff prices in Rp./kWh.

Formula reference (analogous to EKZ/CKW dynamic tariff structure):

  Netzpreis    = α · L_norm²                 (quadratic, clipped)
  Energiepreis = k_pe · (EPEX / 10) + k_le  (linear, clipped)
  Gesamttarif  = Netzpreis + Energiepreis

where:
  L_norm = (net_load - load_min) / (load_max - load_min)   ∈ [0, 1]
  EPEX   = Day-ahead price in EUR/MWh
  10     = unit conversion factor (EUR/MWh → Rp./kWh: 1 EUR/MWh ≈ 0.1 Rp./kWh
           at 1 EUR = 1 CHF; multiply by 10 gives Rp., then divide by 10 → Rp./kWh)

Default parameter values are representative for Winterthur 2026.
"""

from __future__ import annotations

import numpy as np

# ─── Default tariff parameters (Winterthur 2026 estimate) ────────────────────

# Historische Last-Extremwerte (Winterthur, MW-Äquivalent in kWh/15min):
DEFAULT_LOAD_MIN: float = 150.0   # kWh – low-load summer night
DEFAULT_LOAD_MAX: float = 900.0   # kWh – peak winter morning

# Standardtarife (Rp./kWh) – Basis für Clipping
DEFAULT_NETZ_STANDARD:    float = 10.0   # Rp./kWh
DEFAULT_ENERGIE_STANDARD: float = 8.0    # Rp./kWh

# Konversionskonstanten (Energiepreis)
DEFAULT_K_PE: float = 0.15   # Skalierungsfaktor EPEX → Rp./kWh
DEFAULT_K_LE: float = 2.0    # Fixaufschlag Rp./kWh

# Netzpreis-Skalierung
DEFAULT_ALPHA: float = 15.0  # Max-Aufschlag bei Volllast (Rp./kWh)

# Clip-Breiten
NETZ_CLIP_DOWN:    float = 5.0
NETZ_CLIP_UP:      float = 15.0
ENERGIE_CLIP_DOWN: float = 5.0
ENERGIE_CLIP_UP:   float = 5.0


# ─── Core formulas ────────────────────────────────────────────────────────────


def netzpreis(
    net_load: float,
    load_min: float = DEFAULT_LOAD_MIN,
    load_max: float = DEFAULT_LOAD_MAX,
    standardtarif: float = DEFAULT_NETZ_STANDARD,
    alpha: float = DEFAULT_ALPHA,
    clip_down: float = NETZ_CLIP_DOWN,
    clip_up: float = NETZ_CLIP_UP,
) -> float:
    """
    Calculate dynamic grid-usage tariff (Netzpreis) from net load.

    Uses a quadratic formula: price = α · L_norm²
    where L_norm = (net_load − load_min) / (load_max − load_min).

    The result is clipped to [standardtarif − clip_down, standardtarif + clip_up].

    Args:
        net_load:      Forecasted net load in kWh (= bruttolastgang − PV).
        load_min:      Historical minimum net load for normalisation.
        load_max:      Historical maximum net load for normalisation.
        standardtarif: Base tariff in Rp./kWh (clip centre).
        alpha:         Scaling factor (max surcharge at full load, Rp./kWh).
        clip_down:     Max downward deviation from standardtarif.
        clip_up:       Max upward deviation from standardtarif.

    Returns:
        Grid tariff in Rp./kWh.
    """
    load_range = load_max - load_min
    if load_range <= 0:
        return float(standardtarif)

    l_norm = (net_load - load_min) / load_range
    l_norm = float(np.clip(l_norm, 0.0, 1.0))
    raw = alpha * (l_norm ** 2)
    lo = standardtarif - clip_down
    hi = standardtarif + clip_up
    return float(np.clip(raw, lo, hi))


def energiepreis(
    epex_eur_mwh: float,
    k_pe: float = DEFAULT_K_PE,
    k_le: float = DEFAULT_K_LE,
    standardtarif: float = DEFAULT_ENERGIE_STANDARD,
    clip_down: float = ENERGIE_CLIP_DOWN,
    clip_up: float = ENERGIE_CLIP_UP,
) -> float:
    """
    Calculate dynamic energy tariff (Energiepreis) from EPEX Day-Ahead price.

    Formula (analogous to EKZ):
        rp_kwh = k_pe × (epex_eur_mwh / 10) + k_le

    The division by 10 converts EUR/MWh to a Rp./kWh-compatible scale
    (1 EUR/MWh = 0.1 Rp./kWh at parity; k_pe adjusts the pass-through rate).

    Result is clipped to [standardtarif − clip_down, standardtarif + clip_up].

    Args:
        epex_eur_mwh:  EPEX Day-Ahead price in EUR/MWh.
        k_pe:          Pass-through factor (dimensionless).
        k_le:          Fixed surcharge in Rp./kWh.
        standardtarif: Base tariff in Rp./kWh (clip centre).
        clip_down:     Max downward deviation.
        clip_up:       Max upward deviation.

    Returns:
        Energy tariff in Rp./kWh.
    """
    rp_kwh = k_pe * (epex_eur_mwh / 10.0) + k_le
    lo = standardtarif - clip_down
    hi = standardtarif + clip_up
    return float(np.clip(rp_kwh, lo, hi))


def gesamttarif(netz_rp: float, energie_rp: float) -> float:
    """
    Sum grid and energy tariffs to the total end-customer tariff.

    Args:
        netz_rp:    Grid tariff in Rp./kWh.
        energie_rp: Energy tariff in Rp./kWh.

    Returns:
        Total tariff in Rp./kWh.
    """
    return netz_rp + energie_rp


# ─── Convenience ──────────────────────────────────────────────────────────────


def compute_tariff(
    net_load: float,
    epex_eur_mwh: float,
    load_min: float = DEFAULT_LOAD_MIN,
    load_max: float = DEFAULT_LOAD_MAX,
    netz_standard: float = DEFAULT_NETZ_STANDARD,
    energie_standard: float = DEFAULT_ENERGIE_STANDARD,
    alpha: float = DEFAULT_ALPHA,
    k_pe: float = DEFAULT_K_PE,
    k_le: float = DEFAULT_K_LE,
) -> dict[str, float]:
    """
    Compute the full tariff breakdown from model outputs.

    Returns:
        Dict with keys: netzpreis_rp_kwh, energiepreis_rp_kwh, gesamttarif_rp_kwh
    """
    netz = netzpreis(net_load, load_min, load_max, netz_standard, alpha)
    energie = energiepreis(epex_eur_mwh, k_pe, k_le, energie_standard)
    return {
        "netzpreis_rp_kwh":    round(netz, 2),
        "energiepreis_rp_kwh": round(energie, 2),
        "gesamttarif_rp_kwh":  round(gesamttarif(netz, energie), 2),
    }
