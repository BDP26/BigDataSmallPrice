"""
Tariff formulas for BigDataSmallPrice – Phase 3 (Tarifformeln).

Converts model outputs (net-load forecast, EPEX price forecast) into
end-customer tariff prices in Rp./kWh.

Formula reference (analogous to EKZ/CKW dynamic tariff structure):

  Netzpreis    = floor + (ceiling − floor) · L_norm^p   (linear by default, clipped)
  Energiepreis = k_pe · (EPEX / 10) + k_le              (linear, clipped)
  Gesamttarif  = Netzpreis + Energiepreis

where:
  L_norm  = (net_load − load_min) / (load_max − load_min)   ∈ [0, 1]
  floor   = standardtarif − clip_down
  ceiling = standardtarif + clip_up
  p       = curvature exponent (1 = linear, 2 = quadratic)
  EPEX    = Day-ahead price in EUR/MWh
  10      = unit conversion factor (EUR/MWh → Rp./kWh: 1 EUR/MWh ≈ 0.1 Rp./kWh
            at 1 EUR = 1 CHF; multiply by 10 gives Rp., then divide by 10 → Rp./kWh)

Default parameter values are calibrated on EKZ-tariff and Winterthur load
data (2026). The linear default (p=1) keeps mid-range loads from collapsing
to the floor, which the old quadratic form did when the load model output
clusters near l_norm ≈ 0.5.
"""

from __future__ import annotations

import numpy as np

# ─── Default tariff parameters (kalibriert auf EKZ-Daten 2026) ───────────────
#
# Quelle: 30 Tage `ekz_tariffs_raw` + `winterthur_net_load_features` +
# `training_features.price_eur_mwh` (Stand 2026-05-06).
#
#   electricity (Energie):   4.00–14.00 Rp/kWh, Ø 8.84, σ 2.32
#   integrated (Gesamt):    12.23–29.03 Rp/kWh, Ø 19.76, σ 3.01
#   Netz-Anteil (integ−elec): 5.98–16.13 Rp/kWh, Ø 10.92, σ 1.61
#   net_load (kWh/15min):    p05=5986, p95=17510, Ø 12026
#   EPEX→EKZ-electricity Linearfit (r=0.71): elec_rp ≈ 0.0226·epex + 6.96

# Last-Range für Netzpreis-Normierung (5./95. Perzentil aus DB)
DEFAULT_LOAD_MIN: float =  6000.0   # kWh – schwache Nacht-/Sommerlast
DEFAULT_LOAD_MAX: float = 17500.0   # kWh – Winter-Peak

# Standardtarife (Rp./kWh) – Mittelwerte aus EKZ-Daten
DEFAULT_NETZ_STANDARD:    float = 11.0   # Rp./kWh (EKZ Netz-Anteil Ø)
DEFAULT_ENERGIE_STANDARD: float =  9.0   # Rp./kWh (EKZ electricity Ø)

# Konversionskonstanten (Energiepreis): aus Linearfit auf EKZ-electricity
# rp = (k_pe / 10) · epex_eur_mwh + k_le → k_pe = 0.226, k_le = 7.0
DEFAULT_K_PE: float = 0.226   # Skalierungsfaktor EPEX → Rp./kWh
DEFAULT_K_LE: float = 7.0     # Fixaufschlag Rp./kWh

# Netzpreis-Kurvenform: 1.0 = linear, 2.0 = quadratisch
# Linear gibt korrekten Output-Mittelwert auch bei engem Lastmodell-Output
DEFAULT_NETZ_EXPONENT: float = 1.0

# Backward-compat alias (alte Bedeutung "Skalierungsfaktor" entfällt durch
# Linear-Formel; einige Tests greifen noch auf DEFAULT_ALPHA zu).
DEFAULT_ALPHA: float = DEFAULT_NETZ_EXPONENT

# Clip-Breiten – an EKZ-Netz-Min/Max ausgerichtet (real: 8.1 – 14.0 Rp)
NETZ_CLIP_DOWN:    float = 3.0   # → floor    11 − 3 =  8 Rp (EKZ-Netz-Min ≈ 8.1)
NETZ_CLIP_UP:      float = 3.0   # → ceiling  11 + 3 = 14 Rp (EKZ-Netz-Max ≈ 14.0)
ENERGIE_CLIP_DOWN: float = 5.0   # → floor     9 − 5 =  4 Rp (EKZ-elec-Min)
ENERGIE_CLIP_UP:   float = 5.0   # → ceiling   9 + 5 = 14 Rp (EKZ-elec-Max)


# ─── Core formulas ────────────────────────────────────────────────────────────


def netzpreis(
    net_load: float,
    load_min: float = DEFAULT_LOAD_MIN,
    load_max: float = DEFAULT_LOAD_MAX,
    standardtarif: float = DEFAULT_NETZ_STANDARD,
    exponent: float = DEFAULT_NETZ_EXPONENT,
    clip_down: float = NETZ_CLIP_DOWN,
    clip_up: float = NETZ_CLIP_UP,
) -> float:
    """
    Calculate dynamic grid-usage tariff (Netzpreis) from net load.

    Maps L_norm linearly (or with a custom exponent) onto the range
    [floor, ceiling] = [standardtarif − clip_down, standardtarif + clip_up]:

        raw = floor + (ceiling − floor) · L_norm^exponent

    where L_norm = (net_load − load_min) / (load_max − load_min) ∈ [0, 1].

    Linear (exponent=1) keeps the per-slot output centred near standardtarif
    when the load model output clusters in the middle of the load range.
    Quadratic (exponent=2) reproduces the older curve.

    Args:
        net_load:      Forecasted net load in kWh (= bruttolastgang − PV).
        load_min:      Historical minimum net load for normalisation.
        load_max:      Historical maximum net load for normalisation.
        standardtarif: Base tariff in Rp./kWh (clip centre).
        exponent:      Curvature exponent (default 1 = linear).
        clip_down:     Max downward deviation from standardtarif.
        clip_up:       Max upward deviation from standardtarif.

    Returns:
        Grid tariff in Rp./kWh, clipped to [floor, ceiling].
    """
    load_range = load_max - load_min
    if load_range <= 0:
        return float(standardtarif)

    l_norm = (net_load - load_min) / load_range
    l_norm = float(np.clip(l_norm, 0.0, 1.0))
    floor   = standardtarif - clip_down
    ceiling = standardtarif + clip_up
    raw = floor + (ceiling - floor) * (l_norm ** exponent)
    return float(np.clip(raw, floor, ceiling))


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
    exponent: float = DEFAULT_NETZ_EXPONENT,
    k_pe: float = DEFAULT_K_PE,
    k_le: float = DEFAULT_K_LE,
) -> dict[str, float]:
    """
    Compute the full tariff breakdown from model outputs.

    Returns:
        Dict with keys: netzpreis_rp_kwh, energiepreis_rp_kwh, gesamttarif_rp_kwh
    """
    netz = netzpreis(net_load, load_min, load_max, netz_standard, exponent)
    energie = energiepreis(epex_eur_mwh, k_pe, k_le, energie_standard)
    return {
        "netzpreis_rp_kwh":    round(netz, 2),
        "energiepreis_rp_kwh": round(energie, 2),
        "gesamttarif_rp_kwh":  round(gesamttarif(netz, energie), 2),
    }
