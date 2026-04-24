from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Optional


def safe_pct(num: Optional[float], denom: Optional[float]) -> Optional[float]:
    if num is None or denom in (None, 0):
        return None
    try:
        return (num / denom) * 100.0
    except Exception:
        return None


def price_delta_3_to_4(sale_median_3: Optional[float], sale_median_4: Optional[float]) -> tuple[Optional[float], Optional[float]]:
    if sale_median_3 is None or sale_median_4 is None:
        return None, None
    delta = sale_median_4 - sale_median_3
    pct = safe_pct(delta, sale_median_3)
    return delta, pct


def premium_stability(history: Iterable[float]) -> Optional[float]:
    vals = [v for v in history if v is not None]
    if len(vals) < 2:
        return 1.0 if vals else None
    mean = sum(vals) / len(vals)
    if mean == 0:
        return 0.0
    variance = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = math.sqrt(variance)
    ratio = min(std / abs(mean), 1.5)
    # Stability closer to 1.0 when volatility is low
    return max(0.0, 1.0 - ratio)


def refi_confidence_score(comp_count: Optional[int], dispersion_pct: Optional[float]) -> float:
    # Base weight from number of comps
    count_score = 0.0
    if comp_count is not None:
        if comp_count >= 20:
            count_score = 0.95
        elif comp_count >= 15:
            count_score = 0.8
        elif comp_count >= 10:
            count_score = 0.65
        elif comp_count >= 5:
            count_score = 0.5
        elif comp_count >= 3:
            count_score = 0.35
        elif comp_count >= 1:
            count_score = 0.25
        else:
            count_score = 0.1
    # Penalty for wide dispersion
    dispersion_penalty = 0.0
    if dispersion_pct is not None:
        if dispersion_pct > 25:
            dispersion_penalty = 0.25
        elif dispersion_pct > 20:
            dispersion_penalty = 0.18
        elif dispersion_pct > 15:
            dispersion_penalty = 0.12
        elif dispersion_pct > 10:
            dispersion_penalty = 0.08
        elif dispersion_pct > 6:
            dispersion_penalty = 0.05
    score = max(0.0, min(1.0, count_score - dispersion_penalty))
    return score


def capture_pct_from_refi(score: float) -> float:
    # Map confidence to capture of manufactured equity
    if score >= 0.9:
        return 0.75
    if score >= 0.8:
        return 0.7
    if score >= 0.65:
        return 0.65
    if score >= 0.5:
        return 0.6
    if score >= 0.35:
        return 0.55
    return 0.5


def manufactured_equity(expected_bank_value: float, all_in_cost: float) -> float:
    return expected_bank_value - all_in_cost


def usable_equity(expected_bank_value: float, total_debt: float, lvr: float = 0.8) -> float:
    return expected_bank_value * lvr - total_debt


def equity_score(manufactured_eq: float, usable_eq: float, reno_cost: float, premium_stability: Optional[float]) -> float:
    if reno_cost <= 0:
        return 0.0
    roi = manufactured_eq / reno_cost
    usable_factor = 1.0 if usable_eq > 0 else 0.2
    roi_score = min(max((roi - 0.8) / 2.5, 0.0), 1.0)  # >1.0 ROI lifts score
    stability = premium_stability if premium_stability is not None else 0.6
    return max(0.0, min(1.0, 0.5 * roi_score + 0.3 * usable_factor + 0.2 * stability))


def deal_score(equity_score_val: float, refi_conf_score: float) -> float:
    return max(0.0, min(1.0, 0.65 * equity_score_val + 0.35 * refi_conf_score))


def expected_bank_value(
    purchase_price: float,
    reno_cost: float,
    three_bed_median: Optional[float],
    four_bed_median: Optional[float],
    refi_confidence_pct: float,
    weighted_median_4br_comps: Optional[float],
    comps_count: int,
    suburb_confidence_factor: float = 0.9,
    market_decline_flag: bool = False,
) -> tuple[Optional[float], Optional[float]]:
    """Estimate expected bank value and capture percentage under bedroom ladder rules.

    - lower bound anchored to cost + floor on 3BR median.
    - upper bound driven by comps when available; otherwise 4BR median scaled by suburb confidence.
    - confidence interpolates between bounds; optionally prevent values below purchase unless decline flagged.
    Returns (expected_bank_value, capture_pct_of_4br_median).
    """

    if four_bed_median is None or purchase_price is None or reno_cost is None:
        return None, None

    base_cost_floor = purchase_price + reno_cost * 0.6
    three_bed_floor = (three_bed_median * 0.95) if three_bed_median is not None else base_cost_floor
    lower_bound = max(base_cost_floor, three_bed_floor)

    if comps_count >= 5 and weighted_median_4br_comps is not None:
        upper_bound = weighted_median_4br_comps
    else:
        if four_bed_median is None:
            return None, None
        upper_bound = four_bed_median * suburb_confidence_factor

    confidence = max(0.0, min(1.0, (refi_confidence_pct or 0.0) / 100.0))
    expected = lower_bound + confidence * (upper_bound - lower_bound)

    if not market_decline_flag and expected < purchase_price:
        expected = purchase_price

    capture_pct = expected / four_bed_median if four_bed_median else None
    return expected, capture_pct


@dataclass
class DealInputs:
    purchase_price: float
    stamp_duty: float
    legal_fees: float
    reno_cost: float
    contingency_pct: float
    duration_weeks: float
    interest_rate: float
    holding_weekly: float = 0.0
    total_debt: Optional[float] = None


def all_in_cost(inputs: DealInputs) -> float:
    contingency_amt = inputs.reno_cost * inputs.contingency_pct
    debt = inputs.total_debt if inputs.total_debt is not None else inputs.purchase_price
    interest = debt * (inputs.interest_rate / 100.0) * (inputs.duration_weeks / 52.0)
    holding = inputs.holding_weekly * inputs.duration_weeks
    return (
        inputs.purchase_price
        + inputs.stamp_duty
        + inputs.legal_fees
        + inputs.reno_cost
        + contingency_amt
        + interest
        + holding
    )
