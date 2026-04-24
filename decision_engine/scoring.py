from __future__ import annotations

import hashlib
from typing import Dict, List, Optional, Tuple

from .assumptions import DEFAULT_MORTGAGE_ASSUMPTIONS, SCENARIO_PRESETS
from .calculations import borrowing_impact, simulate_downside

DEFAULT_WEIGHTS = {
    "LiquidityScore": 0.20,
    "TenantRiskScore": 0.20,
    "GrowthScore": 0.20,
    "ConcentrationRiskScore": 0.10,
    "DownsideSurvivalScore": 0.15,
    "BorrowingImpactScore": 0.15,
}


def _norm(value: Optional[float], good: Tuple[float, float], invert: bool = False) -> float:
    if value is None:
        return 0.0
    low, high = good
    # clamp
    score = (value - low) / (high - low) if high != low else 0.0
    score = max(0.0, min(1.0, score))
    if invert:
        score = 1.0 - score
    return score * 100


def _safe_float(v: object) -> Optional[float]:
    try:
        f = float(v)
    except Exception:
        return None
    if f != f:  # NaN
        return None
    return f


def score_row(row: Dict[str, object], scenario_name: str = "balanced", weight_overrides: Optional[Dict[str, float]] = None) -> Dict[str, object]:
    weights = {**DEFAULT_WEIGHTS, **(weight_overrides or {})}

    dom = _safe_float(row.get("days_on_market"))
    vendor_disc = _safe_float(row.get("vendor_discount_pct"))
    sales_per_month = _safe_float(row.get("sales_per_month"))
    stock_on_market = _safe_float(row.get("stock_on_market_pct"))

    vacancy = _safe_float(row.get("vacancy_rate_pct"))
    rental_share = _safe_float(row.get("rental_share_pct"))
    income_to_rent = _safe_float(row.get("income_to_rent_ratio"))

    population_growth = _safe_float(row.get("population_growth_pct"))
    distance_cbd = _safe_float(row.get("distance_to_cbd_km"))

    industry_concentration = _safe_float(row.get("industry_concentration_pct"))

    liquidity_score = (
        _norm(dom or 120, (90, 10), invert=True) * 0.35
        + _norm(vendor_disc or 6, (7, 2), invert=True) * 0.25
        + _norm(sales_per_month or 5, (5, 60)) * 0.25
        + _norm(stock_on_market or 2.0, (2.5, 0.5), invert=True) * 0.15
    )

    tenant_score = (
        _norm(vacancy or 4.0, (5.0, 1.0), invert=True) * 0.6
        + _norm(rental_share or 40.0, (60.0, 25.0), invert=True) * 0.2
        + _norm(income_to_rent or 25.0, (20.0, 40.0)) * 0.2
    )

    growth_score = (
        _norm(population_growth or 0.5, (0.0, 3.0)) * 0.5
        + _norm(distance_cbd or 120, (40, 1), invert=True) * 0.3
        + _norm(row.get("dwelling_approvals_pct") or 0.5, (0.0, 3.0)) * 0.2
    )

    concentration_score = _norm(industry_concentration or 40.0, (60.0, 10.0), invert=True)

    downside = simulate_downside(
        price=_safe_float(row.get("median_3br_house_price_aud")) or _safe_float(row.get("median_2br_unit_price_aud")) or 500000,
        rent_pw=_safe_float(row.get("rent_3br_house_pw_aud")) or _safe_float(row.get("rent_2br_unit_pw_aud")) or 450,
        state=str(row.get("state") or "VIC"),
        scenario_name=scenario_name,
        cash_burn_threshold=500.0,
    )
    downside_score = _norm(downside.annual_cashflow, (-20000, 10000)) * 0.7 + _norm(-downside.worst_month_burn, (-3000, 0)) * 0.3

    borrowing = borrowing_impact(
        income_pa=_safe_float(row.get("user_income_pa")) or 180000.0,
        other_debt_monthly=_safe_float(row.get("user_other_debt")) or 800.0,
        savings_rate=0.25,
        equity_available=_safe_float(row.get("user_equity")) or 120000.0,
        target_deposit=120000.0,
        assumptions=DEFAULT_MORTGAGE_ASSUMPTIONS,
    )
    borrowing_score = _norm(borrowing.capacity_remaining, (200000, 1100000)) * 0.6 + _norm(borrowing.next_purchase_eta_months, (60, 6), invert=True) * 0.4

    weighted_sum = (
        liquidity_score * weights["LiquidityScore"]
        + tenant_score * weights["TenantRiskScore"]
        + growth_score * weights["GrowthScore"]
        + concentration_score * weights["ConcentrationRiskScore"]
        + downside_score * weights["DownsideSurvivalScore"]
        + borrowing_score * weights["BorrowingImpactScore"]
    )
    weight_total = sum(weights.values()) or 1.0
    final_score = weighted_sum / weight_total

    missing: List[str] = []
    for label, val in [
        ("vacancy_rate_pct", vacancy),
        ("days_on_market", dom),
        ("population_growth_pct", population_growth),
    ]:
        if val is None:
            missing.append(label)

    risk_score = 100 - (tenant_score * 0.5 + concentration_score * 0.5)

    scenario_hash = hashlib.sha1(scenario_name.encode("utf-8")).hexdigest()[:8]

    return {
        "BuyScore": round(final_score, 2),
        "LiquidityScore": round(liquidity_score, 2),
        "TenantRiskScore": round(tenant_score, 2),
        "GrowthScore": round(growth_score, 2),
        "ConcentrationRiskScore": round(concentration_score, 2),
        "DownsideSurvivalScore": round(downside_score, 2),
        "BorrowingImpactScore": round(borrowing_score, 2),
        "RiskScore": round(risk_score, 2),
        "scenario_hash": scenario_hash,
        "missing_metrics": missing,
        "forced_sale_risk": downside.forced_sale_risk,
        "stress_cashflow": round(downside.annual_cashflow, 2),
        "next_purchase_eta_months": round(borrowing.next_purchase_eta_months, 1),
    }
