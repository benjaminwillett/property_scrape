from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import Iterable, Optional

from .tax.vic import stamp_duty as stamp_duty_vic


@dataclass
class SegmentationResult:
    ratio: Optional[float]
    label: str
    max_capture: float
    uplift_factor_cap: float


@dataclass
class CostInputs:
    purchase_price: float
    reno_cost: float
    contingency_pct: float
    interest_rate: float
    duration_weeks: float
    legal_fees: float
    deposit_pct: float
    reno_funding_mode: str = "cash"  # "cash" or "borrowed"
    misc_costs: float = 2500.0
    include_rent_during_reno: bool = False
    vacancy_weeks: Optional[float] = None
    rent_pw: Optional[float] = None
    state: str = "VIC"


@dataclass
class CostBreakdown:
    purchase_price: float
    stamp_duty: float
    reno_contingency: float
    interest_during_reno: float
    legal_fees: float
    misc_costs: float
    total_debt: float
    all_in_cost: float
    purchase_debt: float
    reno_debt: float
    deposit_cash: float
    reno_cash: float
    rent_income: float
    cash_in: float


@dataclass
class ValuationInputs:
    purchase_price: float
    reno_cost: float
    three_bed_median: Optional[float]
    four_bed_median: Optional[float]
    refi_confidence_score: float
    weighted_comp_value: Optional[float]
    comp_count: int
    max_capture: float
    uplift_factor_cap: float
    valuation_mode: str
    comp_count_12m: Optional[int] = None
    p75_comps_12m: Optional[float] = None
    market_decline_flag: bool = False
    capture_discount: float = 1.0


@dataclass
class ValuationResult:
    expected_value: float
    capture_pct: Optional[float]
    effective_capture: Optional[float]
    base_capture: Optional[float]
    capture_discount: float
    lower_bound: float
    upper_bound: float
    valuation_mode: str


@dataclass
class StabilityResult:
    score: float
    label: str


def clamp01(val: float) -> float:
    return max(0.0, min(1.0, val))


def segmentation_from_medians(three_bed_median: Optional[float], four_bed_median: Optional[float]) -> SegmentationResult:
    if three_bed_median in (None, 0) or four_bed_median in (None, 0):
        return SegmentationResult(ratio=None, label="unknown", max_capture=0.85, uplift_factor_cap=3.0)

    ratio = (four_bed_median - three_bed_median) / three_bed_median
    if ratio > 0.70:
        return SegmentationResult(ratio=ratio, label="prestige", max_capture=0.55, uplift_factor_cap=2.0)
    if ratio > 0.45:
        return SegmentationResult(ratio=ratio, label="high", max_capture=0.70, uplift_factor_cap=3.0)
    if ratio > 0.25:
        return SegmentationResult(ratio=ratio, label="medium", max_capture=0.85, uplift_factor_cap=4.0)
    return SegmentationResult(ratio=ratio, label="low", max_capture=0.95, uplift_factor_cap=5.5)


def premium_stability_score(history: Iterable[Optional[float]]) -> StabilityResult:
    vals = [v for v in history if v is not None]
    if not vals:
        return StabilityResult(score=1.0, label="unknown")
    if len(vals) == 1:
        return StabilityResult(score=1.0, label="stable")
    median_val = statistics.median(vals)
    if median_val == 0:
        return StabilityResult(score=0.0, label="unstable")
    deviations = [abs(v - median_val) / abs(median_val) for v in vals]
    max_dev = max(deviations)
    if max_dev <= 0.20:
        return StabilityResult(score=1.0, label="stable")
    # degrade linearly beyond the ±20% band, floor at 0.2
    score = max(0.2, 1.0 - (max_dev - 0.20))
    label = "variable" if score >= 0.6 else "volatile"
    return StabilityResult(score=score, label=label)


def dispersion_from_prices(prices: Optional[Iterable[Optional[float]]]) -> Optional[float]:
    if prices is None:
        return None
    values = [float(p) for p in prices if p is not None]
    if len(values) < 2:
        return None
    values.sort()
    quartiles = statistics.quantiles(values, n=4, method="inclusive")
    if len(quartiles) < 3:
        return None
    q1, q3 = quartiles[0], quartiles[2]
    median_val = statistics.median(values)
    if median_val == 0:
        return None
    return (q3 - q1) / median_val


def tightness_score_from_dispersion(dispersion: Optional[float]) -> float:
    if dispersion is None:
        return 0.35
    if dispersion <= 0.12:
        return 1.0
    if dispersion <= 0.22:
        # map 0.12..0.22 down to 1.0..0.5 linearly
        span = 0.22 - 0.12
        return max(0.5, 1.0 - ((dispersion - 0.12) / span) * 0.5)
    # above 0.22 map linearly down to 0.1
    # cap to reasonable max (say 0.50 span to 0.1)
    return max(0.1, 0.5 - (dispersion - 0.22))


def plausibility_score(
    median_2br_house: Optional[float],
    median_3br_house: Optional[float],
    median_4br_house: Optional[float],
) -> float:
    if median_2br_house in (None, 0) or median_3br_house in (None, 0) or median_4br_house in (None, 0):
        return 0.5
    gap_2to3 = (median_3br_house - median_2br_house) / max(median_2br_house, 1e-9)
    gap_3to4 = (median_4br_house - median_3br_house) / max(median_3br_house, 1e-9)
    ratio = gap_3to4 / max(gap_2to3, 0.01)

    if 0.8 <= ratio <= 1.6:
        score = 1.0
    elif 0.5 <= ratio < 0.8 or 1.6 < ratio <= 2.2:
        score = 0.7
    else:
        score = 0.4

    if gap_3to4 > 0.50 and gap_2to3 < 0.20:
        score = min(score, 0.4)
    return max(0.0, min(1.0, score))


def land_proxy_score(median_3br_house: Optional[float], median_3br_unit: Optional[float]) -> float:
    if median_3br_house in (None, 0) or median_3br_unit in (None, 0):
        return 0.5
    ratio = median_3br_unit / median_3br_house
    proxy = 1 - clamp01(ratio)
    return max(0.0, min(1.0, proxy))


def valuation_safety_score(
    refi_confidence_score: float,
    tightness: float,
    plausibility: float,
    land_proxy: float,
) -> float:
    safety01 = (
        0.35 * clamp01(refi_confidence_score / 100.0)
        + 0.30 * clamp01(tightness)
        + 0.25 * clamp01(plausibility)
        + 0.10 * clamp01(land_proxy)
    )
    return max(0.0, min(100.0, safety01 * 100.0))


def refi_confidence(comp_count: Optional[int], dispersion_pct: Optional[float], liquidity_per_month: Optional[float], recency_days: Optional[int]) -> float:
    # Component scores are 0-100, weighted per spec
    # Comp count (40%)
    cc = comp_count or 0
    if cc >= 20:
        count_score = 100.0
    elif cc >= 15:
        count_score = 85.0
    elif cc >= 10:
        count_score = 75.0
    elif cc >= 5:
        count_score = 60.0
    elif cc >= 3:
        count_score = 45.0
    elif cc >= 1:
        count_score = 35.0
    else:
        count_score = 20.0

    # Dispersion (30%) — lower dispersion is better
    if dispersion_pct is None:
        dispersion_score = 65.0
    elif dispersion_pct <= 5:
        dispersion_score = 100.0
    elif dispersion_pct <= 10:
        dispersion_score = 85.0
    elif dispersion_pct <= 15:
        dispersion_score = 70.0
    elif dispersion_pct <= 20:
        dispersion_score = 55.0
    elif dispersion_pct <= 30:
        dispersion_score = 35.0
    else:
        dispersion_score = 25.0

    # Liquidity proxy (20%) — if missing, neutral 50
    if liquidity_per_month is None:
        liquidity_score = 50.0
    elif liquidity_per_month >= 40:
        liquidity_score = 100.0
    elif liquidity_per_month >= 20:
        liquidity_score = 80.0
    elif liquidity_per_month >= 10:
        liquidity_score = 65.0
    elif liquidity_per_month >= 5:
        liquidity_score = 50.0
    elif liquidity_per_month > 0:
        liquidity_score = 35.0
    else:
        liquidity_score = 25.0

    # Recency (10%) — fresher data increases confidence
    if recency_days is None:
        recency_score = 50.0
    elif recency_days <= 45:
        recency_score = 100.0
    elif recency_days <= 90:
        recency_score = 80.0
    elif recency_days <= 180:
        recency_score = 65.0
    else:
        recency_score = 45.0

    weighted = (
        0.40 * count_score
        + 0.30 * dispersion_score
        + 0.20 * liquidity_score
        + 0.10 * recency_score
    )
    return max(20.0, min(100.0, weighted))


def classify_valuation_mode(seg_ratio: Optional[float], dispersion_4br: Optional[float], comp_count_4br: Optional[int]) -> str:
    has_comps = (comp_count_4br or 0) > 0
    if seg_ratio is None or dispersion_4br is None or not has_comps:
        return "segmented"
    if seg_ratio <= 0.40 and dispersion_4br <= 0.25:
        return "homogeneous"
    return "segmented"


def compute_total_debt(purchase_price: float, deposit_pct: float, reno_total: float, reno_funding_mode: str) -> tuple[float, float, float]:
    deposit_cash = purchase_price * (deposit_pct / 100.0)
    purchase_debt = purchase_price - deposit_cash
    borrowed_reno = reno_total if reno_funding_mode == "borrowed" else 0.0
    total_debt = purchase_debt + borrowed_reno
    return purchase_debt, borrowed_reno, total_debt


def compute_rent_income(include_rent: bool, rent_pw: Optional[float], duration_weeks: float, vacancy_weeks: Optional[float]) -> float:
    if not include_rent or rent_pw is None:
        return 0.0
    vac = vacancy_weeks if vacancy_weeks is not None else duration_weeks
    rent_weeks = max(0.0, duration_weeks - vac)
    return rent_pw * rent_weeks


def compute_cash_in(
    purchase_price: float,
    deposit_pct: float,
    stamp_duty: float,
    legal_costs: float,
    misc_costs: float,
    reno_total: float,
    reno_funding_mode: str,
    interest_cost: float,
    rent_income: float,
) -> float:
    deposit_cash = purchase_price * (deposit_pct / 100.0)
    reno_cash = 0.0 if reno_funding_mode == "borrowed" else reno_total
    cash_in = deposit_cash + stamp_duty + legal_costs + misc_costs + reno_cash + interest_cost - rent_income
    return max(0.0, cash_in)


def compute_recycle_metrics(cash_in: float, usable_equity_selected_lvr: float) -> dict:
    cash_recovered = max(0.0, usable_equity_selected_lvr)
    cash_tied_up = max(0.0, cash_in - cash_recovered)
    recycle_ratio = cash_recovered / cash_in if cash_in > 0 else 0.0
    return {
        "cash_recovered": cash_recovered,
        "cash_tied_up": cash_tied_up,
        "recycle_ratio": recycle_ratio,
    }


def compute_repeatability_score(
    recycle_ratio: float,
    refi_confidence_score: float,
    stability_score: float,
    valuation_mode: str,
    segmentation_label: str,
    manufactured_eq: float,
    rent_support_score: float,
) -> float:
    base = (
        50.0 * clamp01(recycle_ratio / 0.85)
        + 20.0 * clamp01(refi_confidence_score / 100.0)
        + 20.0 * clamp01(stability_score)
        + 10.0 * clamp01(rent_support_score / 100.0)
    )
    if valuation_mode == "segmented":
        base *= 0.90
    if segmentation_label == "prestige":
        base *= 0.70
    if manufactured_eq < 0 and recycle_ratio < 0.50 and base > 55.0:
        base = 55.0
    return max(0.0, min(100.0, base))


def suggested_capture_from_confidence(confidence_score: float, comp_count: int) -> float:
    if confidence_score >= 80:
        base = 0.95
    elif confidence_score >= 65:
        base = 0.90
    elif confidence_score >= 50:
        base = 0.85
    elif confidence_score >= 35:
        base = 0.80
    else:
        base = 0.75
    if comp_count < 5:
        base = max(base, 0.70)
    return base


def cost_model(inputs: CostInputs) -> CostBreakdown:
    contingency = inputs.reno_cost * (inputs.contingency_pct or 0.0)
    reno_total = inputs.reno_cost + contingency
    stamp = stamp_duty_vic(inputs.purchase_price) if inputs.state.upper() == "VIC" else 0.0

    purchase_debt, reno_debt, total_debt = compute_total_debt(inputs.purchase_price, inputs.deposit_pct, reno_total, inputs.reno_funding_mode)
    interest_base = purchase_debt + reno_debt
    interest = interest_base * (inputs.interest_rate / 100.0) * (inputs.duration_weeks / 52.0)

    rent_income = compute_rent_income(inputs.include_rent_during_reno, inputs.rent_pw, inputs.duration_weeks, inputs.vacancy_weeks)
    cash_in = compute_cash_in(
        inputs.purchase_price,
        inputs.deposit_pct,
        stamp,
        inputs.legal_fees,
        inputs.misc_costs,
        reno_total,
        inputs.reno_funding_mode,
        interest,
        rent_income,
    )

    reno_cash = 0.0 if inputs.reno_funding_mode == "borrowed" else reno_total
    deposit_cash = inputs.purchase_price * (inputs.deposit_pct / 100.0)

    all_in = inputs.purchase_price + stamp + inputs.legal_fees + inputs.misc_costs + reno_total + interest
    return CostBreakdown(
        purchase_price=inputs.purchase_price,
        stamp_duty=stamp,
        reno_contingency=contingency,
        interest_during_reno=interest,
        legal_fees=inputs.legal_fees,
        misc_costs=inputs.misc_costs,
        total_debt=total_debt,
        all_in_cost=all_in,
        purchase_debt=purchase_debt,
        reno_debt=reno_debt,
        deposit_cash=deposit_cash,
        reno_cash=reno_cash,
        rent_income=rent_income,
        cash_in=cash_in,
    )


def bank_valuation(inputs: ValuationInputs) -> Optional[ValuationResult]:
    if inputs.four_bed_median in (None, 0):
        return None

    conf01 = clamp01(inputs.refi_confidence_score / 100.0)
    cost_floor = inputs.purchase_price + inputs.reno_cost * 0.60
    three_bed_floor = (inputs.three_bed_median * 0.95) if inputs.three_bed_median else cost_floor
    lower_bound = max(cost_floor, three_bed_floor)

    suggested_capture = suggested_capture_from_confidence(inputs.refi_confidence_score, inputs.comp_count)
    base_capture = min(suggested_capture, inputs.max_capture)

    capture_pre = base_capture
    if inputs.weighted_comp_value is not None and inputs.four_bed_median:
        comp_capture = inputs.weighted_comp_value / inputs.four_bed_median
        blend_weight = min(1.0, inputs.comp_count / 15.0)
        blended_capture = (base_capture * (1 - blend_weight)) + (comp_capture * blend_weight)
        capture_pre = min(inputs.max_capture, max(0.40, blended_capture))

    effective_capture = capture_pre * clamp01(inputs.capture_discount)
    upper_bound = inputs.four_bed_median * effective_capture

    mode = inputs.valuation_mode or "segmented"
    if mode == "homogeneous":
        expected_val = lower_bound + conf01 * (upper_bound - lower_bound)
        expected_val = min(expected_val, inputs.four_bed_median * 0.98)
        if inputs.comp_count_12m and inputs.comp_count_12m >= 8 and inputs.p75_comps_12m:
            expected_val = min(expected_val, inputs.p75_comps_12m)
        if not inputs.market_decline_flag and expected_val < inputs.purchase_price:
            expected_val = inputs.purchase_price
    else:
        expected_val = lower_bound + conf01 * (upper_bound - lower_bound)
        if not inputs.market_decline_flag and expected_val < inputs.purchase_price:
            expected_val = inputs.purchase_price
        factor = inputs.uplift_factor_cap * (0.70 + 0.60 * conf01)
        max_uplift = inputs.reno_cost * factor
        expected_val = min(expected_val, inputs.purchase_price + max_uplift)

    capture_pct = expected_val / inputs.four_bed_median if inputs.four_bed_median else None
    return ValuationResult(
        expected_value=expected_val,
        capture_pct=capture_pct,
        effective_capture=effective_capture,
        base_capture=base_capture,
        capture_discount=inputs.capture_discount,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        valuation_mode=mode,
    )


def manufactured_equity(expected_bank_value: float, all_in_cost: float) -> float:
    return expected_bank_value - all_in_cost


def usable_equity(expected_bank_value: float, total_debt: float, lvr: float = 0.80) -> float:
    return expected_bank_value * lvr - total_debt


def deal_score(
    manufactured_eq: float,
    usable_eq: float,
    reno_cost: float,
    refi_confidence_score: float,
    premium_stability: float,
    valuation_mode: str,
) -> float:
    if reno_cost <= 0:
        return 0.0
    roi = manufactured_eq / reno_cost
    roi_score = max(0.0, min(1.0, (roi - 0.8) / 2.5))
    usable_score = 1.0 if usable_eq > 0 else 0.2
    stability_score = clamp01(premium_stability)
    conf_score = clamp01(refi_confidence_score / 100.0)
    equity_score = 0.50 * roi_score + 0.20 * usable_score + 0.30 * stability_score
    final = 0.60 * equity_score + 0.40 * conf_score
    if valuation_mode == "segmented":
        final *= 0.85
    if manufactured_eq < 0 and final > 0.60:
        final = 0.60
    return max(0.0, min(1.0, final))


def analyse_row(
    three_bed_median: Optional[float],
    four_bed_median: Optional[float],
    premium_history: Iterable[Optional[float]],
    comp_count_4br: Optional[int],
    sale_dispersion_pct: Optional[float],
    liquidity_per_month: Optional[float],
    recency_days: Optional[int],
    purchase_discount_pct: float,
    reno_cost: float,
    contingency_pct: float,
    duration_weeks: float,
    interest_rate: float,
    legal_fees: float,
    deposit_pct: float,
    reno_funding_mode: str,
    misc_costs: float,
    include_rent_during_reno: bool,
    vacancy_weeks: Optional[float],
    lvr_refi: float,
    state: str = "VIC",
    weighted_median_4br_comps: Optional[float] = None,
    rent_median_3br_house_pw: Optional[float] = None,
    rent_median_4br_house_pw: Optional[float] = None,
    comp_prices_4br: Optional[Iterable[Optional[float]]] = None,
    comp_count_12m: Optional[int] = None,
    p75_comps_12m: Optional[float] = None,
    median_2br_house: Optional[float] = None,
    median_3br_unit: Optional[float] = None,
) -> Optional[dict]:
    if three_bed_median in (None, 0) or four_bed_median in (None, 0):
        return None

    purchase_price = three_bed_median * (1 - purchase_discount_pct / 100.0)
    seg = segmentation_from_medians(three_bed_median, four_bed_median)
    stability = premium_stability_score(premium_history)
    confidence = refi_confidence(comp_count_4br, sale_dispersion_pct, liquidity_per_month, recency_days)
    dispersion_4br = None
    if comp_prices_4br is not None:
        dispersion_4br = dispersion_from_prices(comp_prices_4br)
    else:
        candidate_dispersion = sale_dispersion_pct
        if candidate_dispersion is not None:
            dispersion_4br = candidate_dispersion / (100.0 if candidate_dispersion > 1 else 1.0)
    valuation_mode = classify_valuation_mode(seg.ratio, dispersion_4br, comp_count_4br)

    tightness = tightness_score_from_dispersion(dispersion_4br)
    plausibility = plausibility_score(median_2br_house, three_bed_median, four_bed_median)
    land_proxy = land_proxy_score(three_bed_median, median_3br_unit)
    valuation_safety = valuation_safety_score(confidence, tightness, plausibility, land_proxy)
    safety_discount = clamp01(0.55 + 0.45 * (valuation_safety / 100.0))
    costs = cost_model(
        CostInputs(
            purchase_price=purchase_price,
            reno_cost=reno_cost,
            contingency_pct=contingency_pct,
            interest_rate=interest_rate,
            duration_weeks=duration_weeks,
            legal_fees=legal_fees,
            deposit_pct=deposit_pct,
            reno_funding_mode=reno_funding_mode,
            misc_costs=misc_costs,
            include_rent_during_reno=include_rent_during_reno,
            vacancy_weeks=vacancy_weeks,
            rent_pw=rent_median_3br_house_pw,
            state=state,
        )
    )

    valuation = bank_valuation(
        ValuationInputs(
            purchase_price=purchase_price,
            reno_cost=reno_cost,
            three_bed_median=three_bed_median,
            four_bed_median=four_bed_median,
            refi_confidence_score=confidence,
            weighted_comp_value=weighted_median_4br_comps,
            comp_count=int(comp_count_4br or 0),
            max_capture=seg.max_capture,
            uplift_factor_cap=seg.uplift_factor_cap,
            valuation_mode=valuation_mode,
            comp_count_12m=comp_count_12m or comp_count_4br,
            p75_comps_12m=p75_comps_12m,
            capture_discount=safety_discount,
        )
    )
    if valuation is None:
        return None

    manuf_eq = manufactured_equity(valuation.expected_value, costs.all_in_cost)
    usable_eq_80 = usable_equity(valuation.expected_value, costs.total_debt, 0.80)
    usable_eq_90 = usable_equity(valuation.expected_value, costs.total_debt, 0.90)
    usable_eq_selected = usable_equity(valuation.expected_value, costs.total_debt, lvr_refi)

    recycle = compute_recycle_metrics(costs.cash_in, usable_eq_selected)

    rent_support_score = 50.0
    if rent_median_4br_house_pw and valuation.expected_value:
        yield_proxy = (rent_median_4br_house_pw * 52.0) / valuation.expected_value
        if yield_proxy <= 0.025:
            yield_score = 0.0
        elif yield_proxy >= 0.055:
            yield_score = 1.0
        else:
            yield_score = (yield_proxy - 0.025) / (0.055 - 0.025)
        trend_score = 0.5  # placeholder until history available
        rent_support01 = 0.5 * clamp01(yield_score) + 0.5 * clamp01(trend_score)
        rent_support_score = max(0.0, min(100.0, rent_support01 * 100.0))
    repeatability = compute_repeatability_score(
        recycle["recycle_ratio"],
        confidence,
        stability.score,
        valuation.valuation_mode,
        seg.label,
        manuf_eq,
        rent_support_score,
    )

    score = deal_score(manuf_eq, usable_eq_selected, reno_cost, confidence, stability.score, valuation.valuation_mode)

    net_holding_cost = costs.interest_during_reno - costs.rent_income

    captureable_gap_pct = min(valuation.effective_capture or 0.0, seg.max_capture)
    captureable_upside = manuf_eq * safety_discount

    return {
        "segmentation": seg.label,
        "segmentation_ratio": seg.ratio,
        "max_capture": seg.max_capture,
        "uplift_factor_cap": seg.uplift_factor_cap,
        "premium_stability": stability.score,
        "premium_stability_label": stability.label,
        "refi_confidence_score": confidence,
        "expected_bank_value": valuation.expected_value,
        "capture_pct": valuation.capture_pct,
        "effective_capture": valuation.effective_capture,
        "base_capture": valuation.base_capture,
        "capture_discount": valuation.capture_discount,
        "captureable_gap_pct": captureable_gap_pct,
        "captureable_upside": captureable_upside,
        "valuation_mode": valuation.valuation_mode,
        "dispersion_4br": dispersion_4br,
        "valuation_safety_score": valuation_safety,
        "tightness_score": tightness,
        "plausibility_score": plausibility,
        "land_proxy_score": land_proxy,
        "all_in_cost": costs.all_in_cost,
        "manufactured_equity": manuf_eq,
        "usable_equity_80": usable_eq_80,
        "usable_equity_90": usable_eq_90,
        "usable_equity_selected_lvr": usable_eq_selected,
        "deal_score": score * 100,
        "repeatability_score": repeatability,
        "rent_support_score": rent_support_score,
        "purchase_price": purchase_price,
        "stamp_duty": costs.stamp_duty,
        "contingency": costs.reno_contingency,
        "interest_during_reno": costs.interest_during_reno,
        "misc_costs": costs.misc_costs,
        "deposit_cash": costs.deposit_cash,
        "reno_cash": costs.reno_cash,
        "rent_income_during_reno": costs.rent_income,
        "net_holding_cost": net_holding_cost,
        "cash_in": costs.cash_in,
        "total_debt_at_refi": costs.total_debt,
        "cash_recovered": recycle["cash_recovered"],
        "cash_tied_up": recycle["cash_tied_up"],
        "recycle_ratio": recycle["recycle_ratio"],
        "valuation_lower_bound": valuation.lower_bound,
        "valuation_upper_bound": valuation.upper_bound,
    }
