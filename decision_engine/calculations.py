from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Optional

from .assumptions import DEFAULT_COST_ASSUMPTIONS, DEFAULT_MORTGAGE_ASSUMPTIONS, SCENARIO_PRESETS, STRESS_TOGGLES, Scenario


STATE_STAMP_DUTY_TABLE = {
    "VIC": [(0, 0.014), (250000, 0.024), (400000, 0.049), (750000, 0.055), (1000000, 0.06)],
    "NSW": [(0, 0.0125), (300000, 0.0375), (1000000, 0.0495)],
    "QLD": [(0, 0.015), (540000, 0.035), (1000000, 0.045)],
    "WA": [(0, 0.015), (360000, 0.0275), (725000, 0.0475)],
    "SA": [(0, 0.01), (120000, 0.02), (250000, 0.04), (300000, 0.045)],
    "TAS": [(0, 0.015), (130000, 0.025), (375000, 0.035), (725000, 0.045)],
    "ACT": [(0, 0.01), (200000, 0.02), (500000, 0.04)],
    "NT": [(0, 0.015), (525000, 0.0435)],
}


@dataclass
class CashflowResult:
    annual_cashflow: float
    weekly_cashflow: float
    rent_shaded: float
    loan_amount: float
    annual_repayment: float
    holding_costs: float


@dataclass
class DownsideResult:
    annual_cashflow: float
    worst_month_burn: float
    forced_sale_risk: bool


@dataclass
class BorrowingImpact:
    capacity_remaining: float
    next_purchase_eta_months: float


def monthly_payment(principal: float, annual_rate: float, term_years: int, repayment_type: str) -> float:
    r = annual_rate / 12
    n = term_years * 12
    if principal <= 0:
        return 0.0
    if repayment_type.lower() == "io":
        return principal * r
    if r <= 0:
        return principal / max(n, 1)
    pow_val = (1 + r) ** n
    return principal * (r * pow_val) / (pow_val - 1)


def stamp_duty(amount: float, state: str) -> float:
    state = state.upper()
    brackets = STATE_STAMP_DUTY_TABLE.get(state, [(0, 0.05)])
    duty = 0.0
    for threshold, rate in brackets:
        if amount >= threshold:
            duty = amount * rate
    return duty


def calc_entry_cost(price: float, state: str, assumptions: Dict[str, float]) -> float:
    buyers_agent = price * assumptions.get("buyers_agent_pct", 0.0)
    initial_repairs = price * assumptions.get("initial_repairs_pct", 0.0)
    return (
        stamp_duty(price, state)
        + assumptions.get("conveyancing", 0.0)
        + assumptions.get("building_pest", 0.0)
        + buyers_agent
        + initial_repairs
    )


def calc_exit_cost(price_future: float, assumptions: Dict[str, float]) -> float:
    return (
        price_future * assumptions.get("exit_agent_pct", 0.0)
        + assumptions.get("exit_legals", 0.0)
        + assumptions.get("exit_marketing", 0.0)
        + price_future * assumptions.get("exit_other_pct", 0.0)
    )


def calc_holding_costs(price: float, rent_pw: float, scenario: Scenario, assumptions: Dict[str, float]) -> float:
    shaded_rent = rent_pw * assumptions.get("rent_shading_pct", 0.7)
    pm = shaded_rent * 52 * assumptions.get("pm_pct", 0.06)
    maintenance = shaded_rent * 52 * max(scenario.maintenance_pct, assumptions.get("maintenance_pct", 0.07))
    vacancy = rent_pw * (scenario.vacancy_weeks + assumptions.get("vacancy_weeks", 2.0))
    insurance = assumptions.get("insurance_pa", 0.0)
    council = assumptions.get("council_water_pa", 0.0)
    return pm + maintenance + vacancy + insurance + council


def calc_cashflow(price: float, rent_pw: float, state: str, scenario_name: str, cost_overrides: Optional[Dict[str, float]] = None) -> CashflowResult:
    scenario = SCENARIO_PRESETS[scenario_name]
    cost = {**DEFAULT_COST_ASSUMPTIONS, **(cost_overrides or {})}
    mortgage = DEFAULT_MORTGAGE_ASSUMPTIONS.copy()
    mortgage["interest_rate"] = scenario.interest_rate + mortgage.get("interest_buffer_pct", 0.0)

    deposit_pct = mortgage.get("deposit_pct", 0.2)
    loan_amount = price * (1 - deposit_pct)

    repayment = monthly_payment(loan_amount, mortgage["interest_rate"], int(mortgage.get("term_years", 30)), mortgage.get("repayment_type", "pi"))
    annual_repayment = repayment * 12

    shaded_rent = rent_pw * scenario.rent_shading_pct * 52
    holding = calc_holding_costs(price, rent_pw, scenario, cost)
    annual_cashflow = shaded_rent - annual_repayment - holding
    weekly_cashflow = annual_cashflow / 52

    return CashflowResult(
        annual_cashflow=annual_cashflow,
        weekly_cashflow=weekly_cashflow,
        rent_shaded=shaded_rent,
        loan_amount=loan_amount,
        annual_repayment=annual_repayment,
        holding_costs=holding,
    )


def estimate_cgt(purchase_price: float, sell_price: float, holding_years: int, tax_rate: float) -> float:
    gain = max(0.0, sell_price - purchase_price)
    if holding_years >= 1:
        gain *= 0.5  # 50% discount
    return gain * tax_rate


def simulate_downside(price: float, rent_pw: float, state: str, scenario_name: str, cash_burn_threshold: float) -> DownsideResult:
    base_scenario = SCENARIO_PRESETS[scenario_name]
    stress_rate = max(base_scenario.interest_rate + STRESS_TOGGLES["rate_bps_uplift"], STRESS_TOGGLES["rate_pct"])
    stress_rent = rent_pw * (1 + STRESS_TOGGLES["rent_drawdown_pct"])

    scenario = Scenario(
        name="stress",
        interest_rate=stress_rate,
        rent_growth=base_scenario.rent_growth,
        price_growth=STRESS_TOGGLES["price_drop_pct"],
        vacancy_weeks=base_scenario.vacancy_weeks + STRESS_TOGGLES["vacancy_weeks_extra"],
        maintenance_pct=base_scenario.maintenance_pct + 0.01,
        rent_shading_pct=base_scenario.rent_shading_pct - 0.05,
        buffer_pct=base_scenario.buffer_pct,
    )

    cf = calc_cashflow(price, stress_rent, state, scenario_name="balanced", cost_overrides={"vacancy_weeks": scenario.vacancy_weeks})
    worst_month = (cf.annual_cashflow / 12) - (cf.loan_amount * scenario.buffer_pct / 12)
    forced_sale = worst_month < -abs(cash_burn_threshold)
    return DownsideResult(annual_cashflow=cf.annual_cashflow, worst_month_burn=worst_month, forced_sale_risk=forced_sale)


def borrowing_power(income_pa: float, other_debt_monthly: float, mortgage_assumptions: Optional[Dict[str, float]] = None) -> float:
    mortgage = {**DEFAULT_MORTGAGE_ASSUMPTIONS, **(mortgage_assumptions or {})}
    buffer_rate = mortgage["interest_rate"] + mortgage.get("lender_buffer_pct", 0.03)
    servicing_ratio = 0.35  # conservative portion of income for debt service
    allowable_monthly = (income_pa / 12) * servicing_ratio - other_debt_monthly
    if allowable_monthly <= 0:
        return 0.0
    # approximate inverse amortization using IO at buffer rate as conservative
    return allowable_monthly / (buffer_rate / 12)


def borrowing_impact(income_pa: float, other_debt_monthly: float, savings_rate: float, equity_available: float, target_deposit: float, assumptions: Optional[Dict[str, float]] = None) -> BorrowingImpact:
    capacity = borrowing_power(income_pa, other_debt_monthly, mortgage_assumptions=assumptions)
    if capacity <= 0:
        return BorrowingImpact(capacity_remaining=0.0, next_purchase_eta_months=999.0)
    monthly_saving = (income_pa / 12) * savings_rate
    if monthly_saving <= 0:
        eta = 999.0
    else:
        shortfall = max(0.0, target_deposit - equity_available)
        eta = shortfall / monthly_saving
    return BorrowingImpact(capacity_remaining=capacity, next_purchase_eta_months=eta)
