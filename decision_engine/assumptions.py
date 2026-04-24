from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass
class Scenario:
    name: str
    interest_rate: float
    rent_growth: float
    price_growth: float
    vacancy_weeks: float
    maintenance_pct: float
    rent_shading_pct: float
    buffer_pct: float


DEFAULT_COST_ASSUMPTIONS: Dict[str, float] = {
    "conveyancing": 1800.0,
    "building_pest": 650.0,
    "buyers_agent_pct": 0.0,
    "initial_repairs_pct": 0.01,  # of price
    "pm_pct": 0.06,  # of rent
    "insurance_pa": 1200.0,
    "council_water_pa": 2600.0,
    "maintenance_pct": 0.07,  # of rent
    "vacancy_weeks": 2.0,
    "exit_agent_pct": 0.018,
    "exit_legals": 1800.0,
    "exit_marketing": 2200.0,
    "exit_other_pct": 0.01,
}

DEFAULT_MORTGAGE_ASSUMPTIONS: Dict[str, float] = {
    "deposit_pct": 0.20,
    "lvr": 0.80,
    "interest_rate": 0.065,
    "term_years": 30,
    "repayment_type": "pi",
    "lender_buffer_pct": 0.03,
    "tax_rate": 0.37,
    "rent_shading_pct": 0.70,
    "interest_buffer_pct": 0.02,
}

SCENARIO_PRESETS: Dict[str, Scenario] = {
    "defensive": Scenario(
        name="defensive",
        interest_rate=0.075,
        rent_growth=0.00,
        price_growth=0.00,
        vacancy_weeks=4.0,
        maintenance_pct=0.08,
        rent_shading_pct=0.65,
        buffer_pct=0.02,
    ),
    "balanced": Scenario(
        name="balanced",
        interest_rate=0.065,
        rent_growth=0.02,
        price_growth=0.02,
        vacancy_weeks=2.0,
        maintenance_pct=0.07,
        rent_shading_pct=0.70,
        buffer_pct=0.015,
    ),
    "aggressive": Scenario(
        name="aggressive",
        interest_rate=0.06,
        rent_growth=0.035,
        price_growth=0.035,
        vacancy_weeks=1.0,
        maintenance_pct=0.065,
        rent_shading_pct=0.75,
        buffer_pct=0.01,
    ),
}

STRESS_TOGGLES = {
    "rate_pct": 0.085,
    "rate_bps_uplift": 0.02,
    "rent_drawdown_pct": -0.10,
    "vacancy_weeks_extra": 2.0,
    "price_drop_pct": -0.10,
    "price_drop_years": 2,
}
