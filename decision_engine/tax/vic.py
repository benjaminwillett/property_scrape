from __future__ import annotations

from typing import Optional, Tuple

# Simple VIC stamp duty bands for residential PPR (approximate, excludes concessions)
# Source: SRO VIC as of 2024; values used here are indicative only.
BANDS: Tuple[Tuple[float, float, float], ...] = (
    (0, 250000, 0.0114),
    (250000, 960000, 0.024),
    (960000, 2000000, 0.06),
)


def stamp_duty(amount: float) -> float:
    if amount <= 0:
        return 0.0
    duty = 0.0
    remaining = amount
    prev_cap = 0.0
    for lower, upper, rate in BANDS:
        if amount <= lower:
            break
        band_top = min(amount, upper)
        band_base = max(prev_cap, lower)
        duty += (band_top - band_base) * rate
        prev_cap = upper
    return duty


def stamp_duty_with_fees(amount: float, titles_office_fee: Optional[float] = 1400.0, transfer_fee: Optional[float] = 1500.0) -> float:
    duty = stamp_duty(amount)
    duty += titles_office_fee or 0.0
    duty += transfer_fee or 0.0
    return duty
