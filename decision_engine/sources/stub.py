from __future__ import annotations

import datetime as dt
import hashlib
import itertools
from typing import List, Optional, Tuple

from .base import SourceAdapter
from ..models import DataQuality, MetricRecord, Suburb


STUB_ROWS = [
    {
        "name": "Mentone",
        "state": "VIC",
        "postcode": "3194",
        "as_of": "2025-12",
        "median_house_3": 1250000,
        "median_unit_2": 620000,
        "rent_house_3_pw": 720,
        "rent_unit_2_pw": 520,
        "vacancy_rate_pct": 2.1,
        "dom": 28,
        "vendor_discount_pct": 3.2,
        "sales_per_month": 42,
        "stock_on_market_pct": 0.8,
        "income_to_rent_ratio": 32.0,
        "population_growth_pct": 1.9,
        "distance_cbd_km": 21,
    },
    {
        "name": "Ballarat Central",
        "state": "VIC",
        "postcode": "3350",
        "as_of": "2025-12",
        "median_house_3": 720000,
        "median_unit_2": 460000,
        "rent_house_3_pw": 520,
        "rent_unit_2_pw": 430,
        "vacancy_rate_pct": 1.4,
        "dom": 35,
        "vendor_discount_pct": 4.5,
        "sales_per_month": 33,
        "stock_on_market_pct": 1.1,
        "income_to_rent_ratio": 28.0,
        "population_growth_pct": 1.4,
        "distance_cbd_km": 120,
    },
]


class StubAdapter(SourceAdapter):
    source_name = "stub_decision_engine"

    def fetch_raw(self, state: Optional[str], since: Optional[str]) -> Tuple[List[dict], List[str]]:
        rows = [r for r in STUB_ROWS if (not state or r["state"] == state)]
        logs = [f"state={state or 'ALL'} since={since or 'NA'} rows={len(rows)}"]
        return rows, logs

    def transform(self, raw_rows: List[dict]):
        suburbs: List[Suburb] = []
        metrics: List[MetricRecord] = []
        dq_rows: List[DataQuality] = []

        for r in raw_rows:
            suburb_id = self._suburb_id(r["state"], r["postcode"], r["name"])
            suburbs.append(
                Suburb(
                    suburb_id=suburb_id,
                    name=r["name"],
                    state=r["state"],
                    postcode=r["postcode"],
                )
            )
            period = r.get("as_of") or dt.datetime.utcnow().strftime("%Y-%m")
            metrics.extend(
                [
                    MetricRecord(
                        suburb_id=suburb_id,
                        category="price_median",
                        metric="median_price",
                        property_type="house",
                        bedrooms=3,
                        period=period,
                        value=r.get("median_house_3"),
                        unit="AUD",
                        source=self.source_name,
                        last_updated=dt.datetime.utcnow().isoformat(),
                        confidence=0.65,
                    ),
                    MetricRecord(
                        suburb_id=suburb_id,
                        category="price_median",
                        metric="median_price",
                        property_type="unit",
                        bedrooms=2,
                        period=period,
                        value=r.get("median_unit_2"),
                        unit="AUD",
                        source=self.source_name,
                        last_updated=dt.datetime.utcnow().isoformat(),
                        confidence=0.62,
                    ),
                    MetricRecord(
                        suburb_id=suburb_id,
                        category="rent_median",
                        metric="rent_pw",
                        property_type="house",
                        bedrooms=3,
                        period=period,
                        value=r.get("rent_house_3_pw"),
                        unit="AUD_PW",
                        source=self.source_name,
                        last_updated=dt.datetime.utcnow().isoformat(),
                        confidence=0.60,
                    ),
                    MetricRecord(
                        suburb_id=suburb_id,
                        category="rent_median",
                        metric="rent_pw",
                        property_type="unit",
                        bedrooms=2,
                        period=period,
                        value=r.get("rent_unit_2_pw"),
                        unit="AUD_PW",
                        source=self.source_name,
                        last_updated=dt.datetime.utcnow().isoformat(),
                        confidence=0.58,
                    ),
                    MetricRecord(
                        suburb_id=suburb_id,
                        category="liquidity",
                        metric="days_on_market",
                        period=period,
                        value=r.get("dom"),
                        unit="days",
                        source=self.source_name,
                        last_updated=dt.datetime.utcnow().isoformat(),
                        confidence=0.55,
                    ),
                    MetricRecord(
                        suburb_id=suburb_id,
                        category="liquidity",
                        metric="vendor_discount_pct",
                        period=period,
                        value=r.get("vendor_discount_pct"),
                        unit="pct",
                        source=self.source_name,
                        last_updated=dt.datetime.utcnow().isoformat(),
                        confidence=0.55,
                    ),
                    MetricRecord(
                        suburb_id=suburb_id,
                        category="liquidity",
                        metric="sales_per_month",
                        period=period,
                        value=r.get("sales_per_month"),
                        unit="count",
                        source=self.source_name,
                        last_updated=dt.datetime.utcnow().isoformat(),
                        confidence=0.55,
                    ),
                    MetricRecord(
                        suburb_id=suburb_id,
                        category="liquidity",
                        metric="stock_on_market_pct",
                        period=period,
                        value=r.get("stock_on_market_pct"),
                        unit="pct",
                        source=self.source_name,
                        last_updated=dt.datetime.utcnow().isoformat(),
                        confidence=0.55,
                    ),
                    MetricRecord(
                        suburb_id=suburb_id,
                        category="tenant_risk",
                        metric="vacancy_rate_pct",
                        period=period,
                        value=r.get("vacancy_rate_pct"),
                        unit="pct",
                        source=self.source_name,
                        last_updated=dt.datetime.utcnow().isoformat(),
                        confidence=0.55,
                    ),
                    MetricRecord(
                        suburb_id=suburb_id,
                        category="tenant_risk",
                        metric="income_to_rent_ratio",
                        period=period,
                        value=r.get("income_to_rent_ratio"),
                        unit="pct",
                        source=self.source_name,
                        last_updated=dt.datetime.utcnow().isoformat(),
                        confidence=0.52,
                    ),
                    MetricRecord(
                        suburb_id=suburb_id,
                        category="growth_driver",
                        metric="population_growth_pct",
                        period=period,
                        value=r.get("population_growth_pct"),
                        unit="pct",
                        source=self.source_name,
                        last_updated=dt.datetime.utcnow().isoformat(),
                        confidence=0.50,
                    ),
                    MetricRecord(
                        suburb_id=suburb_id,
                        category="growth_driver",
                        metric="distance_to_cbd_km",
                        period=period,
                        value=r.get("distance_cbd_km"),
                        unit="km",
                        source=self.source_name,
                        last_updated=dt.datetime.utcnow().isoformat(),
                        confidence=0.50,
                    ),
                ]
            )

            dq_rows.extend(
                [
                    DataQuality(
                        suburb_id=suburb_id,
                        category="price_median",
                        metric="median_price",
                        missing_pct=0.0 if r.get("median_house_3") else 1.0,
                        last_updated=period,
                        source_confidence=0.6,
                    ),
                    DataQuality(
                        suburb_id=suburb_id,
                        category="rent_median",
                        metric="rent_pw",
                        missing_pct=0.0 if r.get("rent_house_3_pw") else 1.0,
                        last_updated=period,
                        source_confidence=0.6,
                    ),
                ]
            )

        return suburbs, metrics, dq_rows

    @staticmethod
    def _suburb_id(state: str, postcode: str, name: str) -> str:
        key = f"{state}-{postcode}-{name.lower().strip()}"
        return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
