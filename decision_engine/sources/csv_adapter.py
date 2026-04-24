from __future__ import annotations

import datetime as dt
import statistics
from typing import List, Optional, Tuple

from ..models import (
    BedroomMedian,
    IngestionRun,
    LiquidityMetric,
    PremiumHistory,
    RefiConfidence,
    SalesComp,
    Suburb,
)
from ..storage import (
    bulk_upsert_bedroom_medians,
    bulk_upsert_liquidity,
    bulk_upsert_premium_history,
    bulk_upsert_refi_confidence,
    bulk_upsert_sales_comps,
    record_run,
    upsert_suburb,
)
from .base import SourceAdapter


class CSVBedroomsAdapter(SourceAdapter):
    """CSV ingestion for bedroom ladder medians and comps.

    Expected columns: suburb, state, postcode, sold_date (YYYY-MM-DD), price, bedrooms, property_type.
    Additional columns are ignored. Rent medians are not derived from sales comps and will be null unless provided.
    """

    source_name = "csv_bedrooms"

    def __init__(self, csv_path: str):
        self.csv_path = csv_path

    def fetch_raw(self, state: Optional[str], since: Optional[str]) -> Tuple[List[dict], List[str]]:
        import csv

        rows: List[dict] = []
        logs: List[str] = []
        with open(self.csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if state and row.get("state") and row["state"].upper() != state.upper():
                    continue
                rows.append(row)
        logs.append(f"loaded {len(rows)} rows from {self.csv_path}")
        return rows, logs

    def _suburb_id(self, row: dict) -> str:
        parts = [row.get("state", "").strip().upper(), row.get("postcode", "").strip(), row.get("suburb", "").strip().lower().replace(" ", "-")]
        return "-".join(filter(None, parts))

    def _period(self, sold_date: str) -> str:
        try:
            d = dt.datetime.strptime(sold_date, "%Y-%m-%d").date()
            return d.strftime("%Y-%m")
        except Exception:
            return "unknown"

    def transform(self, raw_rows: List[dict]):
        suburbs: List[Suburb] = []
        medians: List[BedroomMedian] = []
        comps: List[SalesComp] = []
        liquidity: List[LiquidityMetric] = []
        premiums: List[PremiumHistory] = []
        refi_rows: List[RefiConfidence] = []

        # group prices by key for medians
        price_groups = {}
        latest_period_by_key = {}

        for r in raw_rows:
            suburb_id = self._suburb_id(r)
            suburbs.append(
                Suburb(
                    suburb_id=suburb_id,
                    name=r.get("suburb", "").title(),
                    state=r.get("state", "").upper(),
                    postcode=r.get("postcode", ""),
                )
            )
            try:
                price = float(str(r.get("price", "0")).replace(",", ""))
            except ValueError:
                continue
            bedrooms = int(r.get("bedrooms") or 0)
            ptype = (r.get("property_type") or "house").lower()
            sold_date = r.get("sold_date") or ""
            period = self._period(sold_date)
            key = (suburb_id, ptype, bedrooms, period)
            price_groups.setdefault(key, []).append(price)
            latest_period_by_key[(suburb_id, ptype, bedrooms)] = period
            comps.append(
                SalesComp(
                    suburb_id=suburb_id,
                    property_type=ptype,
                    bedrooms=bedrooms,
                    sold_date=sold_date or period,
                    sale_price=price,
                    source=self.source_name,
                    confidence=0.6,
                    address=r.get("address"),
                )
            )

        # build medians + liquidity
        for key, prices in price_groups.items():
            suburb_id, ptype, bedrooms, period = key
            prices_sorted = sorted(prices)
            median_price = statistics.median(prices_sorted)
            medians.append(
                BedroomMedian(
                    suburb_id=suburb_id,
                    property_type=ptype,
                    bedrooms=bedrooms,
                    period=period,
                    sale_median=median_price,
                    rent_median=None,
                    sale_sample=len(prices_sorted),
                    rent_sample=None,
                    source=self.source_name,
                    last_updated=dt.datetime.utcnow().isoformat(),
                )
            )
            liquidity.append(
                LiquidityMetric(
                    suburb_id=suburb_id,
                    property_type=ptype,
                    bedrooms=bedrooms,
                    period=period,
                    sales_per_month=float(len(prices_sorted)),
                    days_on_market=None,
                    stock_on_market_pct=None,
                    vendor_discount_pct=None,
                )
            )

        # premium history where 3 and 4 are available for same period
        periods_by_suburb = {}
        for bm in medians:
            periods_by_suburb.setdefault((bm.suburb_id, bm.property_type, bm.period), {})[bm.bedrooms] = bm.sale_median
        for (suburb_id, ptype, period), vals in periods_by_suburb.items():
            price3 = vals.get(3)
            price4 = vals.get(4)
            if price3 is None or price4 is None:
                continue
            premium = ((price4 - price3) / price3) * 100.0 if price3 else None
            premiums.append(
                PremiumHistory(
                    suburb_id=suburb_id,
                    property_type=ptype,
                    period=period,
                    premium_3_to_4_pct=premium,
                    sample_size=None,
                )
            )

        # refi confidence: trailing 6 months per suburb/type/bed
        trailing_by_key = {}
        now = dt.datetime.utcnow().date()
        cutoff = now - dt.timedelta(days=180)
        for comp in comps:
            try:
                d = dt.datetime.strptime(comp.sold_date[:10], "%Y-%m-%d").date()
            except Exception:
                d = now
            if d < cutoff:
                continue
            key = (comp.suburb_id, comp.property_type, comp.bedrooms)
            trailing_by_key.setdefault(key, []).append(comp.sale_price)
        for key, prices in trailing_by_key.items():
            suburb_id, ptype, beds = key
            mean = sum(prices) / len(prices)
            dispersion = None
            if mean:
                variance = sum((p - mean) ** 2 for p in prices) / len(prices)
                dispersion = (variance ** 0.5) / mean * 100.0
            refi_rows.append(
                RefiConfidence(
                    suburb_id=suburb_id,
                    property_type=ptype,
                    bedrooms=beds,
                    period=now.strftime("%Y-%m"),
                    comp_count=len(prices),
                    sale_dispersion_pct=dispersion,
                )
            )

        return suburbs, medians, comps, liquidity, premiums, refi_rows

    def ingest(self, conn, state: Optional[str], since: Optional[str], snapshot_dir: str, log_fn=print):
        import uuid

        run_id = str(uuid.uuid4())
        started = dt.datetime.utcnow().isoformat()
        raw_rows, debug_logs = self.fetch_raw(state=state, since=since)
        for ln in debug_logs:
            log_fn(f"[INGEST:{self.source_name}] {ln}")

        snapshot_path = self.persist_snapshot(snapshot_dir, raw_rows)
        log_fn(f"[INGEST:{self.source_name}] snapshot saved to {snapshot_path}")

        suburbs, medians, comps, liquidity, premiums, refi_rows = self.transform(raw_rows)
        try:
            for sub in suburbs:
                upsert_suburb(conn, sub)
            bulk_upsert_bedroom_medians(conn, medians)
            bulk_upsert_sales_comps(conn, comps)
            bulk_upsert_liquidity(conn, liquidity)
            bulk_upsert_premium_history(conn, premiums)
            bulk_upsert_refi_confidence(conn, refi_rows)
            status = "success"
            message = f"upserted {len(suburbs)} suburbs, {len(medians)} medians, {len(comps)} comps"
        except Exception as exc:  # pragma: no cover
            status = "failed"
            message = f"failed: {exc}"
            log_fn(f"[INGEST:{self.source_name}] ERROR {exc}")
        finished = dt.datetime.utcnow().isoformat()
        run = IngestionRun(
            run_id=run_id,
            source=self.source_name,
            state=state,
            since=since,
            started_at=started,
            finished_at=finished,
            row_count=len(medians),
            status=status,
            message=message,
        )
        record_run(conn, run)
        return run
