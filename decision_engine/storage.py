from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from typing import Iterable, Optional

from .models import (
    BedroomMedian,
    DataQuality,
    DealAssumption,
    DealResult,
    IngestionRun,
    LiquidityMetric,
    MetricRecord,
    PremiumHistory,
    RefiConfidence,
    SalesComp,
    Suburb,
    default_tables,
)


@contextmanager
def connect(db_path: str):
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def init_db(db_path: str):
    with connect(db_path) as conn:
        cur = conn.cursor()
        for ddl in default_tables().values():
            cur.execute(ddl)
        conn.commit()


def upsert_suburb(conn: sqlite3.Connection, suburb: Suburb):
    conn.execute(
        """
        INSERT INTO suburbs (suburb_id, name, state, postcode, sa2_code, gccsa_name, abs_code, lat, lon)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(suburb_id) DO UPDATE SET
            name=excluded.name,
            state=excluded.state,
            postcode=excluded.postcode,
            sa2_code=excluded.sa2_code,
            gccsa_name=excluded.gccsa_name,
            abs_code=excluded.abs_code,
            lat=excluded.lat,
            lon=excluded.lon
        """,
        (
            suburb.suburb_id,
            suburb.name,
            suburb.state,
            suburb.postcode,
            suburb.sa2_code,
            suburb.gccsa_name,
            suburb.abs_code,
            suburb.lat,
            suburb.lon,
        ),
    )


def upsert_metric(conn: sqlite3.Connection, rec: MetricRecord):
    conn.execute(
        """
        INSERT INTO metrics (suburb_id, category, metric, property_type, bedrooms, period, value, unit, source, last_updated, confidence, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(suburb_id, category, metric, property_type, bedrooms, period) DO UPDATE SET
            value=excluded.value,
            unit=excluded.unit,
            source=excluded.source,
            last_updated=excluded.last_updated,
            confidence=excluded.confidence,
            notes=excluded.notes
        """,
        (
            rec.suburb_id,
            rec.category,
            rec.metric,
            rec.property_type,
            rec.bedrooms,
            rec.period,
            rec.value,
            rec.unit,
            rec.source,
            rec.last_updated,
            rec.confidence,
            rec.notes,
        ),
    )


def bulk_upsert_metrics(conn: sqlite3.Connection, records: Iterable[MetricRecord]):
    for rec in records:
        upsert_metric(conn, rec)


def upsert_quality(conn: sqlite3.Connection, dq: DataQuality):
    conn.execute(
        """
        INSERT INTO data_quality (suburb_id, category, metric, missing_pct, last_updated, source_confidence)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(suburb_id, category, metric) DO UPDATE SET
            missing_pct=excluded.missing_pct,
            last_updated=excluded.last_updated,
            source_confidence=excluded.source_confidence
        """,
        (
            dq.suburb_id,
            dq.category,
            dq.metric,
            dq.missing_pct,
            dq.last_updated,
            dq.source_confidence,
        ),
    )


def upsert_bedroom_median(conn: sqlite3.Connection, rec: BedroomMedian):
    conn.execute(
        """
        INSERT INTO bedroom_medians (suburb_id, property_type, bedrooms, period, sale_median, rent_median, sale_sample, rent_sample, source, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(suburb_id, property_type, bedrooms, period) DO UPDATE SET
            sale_median=excluded.sale_median,
            rent_median=excluded.rent_median,
            sale_sample=excluded.sale_sample,
            rent_sample=excluded.rent_sample,
            source=excluded.source,
            last_updated=excluded.last_updated
        """,
        (
            rec.suburb_id,
            rec.property_type,
            rec.bedrooms,
            rec.period,
            rec.sale_median,
            rec.rent_median,
            rec.sale_sample,
            rec.rent_sample,
            rec.source,
            rec.last_updated,
        ),
    )


def bulk_upsert_bedroom_medians(conn: sqlite3.Connection, records: Iterable[BedroomMedian]):
    for rec in records:
        upsert_bedroom_median(conn, rec)


def upsert_sales_comp(conn: sqlite3.Connection, rec: SalesComp):
    conn.execute(
        """
        INSERT INTO sales_comps (suburb_id, property_type, bedrooms, sold_date, sale_price, source, confidence, address)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(suburb_id, property_type, bedrooms, sold_date, sale_price) DO UPDATE SET
            source=excluded.source,
            confidence=excluded.confidence,
            address=excluded.address
        """,
        (
            rec.suburb_id,
            rec.property_type,
            rec.bedrooms,
            rec.sold_date,
            rec.sale_price,
            rec.source,
            rec.confidence,
            rec.address,
        ),
    )


def bulk_upsert_sales_comps(conn: sqlite3.Connection, records: Iterable[SalesComp]):
    for rec in records:
        upsert_sales_comp(conn, rec)


def upsert_liquidity_metric(conn: sqlite3.Connection, rec: LiquidityMetric):
    conn.execute(
        """
        INSERT INTO liquidity_metrics (suburb_id, property_type, bedrooms, period, sales_per_month, days_on_market, stock_on_market_pct, vendor_discount_pct)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(suburb_id, property_type, bedrooms, period) DO UPDATE SET
            sales_per_month=excluded.sales_per_month,
            days_on_market=excluded.days_on_market,
            stock_on_market_pct=excluded.stock_on_market_pct,
            vendor_discount_pct=excluded.vendor_discount_pct
        """,
        (
            rec.suburb_id,
            rec.property_type,
            rec.bedrooms,
            rec.period,
            rec.sales_per_month,
            rec.days_on_market,
            rec.stock_on_market_pct,
            rec.vendor_discount_pct,
        ),
    )


def bulk_upsert_liquidity(conn: sqlite3.Connection, records: Iterable[LiquidityMetric]):
    for rec in records:
        upsert_liquidity_metric(conn, rec)


def upsert_premium_history(conn: sqlite3.Connection, rec: PremiumHistory):
    conn.execute(
        """
        INSERT INTO premium_history (suburb_id, property_type, period, premium_3_to_4_pct, sample_size)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(suburb_id, property_type, period) DO UPDATE SET
            premium_3_to_4_pct=excluded.premium_3_to_4_pct,
            sample_size=excluded.sample_size
        """,
        (
            rec.suburb_id,
            rec.property_type,
            rec.period,
            rec.premium_3_to_4_pct,
            rec.sample_size,
        ),
    )


def bulk_upsert_premium_history(conn: sqlite3.Connection, records: Iterable[PremiumHistory]):
    for rec in records:
        upsert_premium_history(conn, rec)


def upsert_refi_confidence(conn: sqlite3.Connection, rec: RefiConfidence):
    conn.execute(
        """
        INSERT INTO refi_confidence (suburb_id, property_type, bedrooms, period, comp_count, sale_dispersion_pct)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(suburb_id, property_type, bedrooms, period) DO UPDATE SET
            comp_count=excluded.comp_count,
            sale_dispersion_pct=excluded.sale_dispersion_pct
        """,
        (
            rec.suburb_id,
            rec.property_type,
            rec.bedrooms,
            rec.period,
            rec.comp_count,
            rec.sale_dispersion_pct,
        ),
    )


def bulk_upsert_refi_confidence(conn: sqlite3.Connection, records: Iterable[RefiConfidence]):
    for rec in records:
        upsert_refi_confidence(conn, rec)


def upsert_deal_assumption(conn: sqlite3.Connection, rec: DealAssumption):
    conn.execute(
        """
        INSERT INTO deal_assumptions (scenario, assumptions_json, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT(scenario) DO UPDATE SET
            assumptions_json=excluded.assumptions_json,
            created_at=excluded.created_at
        """,
        (rec.scenario, rec.assumptions_json, rec.created_at),
    )


def upsert_deal_result(conn: sqlite3.Connection, rec: DealResult):
    conn.execute(
        """
        INSERT INTO deal_results (scenario, suburb_id, property_type, bedrooms, period, premium_3_to_4_pct, premium_stability, manufactured_equity, usable_equity_80, refi_confidence_score, equity_score, deal_score, inputs_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario, suburb_id, property_type, bedrooms, period) DO UPDATE SET
            premium_3_to_4_pct=excluded.premium_3_to_4_pct,
            premium_stability=excluded.premium_stability,
            manufactured_equity=excluded.manufactured_equity,
            usable_equity_80=excluded.usable_equity_80,
            refi_confidence_score=excluded.refi_confidence_score,
            equity_score=excluded.equity_score,
            deal_score=excluded.deal_score,
            inputs_json=excluded.inputs_json
        """,
        (
            rec.scenario,
            rec.suburb_id,
            rec.property_type,
            rec.bedrooms,
            rec.period,
            rec.premium_3_to_4_pct,
            rec.premium_stability,
            rec.manufactured_equity,
            rec.usable_equity_80,
            rec.refi_confidence_score,
            rec.equity_score,
            rec.deal_score,
            rec.inputs_json,
        ),
    )


def record_run(conn: sqlite3.Connection, run: IngestionRun):
    conn.execute(
        """
        INSERT INTO ingestion_runs (run_id, source, state, since, started_at, finished_at, row_count, status, message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            finished_at=excluded.finished_at,
            row_count=excluded.row_count,
            status=excluded.status,
            message=excluded.message
        """,
        (
            run.run_id,
            run.source,
            run.state,
            run.since,
            run.started_at,
            run.finished_at,
            run.row_count,
            run.status,
            run.message,
        ),
    )


def _now_ts() -> int:
    return int(time.time())


def upsert_cache(conn: sqlite3.Connection, cache_key: str, payload: dict, ttl_seconds: int, suburb_id: Optional[str], scenario_hash: Optional[str]):
    now = _now_ts()
    expires = now + ttl_seconds
    conn.execute(
        """
        INSERT INTO cache (cache_key, payload, scenario_hash, suburb_id, expires_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET
            payload=excluded.payload,
            scenario_hash=excluded.scenario_hash,
            suburb_id=excluded.suburb_id,
            expires_at=excluded.expires_at,
            created_at=excluded.created_at
        """,
        (cache_key, json.dumps(payload), scenario_hash, suburb_id, str(expires), str(now)),
    )


def get_cache(conn: sqlite3.Connection, cache_key: str) -> Optional[dict]:
    now = _now_ts()
    cur = conn.execute("SELECT payload, expires_at FROM cache WHERE cache_key=?", (cache_key,))
    row = cur.fetchone()
    if not row:
        return None
    payload, expires_at = row
    try:
        if int(expires_at) < now:
            conn.execute("DELETE FROM cache WHERE cache_key=?", (cache_key,))
            return None
    except Exception:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None
