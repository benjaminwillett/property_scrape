from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Literal, Optional

# Core reference dimensions
PropertyType = Literal["house", "unit", "townhouse"]
MetricCategory = Literal[
    "price_median",
    "rent_median",
    "liquidity",
    "tenant_risk",
    "economic_dependency",
    "growth_driver",
    "holding_cost",
    "mortgage_assumption",
    "data_quality",
]


@dataclass
class Suburb:
    suburb_id: str  # stable hash/key, e.g. f"{state}-{postcode}-{slug}"
    name: str
    state: str
    postcode: str
    sa2_code: Optional[str] = None
    gccsa_name: Optional[str] = None
    abs_code: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None


@dataclass
class MetricRecord:
    suburb_id: str
    category: MetricCategory
    metric: str  # e.g. "median_price", "vacancy_rate_pct"
    property_type: Optional[PropertyType] = None
    bedrooms: Optional[int] = None
    period: Optional[str] = None  # ISO month (YYYY-MM) or YYYY-Qn
    value: Optional[float] = None
    unit: Optional[str] = None
    source: Optional[str] = None
    last_updated: Optional[str] = None  # ISO datetime
    confidence: Optional[float] = None  # 0-1
    notes: Optional[str] = None


@dataclass
class BedroomMedian:
    suburb_id: str
    property_type: PropertyType
    bedrooms: int
    period: str  # YYYY or YYYY-MM
    sale_median: Optional[float]
    rent_median: Optional[float]
    sale_sample: Optional[int] = None
    rent_sample: Optional[int] = None
    source: Optional[str] = None
    last_updated: Optional[str] = None


@dataclass
class SalesComp:
    suburb_id: str
    property_type: PropertyType
    bedrooms: int
    sold_date: str  # ISO date
    sale_price: float
    source: Optional[str] = None
    confidence: Optional[float] = None
    address: Optional[str] = None


@dataclass
class LiquidityMetric:
    suburb_id: str
    property_type: PropertyType
    bedrooms: int
    period: str
    sales_per_month: Optional[float]
    days_on_market: Optional[float]
    stock_on_market_pct: Optional[float]
    vendor_discount_pct: Optional[float]


@dataclass
class PremiumHistory:
    suburb_id: str
    property_type: PropertyType
    period: str
    premium_3_to_4_pct: Optional[float]
    sample_size: Optional[int] = None


@dataclass
class RefiConfidence:
    suburb_id: str
    property_type: PropertyType
    bedrooms: int
    period: str
    comp_count: Optional[int]
    sale_dispersion_pct: Optional[float]


@dataclass
class DealAssumption:
    scenario: str
    assumptions_json: str
    created_at: str


@dataclass
class DealResult:
    scenario: str
    suburb_id: str
    property_type: PropertyType
    bedrooms: int
    period: str
    premium_3_to_4_pct: Optional[float]
    premium_stability: Optional[float]
    manufactured_equity: Optional[float]
    usable_equity_80: Optional[float]
    refi_confidence_score: Optional[float]
    equity_score: Optional[float]
    deal_score: Optional[float]
    inputs_json: Optional[str]


@dataclass
class DataQuality:
    suburb_id: str
    category: MetricCategory
    metric: str
    missing_pct: float
    last_updated: Optional[str]
    source_confidence: float


@dataclass
class IngestionRun:
    run_id: str
    source: str
    state: Optional[str]
    since: Optional[str]
    started_at: str
    finished_at: Optional[str]
    row_count: int
    status: str  # success | partial | failed
    message: Optional[str]


def default_tables() -> Dict[str, str]:
    """Return SQL DDL for normalized tables.

    Using SQLite for simplicity; caller can run each statement.
    """

    return {
        "suburbs": """
            CREATE TABLE IF NOT EXISTS suburbs (
                suburb_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                state TEXT NOT NULL,
                postcode TEXT NOT NULL,
                sa2_code TEXT,
                gccsa_name TEXT,
                abs_code TEXT,
                lat REAL,
                lon REAL
            );
        """,
        "metrics": """
            CREATE TABLE IF NOT EXISTS metrics (
                suburb_id TEXT NOT NULL,
                category TEXT NOT NULL,
                metric TEXT NOT NULL,
                property_type TEXT,
                bedrooms INTEGER,
                period TEXT,
                value REAL,
                unit TEXT,
                source TEXT,
                last_updated TEXT,
                confidence REAL,
                notes TEXT,
                PRIMARY KEY (suburb_id, category, metric, property_type, bedrooms, period)
            );
        """,
        "data_quality": """
            CREATE TABLE IF NOT EXISTS data_quality (
                suburb_id TEXT NOT NULL,
                category TEXT NOT NULL,
                metric TEXT NOT NULL,
                missing_pct REAL NOT NULL,
                last_updated TEXT,
                source_confidence REAL,
                PRIMARY KEY (suburb_id, category, metric)
            );
        """,
        "ingestion_runs": """
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                run_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                state TEXT,
                since TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                row_count INTEGER NOT NULL,
                status TEXT NOT NULL,
                message TEXT
            );
        """,
        "cache": """
            CREATE TABLE IF NOT EXISTS cache (
                cache_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                scenario_hash TEXT,
                suburb_id TEXT,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
        """,
        "feature_flags": """
            CREATE TABLE IF NOT EXISTS feature_flags (
                name TEXT PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL
            );
        """,
        "bedroom_medians": """
            CREATE TABLE IF NOT EXISTS bedroom_medians (
                suburb_id TEXT NOT NULL,
                property_type TEXT NOT NULL,
                bedrooms INTEGER NOT NULL,
                period TEXT NOT NULL,
                sale_median REAL,
                rent_median REAL,
                sale_sample INTEGER,
                rent_sample INTEGER,
                source TEXT,
                last_updated TEXT,
                PRIMARY KEY (suburb_id, property_type, bedrooms, period)
            );
        """,
        "sales_comps": """
            CREATE TABLE IF NOT EXISTS sales_comps (
                suburb_id TEXT NOT NULL,
                property_type TEXT NOT NULL,
                bedrooms INTEGER NOT NULL,
                sold_date TEXT NOT NULL,
                sale_price REAL NOT NULL,
                source TEXT,
                confidence REAL,
                address TEXT,
                PRIMARY KEY (suburb_id, property_type, bedrooms, sold_date, sale_price)
            );
        """,
        "liquidity_metrics": """
            CREATE TABLE IF NOT EXISTS liquidity_metrics (
                suburb_id TEXT NOT NULL,
                property_type TEXT NOT NULL,
                bedrooms INTEGER NOT NULL,
                period TEXT NOT NULL,
                sales_per_month REAL,
                days_on_market REAL,
                stock_on_market_pct REAL,
                vendor_discount_pct REAL,
                PRIMARY KEY (suburb_id, property_type, bedrooms, period)
            );
        """,
        "premium_history": """
            CREATE TABLE IF NOT EXISTS premium_history (
                suburb_id TEXT NOT NULL,
                property_type TEXT NOT NULL,
                period TEXT NOT NULL,
                premium_3_to_4_pct REAL,
                sample_size INTEGER,
                PRIMARY KEY (suburb_id, property_type, period)
            );
        """,
        "refi_confidence": """
            CREATE TABLE IF NOT EXISTS refi_confidence (
                suburb_id TEXT NOT NULL,
                property_type TEXT NOT NULL,
                bedrooms INTEGER NOT NULL,
                period TEXT NOT NULL,
                comp_count INTEGER,
                sale_dispersion_pct REAL,
                PRIMARY KEY (suburb_id, property_type, bedrooms, period)
            );
        """,
        "deal_assumptions": """
            CREATE TABLE IF NOT EXISTS deal_assumptions (
                scenario TEXT PRIMARY KEY,
                assumptions_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
        """,
        "deal_results": """
            CREATE TABLE IF NOT EXISTS deal_results (
                scenario TEXT NOT NULL,
                suburb_id TEXT NOT NULL,
                property_type TEXT NOT NULL,
                bedrooms INTEGER NOT NULL,
                period TEXT NOT NULL,
                premium_3_to_4_pct REAL,
                premium_stability REAL,
                manufactured_equity REAL,
                usable_equity_80 REAL,
                refi_confidence_score REAL,
                equity_score REAL,
                deal_score REAL,
                inputs_json TEXT,
                PRIMARY KEY (scenario, suburb_id, property_type, bedrooms, period)
            );
        """,
    }
