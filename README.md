# AU Suburb Medians + Decision Engine

Decision-engine upgrade of the static "AU Suburb Medians" tool. Adds scoring, scenarios, cashflow, stress tests, borrowing impact, compare view, and ingestable normalized data model.

## What's here
- Static frontend builder: `build_site.py` (reads enriched CSV → `site/data.json` + `site/index.html`).
- Decision engine core: `decision_engine/` (models, storage, scoring, calculations, caching, source adapters).
- Ingestion runner: `ingest.py` with a stub adapter to demonstrate idempotent ingest.
- Tests: `tests/test_calculation_engine.py` for core math sanity.
- Docs: `data_sources.md`, `assumptions.md`.

## Quickstart
Requirements: Python 3.10+, no external deps for stub/demo. For real scraping adapters you will need Playwright and browser deps (not required for the stub flow below).

### 1) Ingest stub data into SQLite
```
python ingest.py --source stub --db decision_engine.sqlite --snapshot-dir snapshots
```
- Writes raw snapshot into `snapshots/` and normalized metrics into `decision_engine.sqlite`.

### 2) Build the static site
Use any enriched CSV (existing `suburbs_enriched.csv` works; stub ingest is independent).
```
python build_site.py --csv suburbs_enriched.csv --outdir site
```
Outputs: `site/data.json` and `site/index.html`.

### 3) Serve locally
From repo root:
```
python -m http.server 8000
```
Then open http://localhost:8000/site/

## Running tests
```
python -m pytest tests
```
(Install pytest if not present.)

## Key behaviors
- Scores: Liquidity, Tenant Risk, Growth, Concentration Risk, Downside Survival, Borrowing Impact → BuyScore (weights editable in UI).
- Scenarios: defensive / balanced / aggressive with rate, vacancy, growth presets; stress test toggles embedded.
- Filters: BuyScore, risk cap, vacancy ceiling, sales/month floor, CBD distance, yield, property type/bedrooms.
- Compare: top 3 suburbs side-by-side with mini radar-like canvas.
- Exports: CSV and lightweight “Investment Brief” PDF print.
- Profiles: localStorage save/load; free tier keeps 1 profile (latest retained).

## Data + adapters
- Normalized tables defined in `decision_engine/models.py`; SQLite helpers in `decision_engine/storage.py`.
- Source adapter contract in `decision_engine/sources/base.py`; stub example in `decision_engine/sources/stub.py`.
- `data_sources.md` lists intended sources/licensing notes; real adapters should write raw snapshots to `snapshots/` and upsert metrics via the adapter interface.

## Notes
- Frontend is static; all calculations happen client-side from `data.json` enriched with precomputed scores.
- Feature flags (scoring/borrowing) and scenario selection live in the UI; stress cashflow uses embedded stress toggles.
- This is not financial advice; metrics may be incomplete—confidence degrades with missing data (highlighted in the “Why this score” modal).
