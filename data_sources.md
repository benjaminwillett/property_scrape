# Data Sources

> Draft mapping of metrics to sources. Replace stubs with production feeds when available. Respect licensing/terms before enabling.

## Price Medians
- Metric: median sale price by property type/bedrooms
- Source: REA/Domain public suburb profiles (scraped), internal historical cache
- Update: Monthly
- Licensing: Public pages; verify ToS before automated scraping

## Rent Medians
- Metric: weekly rent by property type/bedrooms
- Source: REIV (VIC), SQM Research (other states), public suburb profiles fallback
- Update: Monthly
- Licensing: Member-only for REIV; ensure compliance. SQM requires license.

## Liquidity Metrics
- Metrics: days on market, vendor discount %, sales volume/month, stock on market %
- Source: SQM Research or CoreLogic (preferred); fallback to portal trend snippets
- Update: Monthly
- Licensing: Commercial license required; placeholder only in stub adapter

## Tenant Risk Metrics
- Metrics: vacancy rate %, rental share %, income-to-rent ratio, govt housing proxy
- Source: SQM vacancy, ABS Census (rental share, income), state housing open data
- Update: Vacancy monthly; Census every 5 years; open data varies
- Licensing: SQM licensed; ABS CC BY 4.0; check state housing datasets

## Economic Dependency / Concentration
- Metrics: industry concentration, single employer flag
- Source: ABS SA2 industry employment split; local council major employers where available
- Update: Annual / Census-derived
- Licensing: ABS CC BY 4.0; council sources per site

## Growth Drivers
- Metrics: population growth, dwelling approvals, wage proxy, infrastructure spend, distance to CBD, school density/rank proxy
- Source: ABS ERP, building approvals; state infrastructure pipelines; Google Places/SOE for schools (if licensed); geodesic distance to CBD centroid
- Update: Quarterly to annual depending on feed
- Licensing: ABS CC BY 4.0; infra data per agency; avoid restricted APIs unless licensed

## Holding / Transaction Costs
- Metrics: stamp duty, conveyancing, B&P, insurance, council rates, water, PM %, maintenance %, vacancy allowance, selling costs
- Source: Internal assumptions table per state; manual maintenance until automated schedule added
- Update: Ad-hoc/manual

## Mortgage Assumptions
- Metrics: rent shading %, interest buffer %, IO/P&I, tax rate, depreciation toggle
- Source: Internal; user adjustable with profiles
- Update: Manual; surfaced in UI

## Data Quality
- Metrics: missingness %, last_updated, source_confidence per metric/suburb
- Source: Derived during ingestion and scoring
- Update: On every ingest run

## Notes
- All adapters must write raw snapshots to `snapshots/` before transforming.
- Mark any metric sourced from licensed feeds with TODO:LICENSING in code until approvals are confirmed.
