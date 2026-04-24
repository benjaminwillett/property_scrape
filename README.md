# Property Scrape

Property suburb-median scraping, enrichment, decision-engine ingest, and static-site generation for Australian suburb analysis.

This folder contains two related workflows:

1. A browser-driven scraper that enriches suburb CSV data with sale and rent medians.
2. A decision engine and static site builder that turn enriched suburb data into investment-oriented analysis views.

## Project Structure

```text
property_scrape/
├── rea_scrape_medians.py      # Main Playwright scraper for suburb sales/rent medians
├── build_site.py              # Builds static JSON + HTML site from enriched CSV
├── ingest.py                  # Loads source data into SQLite via adapters
├── decision_engine/           # Models, calculations, storage, scoring, source adapters
├── tests/                     # Unit tests for calculation and scoring logic
├── data_sources.md            # Data-source notes and assumptions
├── assumptions.md             # Business assumptions and modelling notes
├── suburbs.csv                # Base suburb input dataset
├── suburbs_enriched.csv       # Example enriched output
├── debug_out/                 # Scraper debug HTML/screenshots on failure
└── site/                      # Generated static site output
```

## Main Components

### 1. `rea_scrape_medians.py`

The scraper reads a base suburb CSV and writes an enriched CSV with:

- 2/3/4 bedroom house sale medians
- 2/3/4 bedroom unit sale medians
- optional REIV rent medians for VIC suburbs
- scrape status, last attempt, failure counts, blacklist status
- source metadata and error fields

It uses Playwright and supports:

- `domain` or `rea` as the sales source
- resumable runs via output CSV state
- failure tracking and blacklisting
- filtered runs by state, suburb, postcode
- retry modes for previously failed rows
- checkpoint saves
- debug HTML/screenshots on scrape failures

### 2. `build_site.py`

Reads an enriched CSV and generates a static site payload for suburb analysis.

Outputs:

- `site/data.json`
- `site/index.html`

It also computes derived metrics such as:

- yields
- 3BR → 4BR price delta
- premium stability
- refinance confidence
- manufactured equity default scenario metrics
- VIC stamp duty
- segmentation and uplift caps

### 3. `ingest.py`

Loads normalized data into SQLite using adapters in `decision_engine/sources/`.

Currently supported ingest sources:

- `stub`
- `csv_bedrooms`

This is the path for storing structured data for downstream decision-engine use.

### 4. `decision_engine/`

Contains the reusable domain logic:

- data models
- SQLite storage and upsert helpers
- bedroom pricing logic
- manufactured equity calculations
- scoring and assumptions
- source adapters
- VIC tax/stamp duty calculations

## Requirements

Minimum practical requirements:

- Python 3.10+
- Playwright for live scraping
- browser binaries for Playwright
- `pytest` for tests

The exact Python environment in this repo may already include these via the local virtual environment under `.venv/`.

## Setup

Create or activate a Python environment, then install the main dependencies you need.

Typical scrape/build flow dependencies:

```bash
pip install playwright pytest
python -m playwright install
```

If you use the repo-local environment on Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
python -m playwright install
```

## Scraper Workflow

The intended pattern is:

1. Start from a clean source dataset such as `suburbs.csv`
2. Write enriched output to a separate file
3. Resume or retry against the enriched output file as needed
4. Build the site from the enriched CSV once the dataset is usable

The scraper is designed so the input CSV is not overwritten directly.

## `rea_scrape_medians.py` Usage

### Core command

```bash
python rea_scrape_medians.py --csv suburbs.csv --out suburbs_enriched.csv --site domain
```

### Main flags

| Flag | Purpose |
|---|---|
| `--csv` | Input suburb dataset. This is treated as the source file. |
| `--out` | Output CSV path for enriched rows and scrape state. |
| `--site` | Sales source: `domain` or `rea`. |
| `--retry-hard-fails` | Retry rows already marked `404` or `nodata`. |
| `--ignore-blacklist` | Process blacklisted rows anyway. |
| `--limit` | Process only the first `N` selected rows. |
| `--headful` | Run Playwright with a visible browser. |
| `--state` | Filter to one or more states, e.g. `VIC` or `VIC,NSW`. |
| `--suburb` | Filter to suburb names containing a text fragment. |
| `--postcode` | Filter to one or more postcodes. |
| `--reiv-rent` | Also scrape REIV weekly rent data for VIC suburbs. |

### Examples

Scrape a small VIC test batch:

```bash
python rea_scrape_medians.py --csv suburbs.csv --out suburbs_enriched.csv --site domain --state VIC --limit 50
```

Run visible browser mode for debugging:

```bash
python rea_scrape_medians.py --csv suburbs.csv --out suburbs_enriched.csv --site rea --headful --limit 10
```

Retry only difficult rows and ignore blacklist protection:

```bash
python rea_scrape_medians.py --csv suburbs.csv --out suburbs_enriched.csv --site domain --retry-hard-fails --ignore-blacklist
```

Scrape VIC suburbs and include REIV rent medians:

```bash
python rea_scrape_medians.py --csv suburbs.csv --out suburbs_enriched.csv --site domain --state VIC --reiv-rent
```

### Scraper state fields

The scraper maintains crawl state in the output CSV, including:

- `scrape_status`
- `last_attempt`
- `fail_count`
- `blacklisted`
- `last_error`

This is what makes retries and resumable runs possible.

### Debug artifacts

On failures, the scraper can write debug output to `debug_out/`, including:

- screenshots
- saved page HTML

That is the first place to inspect when selectors drift or a source site changes structure.

## Build Static Site

Once you have an enriched CSV:

```bash
python build_site.py --csv suburbs_enriched.csv --outdir site
```

Outputs:

- `site/index.html`
- `site/data.json`

The site is static, so you can serve it locally with:

```bash
python -m http.server 8000
```

Then open:

```text
http://localhost:8000/site/
```

## Ingest to SQLite

### Stub demo

```bash
python ingest.py --source stub --db decision_engine.sqlite --snapshot-dir snapshots
```

### CSV bedroom sales ingest

```bash
python ingest.py --source csv_bedrooms --csv-path sales.csv --db decision_engine.sqlite --snapshot-dir snapshots --state VIC
```

The ingest runner:

- initializes the SQLite schema if needed
- writes raw snapshots to the snapshot directory
- upserts normalized records into the decision-engine database

## Decision Engine Storage

The SQLite-backed decision engine stores normalized records such as:

- suburbs
- bedroom medians
- sales comps
- liquidity metrics
- premium history
- refinance confidence
- deal assumptions and results

The storage helpers live in `decision_engine/storage.py` and are designed around idempotent upserts.

## Tests

Run the test suite from the `property_scrape` folder:

```bash
python -m pytest tests
```

Current tests cover areas including:

- calculation engine
- bedroom engine
- manufactured equity logic

## Typical End-to-End Flow

### Option 1: scrape → build site

```bash
python rea_scrape_medians.py --csv suburbs.csv --out suburbs_enriched.csv --site domain --state VIC --reiv-rent
python build_site.py --csv suburbs_enriched.csv --outdir site
python -m http.server 8000
```

### Option 2: ingest normalized data

```bash
python ingest.py --source csv_bedrooms --csv-path sales.csv --db decision_engine.sqlite --snapshot-dir snapshots
```

## Notes

- `suburbs.csv` should be treated as the source dataset; write enrichment to a separate output CSV.
- The scraper includes blacklist/failure logic to avoid repeatedly hammering bad rows.
- `--reiv-rent` is specifically for VIC REIV rent enrichment.
- The site generator is static-only; it does not run a backend.
- Source-site HTML can drift over time, so scraper maintenance is expected.

## Troubleshooting

### Playwright will not launch

Typical symptoms:

- browser executable not found
- Playwright import errors
- immediate crash before any suburb is processed

Checks:

```bash
pip install playwright
python -m playwright install
```

If you are using the repo-local environment on Windows:

```powershell
.\.venv\Scripts\Activate.ps1
python -m playwright install
```

### The scraper runs but returns lots of failures

First steps:

1. Re-run with `--headful` so you can see what the page is doing.
2. Use a small filtered run, for example `--state VIC --limit 10`.
3. Inspect the files written to `debug_out/`.

Useful command:

```bash
python rea_scrape_medians.py --csv suburbs.csv --out suburbs_enriched.csv --site domain --state VIC --limit 10 --headful
```

### Selectors have drifted

Typical symptoms:

- page loads, but medians stay blank
- rows move to `error`, `nodata`, or `404`
- debug HTML shows the site structure has changed

What to inspect:

- latest saved HTML in `debug_out/`
- latest screenshot in `debug_out/`
- whether the target text now appears under a different label or container

This project depends on third-party site markup, so selector maintenance is expected over time.

### Too many rows are blacklisted

The scraper tracks repeated failures using these output fields:

- `fail_count`
- `blacklisted`
- `scrape_status`
- `last_error`

If you want to retry rows that were previously skipped:

```bash
python rea_scrape_medians.py --csv suburbs.csv --out suburbs_enriched.csv --site domain --retry-hard-fails --ignore-blacklist
```

Use that carefully. The blacklist exists to stop repeatedly hammering bad or permanently broken rows.

### Rent data is missing

`--reiv-rent` only applies to VIC REIV rent enrichment. If you are scraping non-VIC suburbs, missing REIV rent fields are expected.

Example:

```bash
python rea_scrape_medians.py --csv suburbs.csv --out suburbs_enriched.csv --site domain --state VIC --reiv-rent
```

### `build_site.py` runs but the site looks stale

Check these points:

1. Confirm you built from the intended enriched CSV.
2. Confirm the generated files in `site/` were replaced.
3. Hard-refresh the browser or open the site in a fresh tab.
4. If serving locally, restart `python -m http.server` after rebuilding.

### The site builds, but metrics look incomplete

That usually means the enriched CSV is missing some upstream inputs, not that the site build itself failed.

Examples:

- missing rent medians
- missing 3BR or 4BR sale medians
- missing comp/sample columns needed for confidence calculations

In that case, inspect the enriched CSV before debugging the frontend.

### SQLite ingest succeeds but results look sparse

For `csv_bedrooms`, confirm the source CSV actually contains the expected columns:

- `suburb`
- `state`
- `postcode`
- `sold_date`
- `price`
- `bedrooms`
- `property_type`

If those columns are incomplete or differently named, the ingest will not produce useful normalized records.

## Related Files

- [data_sources.md](c:\Users\BenWillett\Desktop\tools\property_scrape\data_sources.md)
- [assumptions.md](c:\Users\BenWillett\Desktop\tools\property_scrape\assumptions.md)
- [build_site.py](c:\Users\BenWillett\Desktop\tools\property_scrape\build_site.py)
- [ingest.py](c:\Users\BenWillett\Desktop\tools\property_scrape\ingest.py)
- [rea_scrape_medians.py](c:\Users\BenWillett\Desktop\tools\property_scrape\rea_scrape_medians.py)
