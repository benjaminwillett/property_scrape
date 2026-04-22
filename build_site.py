#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import json
import os
from typing import Dict, List, Optional

from decision_engine.bedroom_engine import price_delta_3_to_4
from decision_engine.manufactured_equity import analyse_row, premium_stability_score, refi_confidence, segmentation_from_medians
from decision_engine.tax.vic import stamp_duty


DEFAULT_SCENARIO_INPUTS = {
  "purchase_discount_pct": 5.0,
  "reno_cost": 100_000.0,
  "contingency_pct": 0.10,
  "duration_weeks": 16.0,
  "interest_rate": 6.5,
  "legal_fees": 2000.0,
  "deposit_pct": 20.0,
  "lvr_refi": 0.80,
  "reno_funding_mode": "cash",
  "include_rent_during_reno": False,
  "vacancy_weeks": 16.0,
  "misc_costs": 2500.0,
}


def load_csv(path: str):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def safe_float(val: Optional[str]) -> Optional[float]:
    try:
        if val is None:
            return None
        s = str(val).replace(",", "").strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def safe_int(val: Optional[str]) -> Optional[int]:
    try:
        if val is None:
            return None
        s = str(val).replace(",", "").strip()
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def enrich_rows(rows: List[Dict]) -> List[Dict]:
    now = dt.datetime.now(dt.timezone.utc).date()
    enriched = []
    for r in rows:
        comp_prices = None
        row = dict(r)
        row["suburb"] = (row.get("suburb") or "").title()
        row["state"] = (row.get("state") or "").upper()
        row["postcode"] = row.get("postcode") or ""
        row_id = row.get("suburb_id") or f"{row['state']}-{row['postcode']}-{row['suburb'].lower().replace(' ', '-')}"
        row["suburb_id"] = row_id

        row["sale_2br_house"] = safe_float(row.get("median_2br_house_price_aud") or row.get("median_2br_price"))
        row["sale_3br_house"] = safe_float(row.get("median_3br_house_price_aud") or row.get("median_3br_price"))
        row["sale_4br_house"] = safe_float(row.get("median_4br_house_price_aud") or row.get("median_4br_price"))

        row["sale_2br_unit"] = safe_float(row.get("median_2br_unit_price_aud") or row.get("median_2br_unit_price"))
        row["sale_3br_unit"] = safe_float(row.get("median_3br_unit_price_aud") or row.get("median_3br_unit_price"))
        row["sale_4br_unit"] = safe_float(row.get("median_4br_unit_price_aud") or row.get("median_4br_unit_price"))

        row["rent_2br_house"] = safe_float(row.get("median_2br_house_rent_per_week_aud") or row.get("median_2br_rent_pw"))
        row["rent_3br_house"] = safe_float(row.get("median_3br_house_rent_per_week_aud") or row.get("median_3br_rent_pw"))
        row["rent_4br_house"] = safe_float(row.get("median_4br_house_rent_per_week_aud") or row.get("median_4br_rent_pw"))

        row["rent_2br_unit"] = safe_float(row.get("median_2br_unit_rent_per_week_aud") or row.get("median_2br_unit_rent_pw"))
        row["rent_3br_unit"] = safe_float(row.get("median_3br_unit_rent_per_week_aud") or row.get("median_3br_unit_rent_pw"))
        row["rent_4br_unit"] = safe_float(row.get("median_4br_unit_rent_per_week_aud") or row.get("median_4br_unit_rent_pw"))

        # ensure canonical keys are filled for table rendering
        row["median_2br_house_price_aud"] = row.get("sale_2br_house")
        row["median_3br_house_price_aud"] = row.get("sale_3br_house")
        row["median_4br_house_price_aud"] = row.get("sale_4br_house")
        row["median_2br_unit_price_aud"] = row.get("sale_2br_unit")
        row["median_3br_unit_price_aud"] = row.get("sale_3br_unit")
        row["median_4br_unit_price_aud"] = row.get("sale_4br_unit")

        row["median_2br_house_rent_per_week_aud"] = row.get("rent_2br_house")
        row["median_3br_house_rent_per_week_aud"] = row.get("rent_3br_house")
        row["median_4br_house_rent_per_week_aud"] = row.get("rent_4br_house")
        row["median_2br_unit_rent_per_week_aud"] = row.get("rent_2br_unit")
        row["median_3br_unit_rent_per_week_aud"] = row.get("rent_3br_unit")
        row["median_4br_unit_rent_per_week_aud"] = row.get("rent_4br_unit")

        def implied_yield(rent_pw: Optional[float], price: Optional[float]) -> Optional[float]:
          if rent_pw in (None, 0) or price in (None, 0):
            return None
          return (rent_pw * 52.0 / price) * 100.0

        row["yield_2br_house_pct"] = implied_yield(row.get("rent_2br_house"), row.get("sale_2br_house"))
        row["yield_3br_house_pct"] = implied_yield(row.get("rent_3br_house"), row.get("sale_3br_house"))
        row["yield_4br_house_pct"] = implied_yield(row.get("rent_4br_house"), row.get("sale_4br_house"))
        row["yield_2br_unit_pct"] = implied_yield(row.get("rent_2br_unit"), row.get("sale_2br_unit"))
        row["yield_3br_unit_pct"] = implied_yield(row.get("rent_3br_unit"), row.get("sale_3br_unit"))
        row["yield_4br_unit_pct"] = implied_yield(row.get("rent_4br_unit"), row.get("sale_4br_unit"))

        delta, pct = price_delta_3_to_4(row.get("sale_3br_house"), row.get("sale_4br_house"))
        row["price_delta_3_to_4"] = delta
        row["pct_delta_3_to_4"] = pct
        stability = safe_float(row.get("premium_stability_years") or row.get("premium_stability"))
        if stability is None and pct is not None:
            stability = 1.0
        row["premium_stability"] = stability

        row["sales_per_month"] = safe_float(row.get("sales_per_month"))
        row["stock_on_market_pct"] = safe_float(row.get("stock_on_market_pct"))
        row["vendor_discount_pct"] = safe_float(row.get("vendor_discount_pct"))
        row["comp_count_4br"] = safe_int(row.get("comp_count_4br") or row.get("sales_4br_samples")) or 0
        row["sale_dispersion_4br_pct"] = safe_float(row.get("sale_dispersion_4br_pct"))
        row["weighted_median_4br_comps"] = safe_float(row.get("weighted_median_4br_comps"))

        as_of = (row.get("as_of_date") or row.get("as_of") or "").strip()
        freshness_days = None
        if as_of:
            for fmt in ("%Y-%m-%d", "%Y-%m"):
                try:
                    dt_as_of = dt.datetime.strptime(as_of, fmt).date()
                    freshness_days = (now - dt_as_of).days
                    break
                except ValueError:
                    continue
        row["data_freshness_days"] = freshness_days
        if freshness_days is None:
            row["data_freshness_label"] = "unknown"
        elif freshness_days <= 60:
            row["data_freshness_label"] = "< 2 mo"
        elif freshness_days <= 180:
            row["data_freshness_label"] = "< 6 mo"
        else:
            row["data_freshness_label"] = "> 6 mo"

        base_purchase = row.get("sale_3br_house") or 0
        row["stamp_duty_vic"] = stamp_duty(base_purchase) if base_purchase else None

        seg = segmentation_from_medians(row.get("sale_3br_house"), row.get("sale_4br_house"))
        row["segmentation"] = seg.label
        row["segmentation_ratio"] = seg.ratio
        row["max_capture"] = seg.max_capture
        row["uplift_factor_cap"] = seg.uplift_factor_cap

        premium_history: List[Optional[float]] = []
        stability_result = premium_stability_score(premium_history)
        if row.get("premium_stability") is None:
          row["premium_stability"] = stability_result.score
        row["premium_stability_label"] = stability_result.label if row.get("premium_stability") is None else ("stable" if row["premium_stability"] >= 0.8 else "variable")

        row["refi_confidence_score"] = refi_confidence(
          row.get("comp_count_4br"),
          row.get("sale_dispersion_4br_pct"),
          row.get("sales_per_month"),
          row.get("data_freshness_days"),
        )

        analysis = analyse_row(
           row.get("sale_3br_house"),
           row.get("sale_4br_house"),
           premium_history,
           row.get("comp_count_4br"),
           row.get("sale_dispersion_4br_pct"),
           row.get("sales_per_month"),
           row.get("data_freshness_days"),
           purchase_discount_pct=DEFAULT_SCENARIO_INPUTS["purchase_discount_pct"],
           reno_cost=DEFAULT_SCENARIO_INPUTS["reno_cost"],
           contingency_pct=DEFAULT_SCENARIO_INPUTS["contingency_pct"],
           duration_weeks=DEFAULT_SCENARIO_INPUTS["duration_weeks"],
           interest_rate=DEFAULT_SCENARIO_INPUTS["interest_rate"],
           legal_fees=DEFAULT_SCENARIO_INPUTS["legal_fees"],
           deposit_pct=DEFAULT_SCENARIO_INPUTS["deposit_pct"],
           reno_funding_mode=DEFAULT_SCENARIO_INPUTS["reno_funding_mode"],
           misc_costs=DEFAULT_SCENARIO_INPUTS["misc_costs"],
           include_rent_during_reno=DEFAULT_SCENARIO_INPUTS["include_rent_during_reno"],
           vacancy_weeks=DEFAULT_SCENARIO_INPUTS["vacancy_weeks"],
           lvr_refi=DEFAULT_SCENARIO_INPUTS["lvr_refi"],
           state=row["state"],
           weighted_median_4br_comps=row.get("weighted_median_4br_comps"),
           rent_median_3br_house_pw=row.get("rent_3br_house"),
           rent_median_4br_house_pw=row.get("rent_4br_house"),
          comp_prices_4br=comp_prices,
          comp_count_12m=row.get("comp_count_4br"),
          p75_comps_12m=row.get("p75_4br_comps_12m"),
          median_2br_house=row.get("sale_2br_house"),
          median_3br_unit=row.get("sale_3br_unit"),
        )
        if analysis:
          for key, val in analysis.items():
            row[f"default_{key}"] = val

        enriched.append(row)
    return enriched


def build_main_html() -> str:
    return """<!doctype html>
<html>
<head>
<meta charset=\"utf-8\">
<title>AU Suburb Medians</title>
<style>
body { font-family:'Manrope','Segoe UI',system-ui,-apple-system,sans-serif; margin:0; background:#f6f7fb; color:#0f172a; }
.nav { position:sticky; top:0; z-index:20; backdrop-filter:blur(8px); background:rgba(246,247,251,0.95); border-bottom:1px solid #e6e8f0; padding:12px 18px; display:flex; align-items:center; justify-content:space-between; }
.nav a { color:#0f172a; text-decoration:none; font-weight:600; margin-right:12px; }
.nav .brand { font-weight:800; letter-spacing:-0.2px; }
.page { max-width:1200px; margin:0 auto; padding:18px 18px 120px; }
h2 { margin:6px 0 4px; letter-spacing:-0.3px; }
.small { color:#475569; font-size:13px; line-height:1.6; }
.controls { display:flex; flex-wrap:wrap; align-items:flex-end; gap:12px; margin:14px 0; }
label { font-size:12px; color:#334155; }
input, select { padding:8px 10px; border:1px solid #cbd5e1; border-radius:10px; font-size:14px; background:white; }
.table-wrap { border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; background:white; box-shadow:0 10px 28px rgba(15,31,43,0.05); }
table { border-collapse:collapse; width:100%; }
th, td { padding:9px 10px; border-bottom:1px solid #e9edf5; }
th { position:sticky; top:0; background:#f9fbff; font-size:12px; text-transform:uppercase; letter-spacing:0.5px; cursor:pointer; }
td.num { text-align:right; }
.badge { display:inline-flex; align-items:center; padding:2px 8px; border-radius:12px; font-size:12px; }
.badge.nodata { background:#e2e8f0; color:#334155; }
.badge.err { background:#fee2e2; color:#991b1b; }
.hl { background:#fff7cc; }
.mono { font-variant-numeric: tabular-nums; }
[data-view="sales"] .col-rent, [data-view="sales"] .col-yield { display:none; }
.viewnote { background:#eef2ff; color:#312e81; border:1px solid #c7d2fe; padding:8px 10px; border-radius:10px; font-size:12px; margin:6px 0; }
</style>
</head>
<body>
  <div class=\"nav\">
    <div>
      <a class=\"brand\" href=\"index.html\">AU Suburb Medians</a>
      <span class=\"small\">Sales + rents (2/3/4BR)</span>
    </div>
    <div>
      <a href=\"index.html\">Yield & Cashflow</a>
      <a href=\"manufactured-equity.html\">3→4 Manufactured Equity</a>
    </div>
  </div>
  <div class=\"page\">
    <h2>AU Suburb Medians (2/3/4BR Houses + Units)</h2>
    <div class=\"small\">Tip: “—” means NO_DATA. Blank means not scraped/unknown. % gap uses (n+1 − n) / n. $ gap uses (n+1 − n). Price range filter applies to the selected field.</div>

    <div class=\"controls\">
      <div>
        <label>Search suburb</label><br>
        <input id=\"search\" placeholder=\"e.g. Mentone\">
      </div>
      <div>
        <label>State</label><br>
        <select id=\"state\">
          <option value=\"\">All States</option>
          <option>ACT</option><option>NSW</option><option>NT</option><option>QLD</option><option>SA</option><option>TAS</option><option>VIC</option><option>WA</option>
        </select>
      </div>
      <div>
        <label>Sort by</label><br>
        <select id=\"sortKey\">
          <option value=\"\">No sorting</option>
          <option value=\"median_2br_house_price_aud\">House 2BR</option>
          <option value=\"median_3br_house_price_aud\">House 3BR</option>
          <option value=\"median_4br_house_price_aud\">House 4BR</option>
          <option value=\"median_2br_unit_price_aud\">Unit 2BR</option>
          <option value=\"median_3br_unit_price_aud\">Unit 3BR</option>
          <option value=\"median_4br_unit_price_aud\">Unit 4BR</option>
          <option value=\"pct_gap\">% Gap (n → n+1)</option>
          <option value=\"abs_gap\">$ Gap (n → n+1)</option>
        </select>
      </div>
      <div>
        <label>Direction</label><br>
        <select id=\"sortDir\">
          <option value=\"desc\">Descending</option>
          <option value=\"asc\">Ascending</option>
        </select>
      </div>
      <div>
        <label>Gap type</label><br>
        <select id=\"gapType\">
          <option value=\"house\">House</option>
          <option value=\"unit\">Unit</option>
        </select>
      </div>
      <div>
        <label>Compare</label><br>
        <select id=\"gapPair\">
          <option value=\"2-3\">2 → 3</option>
          <option value=\"3-4\">3 → 4</option>
        </select>
      </div>
      <div>
        <label>View</label><br>
        <select id="viewMode">
          <option value="sales">Sales only</option>
          <option value="rents">Rents + sales</option>
        </select>
      </div>
      <div>
        <label>Highlight if % ≥</label><br>
        <input id=\"gapThreshold\" class=\"range\" type=\"number\" step=\"0.1\" value=\"20\">
      </div>
      <div>
        <label>Range field</label><br>
        <select id=\"rangeField\">
          <option value=\"\">(no range filter)</option>
          <option value=\"median_2br_house_price_aud\">House 2BR</option>
          <option value=\"median_3br_house_price_aud\">House 3BR</option>
          <option value=\"median_4br_house_price_aud\">House 4BR</option>
          <option value=\"median_2br_unit_price_aud\">Unit 2BR</option>
          <option value=\"median_3br_unit_price_aud\">Unit 3BR</option>
          <option value=\"median_4br_unit_price_aud\">Unit 4BR</option>
          <option value=\"abs_gap\">$ Gap (n→n+1)</option>
          <option value=\"pct_gap\">% Gap (n→n+1)</option>
        </select>
      </div>
      <div>
        <label>Min</label><br>
        <input id=\"minVal\" class=\"range\" type=\"number\" step=\"1\" placeholder=\"e.g. 500000\">
      </div>
      <div>
        <label>Max</label><br>
        <input id=\"maxVal\" class=\"range\" type=\"number\" step=\"1\" placeholder=\"e.g. 1500000\">
      </div>
    </div>

    <div class=\"table-wrap\">
      <table>
        <thead>
          <tr>
            <th rowspan="2">Postcode</th>
            <th rowspan="2">State</th>
            <th rowspan="2">Suburb</th>
            <th colspan="3" class="col-sale">House sale</th>
            <th colspan="3" class="col-sale">Unit sale</th>
            <th colspan="3" class="col-rent">House rent pw</th>
            <th colspan="3" class="col-rent">Unit rent pw</th>
            <th colspan="2" class="col-yield">Gross yield</th>
            <th colspan="2" id="gapHeader">Gap (n→n+1)</th>
            <th rowspan="2">As Of</th>
            <th rowspan="2">Status</th>
          </tr>
          <tr>
            <th class=\"num\">2BR</th><th class=\"num\">3BR</th><th class=\"num\">4BR</th>
            <th class=\"num\">2BR</th><th class=\"num\">3BR</th><th class=\"num\">4BR</th>
            <th class=\"num col-rent\">2BR</th><th class=\"num col-rent\">3BR</th><th class=\"num col-rent\">4BR</th>
            <th class=\"num col-rent\">2BR</th><th class=\"num col-rent\">3BR</th><th class=\"num col-rent\">4BR</th>
            <th class=\"num col-yield\">3BR H</th><th class=\"num col-yield\">2BR U</th>
            <th class=\"num mono\" id=\"gapPctHeader\">% Gap</th>
            <th class=\"num mono\" id=\"gapAbsHeader\">$ Gap</th>
          </tr>
        </thead>
        <tbody id=\"rows\"></tbody>
      </table>
    </div>
    <div id=\"status\" class=\"small\"></div>
  </div>

<script>
const fmt = new Intl.NumberFormat('en-AU', { style:'currency', currency:'AUD', maximumFractionDigits:0 });
const pctFmt = new Intl.NumberFormat('en-AU', { maximumFractionDigits:1, minimumFractionDigits:0 });
let DATA = [];

function money(x){
  if(x === null || x === undefined || x === '' || x === 'NO_DATA') return '—';
  const n = Number(x);
  if (Number.isNaN(n)) return '';
  return fmt.format(n);
}

function statusBadge(r){
  const e = (r.last_error || '').trim();
  if (!e) return '';
  if (e === 'NO_DATA') return '<span class="badge nodata">NO_DATA</span>';
  return `<span class="badge err">${e}</span>`;
}

function numericOrNull(v){
  if (v === null || v === undefined || v === '' || v === 'NO_DATA') return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}

function pctGap(base, next){
  if (base === null || next === null) return null;
  if (base <= 0) return null;
  return ((next - base) / base) * 100.0;
}

function absGap(base, next){
  if (base === null || next === null) return null;
  return (next - base);
}

function gapKeys(gapType, pair){
  const isHouse = gapType === 'house';
  const baseKey = isHouse
    ? (pair === '2-3' ? 'median_2br_house_price_aud' : 'median_3br_house_price_aud')
    : (pair === '2-3' ? 'median_2br_unit_price_aud'  : 'median_3br_unit_price_aud');

  const nextKey = isHouse
    ? (pair === '2-3' ? 'median_3br_house_price_aud' : 'median_4br_house_price_aud')
    : (pair === '2-3' ? 'median_3br_unit_price_aud'  : 'median_4br_unit_price_aud');

  return { baseKey, nextKey };
}

function getGapValues(r, gapType, pair){
  const { baseKey, nextKey } = gapKeys(gapType, pair);
  const base = numericOrNull(r[baseKey]);
  const next = numericOrNull(r[nextKey]);
  return {
    base,
    next,
    pct: pctGap(base, next),
    abs: absGap(base, next)
  };
}

function fmtPct(x){
  if (x === null || x === undefined) return '';
  if (Number.isNaN(x)) return '';
  const sign = x > 0 ? '+' : '';
  return sign + pctFmt.format(x) + '%';
}

function fmtAbs(x){
  if (x === null || x === undefined) return '';
  if (Number.isNaN(x)) return '';
  const sign = x > 0 ? '+' : '';
  return sign + fmt.format(x);
}

function inRange(val, minV, maxV){
  if (val === null) return false;
  if (minV !== null && val < minV) return false;
  if (maxV !== null && val > maxV) return false;
  return true;
}

function render(){
  const search = document.getElementById('search').value.toLowerCase();
  const state = document.getElementById('state').value;
  const sortKey = document.getElementById('sortKey').value;
  const sortDir = document.getElementById('sortDir').value;
  const gapType = document.getElementById('gapType').value;
  const gapPair = document.getElementById('gapPair').value;
  const viewMode = document.getElementById('viewMode').value;
  const threshold = Number(document.getElementById('gapThreshold').value || 0);
  const rangeField = document.getElementById('rangeField').value;
  const minRaw = document.getElementById('minVal').value.trim();
  const maxRaw = document.getElementById('maxVal').value.trim();
  const minV = minRaw === '' ? null : Number(minRaw);
  const maxV = maxRaw === '' ? null : Number(maxRaw);

  document.body.setAttribute('data-view', viewMode);

  const compareLabel = (gapPair === '2-3' ? '2→3' : '3→4') + ' ' + (gapType === 'house' ? 'House' : 'Unit');
  document.getElementById('gapPctHeader').textContent = '% Gap ' + compareLabel;
  document.getElementById('gapAbsHeader').textContent = '$ Gap ' + compareLabel;

  let filtered = DATA.filter(r =>
    (!state || r.state === state) &&
    (!search || (r.suburb || '').toLowerCase().includes(search))
  );

  if (rangeField) {
    filtered = filtered.filter(r => {
      const g = getGapValues(r, gapType, gapPair);
      let val = null;
      if (rangeField === 'pct_gap') val = g.pct;
      else if (rangeField === 'abs_gap') val = g.abs;
      else val = numericOrNull(r[rangeField]);
      if (minV !== null || maxV !== null) {
        return inRange(val, minV, maxV);
      }
      return true;
    });
  }

  if (sortKey) {
    filtered = filtered.slice();
    filtered.sort((a,b) => {
      let av = null, bv = null;
      const ga = getGapValues(a, gapType, gapPair);
      const gb = getGapValues(b, gapType, gapPair);
      if (sortKey === 'pct_gap') { av = ga.pct; bv = gb.pct; }
      else if (sortKey === 'abs_gap') { av = ga.abs; bv = gb.abs; }
      else { av = numericOrNull(a[sortKey]); bv = numericOrNull(b[sortKey]); }
      if (av === null && bv === null) return 0;
      if (av === null) return 1;
      if (bv === null) return -1;
      return sortDir === 'asc' ? (av - bv) : (bv - av);
    });
  }

  const tbody = document.getElementById('rows');
  tbody.innerHTML = filtered.map(r => {
    const g = getGapValues(r, gapType, gapPair);
    const highlight = (g.pct !== null && g.pct >= threshold);
    return `
      <tr class="${highlight ? 'hl' : ''}">
        <td>${r.postcode || ''}</td>
        <td>${r.state || ''}</td>
        <td>${r.suburb || ''}</td>
        <td class="num">${money(r.median_2br_house_price_aud)}</td>
        <td class="num">${money(r.median_3br_house_price_aud)}</td>
        <td class="num">${money(r.median_4br_house_price_aud)}</td>
        <td class="num">${money(r.median_2br_unit_price_aud)}</td>
        <td class="num">${money(r.median_3br_unit_price_aud)}</td>
        <td class="num">${money(r.median_4br_unit_price_aud)}</td>
        <td class="num col-rent">${money(r.median_2br_house_rent_per_week_aud)}</td>
        <td class="num col-rent">${money(r.median_3br_house_rent_per_week_aud)}</td>
        <td class="num col-rent">${money(r.median_4br_house_rent_per_week_aud)}</td>
        <td class="num col-rent">${money(r.median_2br_unit_rent_per_week_aud)}</td>
        <td class="num col-rent">${money(r.median_3br_unit_rent_per_week_aud)}</td>
        <td class="num col-rent">${money(r.median_4br_unit_rent_per_week_aud)}</td>
        <td class="num mono col-yield">${fmtPct(r.yield_3br_house_pct)}</td>
        <td class="num mono col-yield">${fmtPct(r.yield_2br_unit_pct)}</td>
        <td class="num mono">${fmtPct(g.pct)}</td>
        <td class="num mono">${fmtAbs(g.abs)}</td>
        <td>${r.as_of_date || ''}</td>
        <td>${statusBadge(r)}</td>
      </tr>`;
  }).join('');

  document.getElementById('status').textContent = `${filtered.length} suburbs ${sortKey ? `(sorted by ${sortKey} ${sortDir})` : ''}`;
}

['search','state','sortKey','sortDir','gapType','gapPair','gapThreshold','rangeField','minVal','maxVal','viewMode'].forEach(id => {
  const el = document.getElementById(id);
  el.addEventListener(id === 'search' || id === 'gapThreshold' || id === 'minVal' || id === 'maxVal' ? 'input' : 'change', render);
});

fetch('data.json').then(r => r.json()).then(json => { DATA = json; render(); }).catch(err => {
  document.getElementById('status').textContent = 'Failed to load data';
  console.error(err);
});
</script>
</body>
</html>
"""


def build_manufactured_html() -> str:
    return """<!doctype html>
<html>
<head>
<meta charset=\"utf-8\">
<title>3→4 Manufactured Equity</title>
<style>
body { margin:0; background: radial-gradient(circle at 10% 16%, #eef3ff, #f7f9ff 40%, #ffffff); color:#0e1c2c; font-family:'Manrope','Segoe UI',system-ui,-apple-system,sans-serif; }
.nav { position:sticky; top:0; z-index:30; backdrop-filter:blur(10px); background:rgba(255,255,255,0.9); border-bottom:1px solid #e5eaf1; padding:12px 18px; display:flex; align-items:center; justify-content:space-between; }
.nav a { color:#0e1c2c; text-decoration:none; font-weight:700; margin-right:14px; }
.nav .brand { letter-spacing:-0.2px; }
.page { max-width: 1420px; margin:0 auto; padding: 22px 24px 120px; }
h1 { margin:0 0 4px; letter-spacing:-0.4px; }
.muted { color:#5b6673; }
.section { background:white; border:1px solid #e5eaf1; border-radius:14px; padding:16px 18px; margin-top:14px; box-shadow:0 10px 28px rgba(15,31,43,0.05); }
.pill { padding:6px 10px; border-radius:14px; background:#0f6ad8; color:white; font-size:12px; letter-spacing:0.2px; }
.controls { display:grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap:12px; align-items:end; }
.controls label { font-size:12px; color:#314356; letter-spacing:0.3px; }
input, select, button { padding:9px 10px; border:1px solid #cbd5e1; border-radius:10px; font-size:14px; background:white; }
button { background:#0f6ad8; color:white; border:none; box-shadow:0 10px 26px rgba(15,106,216,0.18); cursor:pointer; transition:transform 80ms ease, box-shadow 120ms ease; }
button:hover { transform:translateY(-1px); box-shadow:0 12px 30px rgba(15,106,216,0.24); }
button.ghost { background:white; color:#0f6ad8; border:1px solid #0f6ad8; box-shadow:none; }
.table-wrap { overflow:auto; max-height:640px; border:1px solid #e5eaf1; border-radius:12px; background:white; }
table { border-collapse:collapse; width:100%; }
th, td { padding:8px 10px; border-bottom:1px solid #edf1f7; }
th { position:sticky; top:0; background:white; cursor:pointer; font-size:12px; text-transform:uppercase; letter-spacing:0.6px; }
td { font-size:13px; }
.num { text-align:right; }
.score { font-weight:700; }
.score.good { color:#0a8a4b; }
.score.mid { color:#d99200; }
.score.bad { color:#c0392b; }
.grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap:12px; }
.card { border:1px solid #e5eaf1; border-radius:12px; padding:12px; background:#fbfdff; box-shadow:0 6px 16px rgba(15,31,43,0.06); }
.small { font-size:12px; color:#5c6b78; line-height:1.5; }
.badge { display:inline-flex; align-items:center; gap:6px; padding:6px 9px; border-radius:12px; background:#0e1c2c; color:white; font-size:12px; }
.chip-row { display:flex; flex-wrap:wrap; gap:8px; }
.alert { padding:8px 10px; background:#fff6e5; border:1px solid #ffe5ad; border-radius:10px; color:#7a4c00; }
.sticky { position:sticky; top:0; z-index:5; background:white; }
.tag { display:inline-block; padding:4px 8px; border-radius:10px; background:#eef3ff; color:#0e1c2c; font-size:11px; letter-spacing:0.2px; }
</style>
</head>
<body>
<div class=\"nav\">
  <div>
    <a class=\"brand\" href=\"index.html\">AU Suburb Medians</a>
    <span style=\"color:#475569; font-weight:600;\">3→4 Manufactured Equity</span>
  </div>
  <div>
    <a href=\"index.html\">Yield & Cashflow</a>
    <a href=\"manufactured-equity.html\">3→4 Manufactured Equity</a>
  </div>
</div>
<div class="page">
  <header class="sticky">
    <div style="display:flex; justify-content:space-between; gap:12px; flex-wrap:wrap; align-items:flex-start;">
      <div>
        <h1>3→4 Bedroom Manufactured Equity</h1>
        <div class="muted">Rank suburbs by uplift after adding a bedroom. Bank-style valuation, segmentation caps, and refi confidence baked in.</div>
        <div class="chip-row" style="margin-top:6px;">
          <span class="pill">Focus: House 3→4 conversions</span>
          <span class="pill">Outputs: Manufactured equity + refi confidence</span>
          <span class="pill">Not financial advice</span>
        </div>
      </div>
      <div class="badge">Static tool — rebuild after ingest</div>
    </div>
  </header>

  <section class="section">
    <div class="controls">
      <div>
        <label>Search suburb</label>
        <input id="search" placeholder="e.g. Mentone">
      </div>
      <div>
        <label>State</label>
        <select id="state">
          <option value="">All</option>
          <option>ACT</option><option>NSW</option><option>NT</option><option>QLD</option><option>SA</option><option>TAS</option><option>VIC</option><option>WA</option>
        </select>
      </div>
      <div>
        <label>Scenario preset</label>
        <select id="preset">
          <option value="conservative">Conservative</option>
          <option value="standard" selected>Standard</option>
          <option value="aggressive">Aggressive</option>
        </select>
      </div>
      <div>
        <label>Purchase discount % vs 3BR median</label>
        <input id="purchaseDiscount" type="number" step="0.5" value="5">
      </div>
      <div>
        <label>Deposit %</label>
        <input id="depositPct" type="number" step="0.5" value="20">
      </div>
      <div>
        <label>Reno cost (AUD)</label>
        <input id="renoCost" type="number" step="1000" value="100000">
      </div>
      <div>
        <label>Contingency % (of reno)</label>
        <input id="contingencyPct" type="number" step="1" value="10">
      </div>
      <div>
        <label>Duration (weeks)</label>
        <input id="durationWeeks" type="number" step="1" value="16">
      </div>
      <div>
        <label>Interest rate % (annual)</label>
        <input id="interestRate" type="number" step="0.1" value="6.5">
      </div>
      <div>
        <label>Legal / conveyancing (AUD)</label>
        <input id="legalFees" type="number" step="500" value="2000">
      </div>
      <div>
        <label>Misc costs (AUD)</label>
        <input id="miscCosts" type="number" step="500" value="2500">
      </div>
      <div>
        <label>Reno funding</label>
        <select id="renoFunding">
          <option value="cash" selected>Cash (savings)</option>
          <option value="borrowed">Borrowed</option>
        </select>
      </div>
      <div>
        <label>Refi target LVR</label>
        <select id="refiLvr">
          <option value="0.8" selected>80%</option>
          <option value="0.9">90%</option>
        </select>
      </div>
      <div>
        <label>Include rent during reno</label>
        <select id="includeRent">
          <option value="off" selected>Off</option>
          <option value="on">On</option>
        </select>
      </div>
      <div>
        <label>Vacancy weeks during reno</label>
        <input id="vacancyWeeks" type="number" step="1" value="16">
      </div>
      <div>
        <label>Minimum 4BR comps (6m)</label>
        <input id="minComps" type="number" step="1" value="0">
      </div>
      <div>
        <label>Sort by</label>
        <select id="sortKey">
          <option value="repeatability_score" selected>Repeatability score</option>
          <option value="deal_score">Deal score</option>
          <option value="manufactured_equity">Manufactured equity</option>
          <option value="refi_confidence_score">Refi confidence</option>
          <option value="valuation_safety_score">Valuation safety</option>
          <option value="rent_support_score">Rent support</option>
          <option value="usable_equity_80">Usable equity @80%</option>
          <option value="capture_pct">Capture %</option>
          <option value="premium_stability">Premium stability</option>
        </select>
      </div>
    </div>
    <div style="margin-top:10px; display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
      <button class="ghost" id="applyPreset">Apply preset</button>
      <div class="alert">Assumes VIC stamp duty schedule; holding cost is interest-only during reno; usable equity uses purchase + reno debt (v1 simplification).</div>
    </div>
  </section>

  <section class="section">
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th data-key="suburb">Suburb</th>
            <th data-key="state">State</th>
            <th data-key="postcode">Postcode</th>
            <th data-key="sale_3br_house">3BR median</th>
            <th data-key="sale_4br_house">4BR median</th>
            <th data-key="price_delta_3_to_4">Δ 3→4</th>
            <th data-key="pct_delta_3_to_4">Premium %</th>
            <th data-key="premium_stability">Premium stability</th>
            <th data-key="all_in_cost">All-in cost</th>
            <th data-key="expected_bank_value">Expected bank val</th>
            <th data-key="manufactured_equity">Manufactured equity</th>
            <th data-key="usable_equity_80">Usable equity @80%</th>
            <th data-key="refi_confidence_score">Refi confidence</th>
            <th data-key="capture_pct">Capture %</th>
            <th data-key="valuation_safety_score">Val safety</th>
            <th data-key="capture_discount">Capture discount</th>
            <th data-key="captureable_gap_pct">Captureable gap %</th>
            <th data-key="captureable_upside">Captureable upside</th>
            <th data-key="rent_support_score">Rent support</th>
            <th data-key="total_debt_at_refi">Total debt @ refi</th>
            <th data-key="cash_in">Cash in</th>
            <th data-key="net_holding_cost">Net holding</th>
            <th data-key="usable_equity_selected_lvr">Usable equity @ LVR</th>
            <th data-key="cash_recovered">Cash recovered</th>
            <th data-key="recycle_ratio">Recycle ratio</th>
            <th data-key="repeatability_score">Repeatability score</th>
            <th data-key="deal_score">Deal score</th>
          </tr>
        </thead>
        <tbody id="rows"></tbody>
      </table>
    </div>
    <div class="small" id="count"></div>
  </section>

  <section class="section">
    <div class="grid" id="detail"></div>
  </section>

  <section class="section">
    <h4 style="margin:0 0 6px;">Disclaimers</h4>
    <div class="small">Indicative only. Segmentation caps aim to avoid prestige false positives; validation with lender valuer still required. Confidence model blends comp count, dispersion, liquidity, and recency. Holding income excluded during reno. Usable equity assumes 80% LVR on expected bank value.</div>
  </section>
</div>

<script>
let data = [];
let filtered = [];
let sortKey = 'repeatability_score';
let sortDir = 'desc';

const DEFAULTS = {
  purchaseDiscount: 5,
  depositPct: 20,
  renoCost: 100000,
  contingencyPct: 10,
  durationWeeks: 16,
  interestRate: 6.5,
  legalFees: 2000,
  miscCosts: 2500,
  renoFunding: 'cash',
  refiLvr: 0.8,
  includeRent: false,
  vacancyWeeks: 16,
};
const presets = {
  conservative: { purchaseDiscount: 4, renoCost: 120000, contingencyPct: 12, durationWeeks: 20, interestRate: 7.2, legalFees: 2200 },
  standard: { purchaseDiscount: DEFAULTS.purchaseDiscount, renoCost: DEFAULTS.renoCost, contingencyPct: DEFAULTS.contingencyPct, durationWeeks: DEFAULTS.durationWeeks, interestRate: DEFAULTS.interestRate, legalFees: DEFAULTS.legalFees },
  aggressive: { purchaseDiscount: 7, renoCost: 90000, contingencyPct: 8, durationWeeks: 12, interestRate: 6.0, legalFees: 1800 },
};

function clamp01(x){ return Math.max(0, Math.min(1, x)); }
function num(val){ const n = Number(val); return Number.isFinite(n) ? n : null; }
function money(val){ if(val === null || val === undefined || Number.isNaN(val)) return ''; return new Intl.NumberFormat('en-AU', {style:'currency', currency:'AUD', maximumFractionDigits:0}).format(val); }
function pct(val){ if(val === null || val === undefined || Number.isNaN(val)) return ''; return `${val.toFixed(1)}%`; }

function segment(row){
  const sale3 = num(row.sale_3br_house);
  const sale4 = num(row.sale_4br_house);
  if(!sale3 || !sale4) return { label:'unknown', ratio:null, max_capture:0.85, uplift_factor_cap:3.0 };
  const ratio = (sale4 - sale3) / sale3;
  if(row.max_capture && row.uplift_factor_cap){
    return { label: row.segmentation || 'unknown', ratio: row.segmentation_ratio || ratio, max_capture: row.max_capture, uplift_factor_cap: row.uplift_factor_cap };
  }
  if(ratio > 0.70) return { label:'prestige', ratio, max_capture:0.55, uplift_factor_cap:2.0 };
  if(ratio > 0.45) return { label:'high', ratio, max_capture:0.70, uplift_factor_cap:3.0 };
  if(ratio > 0.25) return { label:'medium', ratio, max_capture:0.85, uplift_factor_cap:4.0 };
  return { label:'low', ratio, max_capture:0.95, uplift_factor_cap:5.5 };
}

function premiumStability(row){
  const score = num(row.premium_stability);
  if(score === null || score === undefined){
    return { score: 1.0, label: row.premium_stability_label || 'unknown' };
  }
  const label = row.premium_stability_label || (score >= 0.8 ? 'stable' : score >= 0.6 ? 'variable' : 'volatile');
  return { score, label };
}

function refiConfidence(compCount, dispersionPct, liquidityPerMonth, recencyDays){
  const cc = compCount || 0;
  let countScore = 20;
  if(cc >= 20) countScore = 100;
  else if(cc >= 15) countScore = 85;
  else if(cc >= 10) countScore = 75;
  else if(cc >= 5) countScore = 60;
  else if(cc >= 3) countScore = 45;
  else if(cc >= 1) countScore = 35;

  let dispersionScore = 65;
  if(dispersionPct !== null && dispersionPct !== undefined){
    if(dispersionPct <= 5) dispersionScore = 100;
    else if(dispersionPct <= 10) dispersionScore = 85;
    else if(dispersionPct <= 15) dispersionScore = 70;
    else if(dispersionPct <= 20) dispersionScore = 55;
    else if(dispersionPct <= 30) dispersionScore = 35;
    else dispersionScore = 25;
  }

  let liquidityScore = 50;
  if(liquidityPerMonth !== null && liquidityPerMonth !== undefined){
    if(liquidityPerMonth >= 40) liquidityScore = 100;
    else if(liquidityPerMonth >= 20) liquidityScore = 80;
    else if(liquidityPerMonth >= 10) liquidityScore = 65;
    else if(liquidityPerMonth >= 5) liquidityScore = 50;
    else if(liquidityPerMonth > 0) liquidityScore = 35;
    else liquidityScore = 25;
  }

  let recencyScore = 50;
  if(recencyDays !== null && recencyDays !== undefined){
    if(recencyDays <= 45) recencyScore = 100;
    else if(recencyDays <= 90) recencyScore = 80;
    else if(recencyDays <= 180) recencyScore = 65;
    else recencyScore = 45;
  }

  const weighted = 0.40 * countScore + 0.30 * dispersionScore + 0.20 * liquidityScore + 0.10 * recencyScore;
  return Math.max(20, Math.min(100, weighted));
}

function captureFromConfidence(score, compCount){
  let base;
  if(score >= 80) base = 0.95;
  else if(score >= 65) base = 0.90;
  else if(score >= 50) base = 0.85;
  else if(score >= 35) base = 0.80;
  else base = 0.75;
  if((compCount || 0) < 5) base = Math.max(base, 0.70);
  return base;
}

function dispersion4br(row){
  const disp = num(row.sale_dispersion_4br_pct);
  if(disp === null || disp === undefined) return null;
  return disp > 1 ? disp / 100 : disp;
}

function tightnessScore(dispersion){
  if(dispersion === null || dispersion === undefined) return 0.35;
  if(dispersion <= 0.12) return 1.0;
  if(dispersion <= 0.22){
    const span = 0.22 - 0.12;
    return Math.max(0.5, 1.0 - ((dispersion - 0.12) / span) * 0.5);
  }
  return Math.max(0.1, 0.5 - (dispersion - 0.22));
}

function plausibilityScore(m2, m3, m4){
  if(!m2 || !m3 || !m4) return 0.5;
  const gap23 = (m3 - m2) / m2;
  const gap34 = (m4 - m3) / m3;
  const ratio = gap34 / Math.max(gap23, 0.01);
  let score;
  if(ratio >= 0.8 && ratio <= 1.6) score = 1.0;
  else if((ratio >= 0.5 && ratio < 0.8) || (ratio > 1.6 && ratio <= 2.2)) score = 0.7;
  else score = 0.4;
  if(gap34 > 0.50 && gap23 < 0.20) score = Math.min(score, 0.4);
  return Math.max(0, Math.min(1, score));
}

function landProxyScore(m3House, m3Unit){
  if(!m3House || !m3Unit) return 0.5;
  const ratio = m3Unit / m3House;
  return Math.max(0, Math.min(1, 1 - Math.max(0, Math.min(1, ratio))));
}

function valuationSafetyScore(refiConf, tightness, plausibility, landProxy){
  const safety01 = 0.35 * Math.max(0, Math.min(1, (refiConf || 0) / 100))
    + 0.30 * Math.max(0, Math.min(1, tightness || 0))
    + 0.25 * Math.max(0, Math.min(1, plausibility || 0))
    + 0.10 * Math.max(0, Math.min(1, landProxy || 0));
  return Math.max(0, Math.min(100, safety01 * 100));
}

function safetyLabelFromTightness(t){
  if(t >= 0.80) return 'tight';
  if(t >= 0.40) return 'medium';
  return 'loose';
}

function rentSupportScore(expectedBankValue, rentPw4){
  if(!expectedBankValue || !rentPw4) return 50;
  const yieldProxy = (rentPw4 * 52) / expectedBankValue;
  let yieldScore;
  if(yieldProxy <= 0.025) yieldScore = 0;
  else if(yieldProxy >= 0.055) yieldScore = 1;
  else yieldScore = (yieldProxy - 0.025) / (0.055 - 0.025);
  const trendScore = 0.5; // until history is wired
  const support01 = 0.5 * Math.max(0, Math.min(1, yieldScore)) + 0.5 * Math.max(0, Math.min(1, trendScore));
  return Math.max(0, Math.min(100, support01 * 100));
}

function valuationMode(segRatio, dispersion, compCount){
  const hasComps = (compCount || 0) > 0;
  if(segRatio === null || segRatio === undefined) return 'segmented';
  if(dispersion === null || dispersion === undefined || !hasComps) return 'segmented';
  if(segRatio <= 0.40 && dispersion <= 0.25) return 'homogeneous';
  return 'segmented';
}

function stampDutyVic(amount){
  if(!amount || amount <= 0) return 0;
  const bands = [ {cap:250000, rate:0.0114}, {cap:960000, rate:0.024}, {cap:2000000, rate:0.06} ];
  let duty = 0; let prev = 0;
  for(const band of bands){
    if(amount <= band.cap){ duty += (amount - prev) * band.rate; return duty; }
    duty += (band.cap - prev) * band.rate; prev = band.cap;
  }
  duty += (amount - prev) * bands[bands.length-1].rate;
  return duty;
}

function computeCosts(purchasePrice, inputs, rentPw){
  const contingency = inputs.renoCost * (inputs.contingencyPct / 100);
  const stamp = stampDutyVic(purchasePrice);
  const renoTotal = inputs.renoCost + contingency;
  const depositCash = purchasePrice * (inputs.depositPct / 100);
  const purchaseDebt = purchasePrice - depositCash;
  const renoDebt = inputs.renoFunding === 'borrowed' ? renoTotal : 0;
  const renoCash = renoDebt > 0 ? 0 : renoTotal;
  const interestBase = purchaseDebt + renoDebt;
  const interest = interestBase * (inputs.interestRate / 100) * (inputs.durationWeeks / 52);
  const vacancy = inputs.vacancyWeeks !== null && inputs.vacancyWeeks !== undefined ? inputs.vacancyWeeks : inputs.durationWeeks;
  const rentWeeks = Math.max(0, inputs.durationWeeks - vacancy);
  const rentIncome = inputs.includeRent && rentPw ? rentPw * rentWeeks : 0;
  const cashIn = Math.max(0, depositCash + stamp + inputs.legalFees + inputs.miscCosts + renoCash + interest - rentIncome);
  const totalDebt = purchaseDebt + renoDebt;
  const allIn = purchasePrice + stamp + inputs.legalFees + inputs.miscCosts + renoTotal + interest;
  return { contingency, stamp, interest, renoTotal, totalDebt, allIn, depositCash, purchaseDebt, renoDebt, renoCash, rentIncome, cashIn };
}

function bankValuation(params){
  if(!params.fourBedMedian) return null;
  const conf01 = clamp01((params.confidenceScore || 0) / 100);
  const costFloor = params.purchasePrice + params.renoCost * 0.60;
  const threeFloor = params.threeBedMedian ? params.threeBedMedian * 0.95 : costFloor;
  const lower = Math.max(costFloor, threeFloor);

  const captureSuggested = captureFromConfidence(params.confidenceScore || 0, params.compCount || 0);
  const baseCapture = Math.min(captureSuggested, params.maxCapture);
  let capturePre = baseCapture;
  if(params.weightedComps){
    const compCapture = params.weightedComps / params.fourBedMedian;
    const blendWeight = Math.min(1, (params.compCount || 0) / 15);
    const blendedCapture = (baseCapture * (1 - blendWeight)) + (compCapture * blendWeight);
    capturePre = Math.min(params.maxCapture, Math.max(0.40, blendedCapture));
  }

  const captureDiscount = params.captureDiscount !== undefined ? params.captureDiscount : 1;
  const effectiveCapture = capturePre * Math.max(0, Math.min(1, captureDiscount));
  let upper = params.fourBedMedian * effectiveCapture;

  const mode = params.valuationMode || 'segmented';
  let expected;
  if(mode === 'homogeneous'){
    const marketTarget = upper;
    expected = lower + conf01 * (marketTarget - lower);
    expected = Math.min(expected, params.fourBedMedian * 0.98);
    if((params.compCount12m || 0) >= 8 && params.p75Comps12m){
      expected = Math.min(expected, params.p75Comps12m);
    }
    if(!params.marketDeclineFlag && expected < params.purchasePrice){ expected = params.purchasePrice; }
  } else {
    expected = lower + conf01 * (upper - lower);
    if(!params.marketDeclineFlag && expected < params.purchasePrice){ expected = params.purchasePrice; }
    const factor = params.upliftCap * (0.70 + 0.60 * conf01);
    const maxUplift = params.renoCost * factor;
    expected = Math.min(expected, params.purchasePrice + maxUplift);
  }

  const capture = params.fourBedMedian ? expected / params.fourBedMedian : null;
  return { expected, capture, effective_capture: effectiveCapture, base_capture: baseCapture, capture_discount: captureDiscount, lower, upper };
}

function dealScore(manufacturedEq, usableEq, renoCost, confidenceScore, stabilityScore, valuationMode){
  if(!renoCost || renoCost <= 0) return 0;
  const roi = manufacturedEq / renoCost;
  const roiScore = Math.min(Math.max((roi - 0.8) / 2.5, 0), 1);
  const usableScore = usableEq > 0 ? 1 : 0.2;
  const confScore = clamp01((confidenceScore || 0) / 100);
  const stability = clamp01(stabilityScore !== null && stabilityScore !== undefined ? stabilityScore : 0.6);
  const equityScore = 0.50 * roiScore + 0.20 * usableScore + 0.30 * stability;
  let final = 0.60 * equityScore + 0.40 * confScore;
  if(valuationMode === 'segmented'){ final *= 0.85; }
  if(manufacturedEq < 0 && final > 0.60){ final = 0.60; }
  return Math.max(0, Math.min(1, final));
}

function recycleMetrics(cashIn, usableSelected){
  const recovered = Math.max(0, usableSelected || 0);
  const tiedUp = Math.max(0, (cashIn || 0) - recovered);
  const ratio = cashIn && cashIn > 0 ? recovered / cashIn : 0;
  return { cash_recovered: recovered, cash_tied_up: tiedUp, recycle_ratio: ratio };
}

function repeatabilityScore(recycleRatio, confidenceScore, stabilityScore, valuationMode, segmentationLabel, manufacturedEq, rentSupportScore){
  let base = 50 * clamp01((recycleRatio || 0) / 0.85)
    + 20 * clamp01((confidenceScore || 0) / 100)
    + 20 * clamp01(stabilityScore || 0)
    + 10 * clamp01((rentSupportScore || 0) / 100);
  if(valuationMode === 'segmented') base *= 0.90;
  if(segmentationLabel === 'prestige') base *= 0.70;
  if(manufacturedEq < 0 && (recycleRatio || 0) < 0.50 && base > 55){ base = 55; }
  return Math.max(0, Math.min(100, base));
}

function getInputs(){
  return {
    purchaseDiscount: num(document.getElementById('purchaseDiscount').value) || DEFAULTS.purchaseDiscount,
    depositPct: num(document.getElementById('depositPct').value) || DEFAULTS.depositPct,
    renoCost: num(document.getElementById('renoCost').value) || DEFAULTS.renoCost,
    contingencyPct: num(document.getElementById('contingencyPct').value) || DEFAULTS.contingencyPct,
    durationWeeks: num(document.getElementById('durationWeeks').value) || DEFAULTS.durationWeeks,
    interestRate: num(document.getElementById('interestRate').value) || DEFAULTS.interestRate,
    legalFees: num(document.getElementById('legalFees').value) || DEFAULTS.legalFees,
    miscCosts: num(document.getElementById('miscCosts').value) || DEFAULTS.miscCosts,
    renoFunding: document.getElementById('renoFunding').value || DEFAULTS.renoFunding,
    refiLvr: parseFloat(document.getElementById('refiLvr').value) || DEFAULTS.refiLvr,
    includeRent: (document.getElementById('includeRent').value || 'off') === 'on',
    vacancyWeeks: num(document.getElementById('vacancyWeeks').value),
  };
}

function computeDeal(row){
  const sale3 = num(row.sale_3br_house);
  const sale4 = num(row.sale_4br_house);
  if(!sale3 || !sale4) return null;
  const inputs = getInputs();
  const purchasePrice = sale3 * (1 - inputs.purchaseDiscount / 100);
  const seg = segment(row);
  const disp = dispersion4br(row);
  const tightness = tightnessScore(disp);
  const plaus = plausibilityScore(num(row.sale_2br_house), sale3, sale4);
  const landProxy = landProxyScore(sale3, num(row.sale_3br_unit));
  const valMode = valuationMode(seg.ratio, disp, num(row.comp_count_4br));
  const stability = premiumStability(row);
  const costs = computeCosts(purchasePrice, inputs, num(row.rent_3br_house));
  const confidence = refiConfidence(num(row.comp_count_4br), num(row.sale_dispersion_4br_pct), num(row.sales_per_month), row.data_freshness_days);
  const safetyScore = valuationSafetyScore(confidence, tightness, plaus, landProxy);
  const captureDiscount = Math.max(0, Math.min(1, 0.55 + 0.45 * (safetyScore / 100)));
  const valuation = bankValuation({
    purchasePrice,
    renoCost: inputs.renoCost,
    threeBedMedian: sale3,
    fourBedMedian: sale4,
    confidenceScore: confidence,
    weightedComps: num(row.weighted_median_4br_comps),
    compCount: num(row.comp_count_4br) || 0,
    maxCapture: seg.max_capture,
    upliftCap: seg.uplift_factor_cap,
    valuationMode: valMode,
    compCount12m: num(row.comp_count_4br) || 0,
    p75Comps12m: num(row.p75_4br_comps_12m),
    marketDeclineFlag: false,
    captureDiscount,
  });
  if(!valuation) return null;

  const rentSupport = rentSupportScore(valuation.expected, num(row.rent_4br_house));

  const manufacturedEq = valuation.expected - costs.allIn;
  const usable80 = valuation.expected * 0.8 - costs.totalDebt;
  const usable90 = valuation.expected * 0.9 - costs.totalDebt;
  const usableSelected = valuation.expected * (inputs.refiLvr || 0.8) - costs.totalDebt;
  const recycle = recycleMetrics(costs.cashIn, usableSelected);
  const repeat = repeatabilityScore(recycle.recycle_ratio, confidence, stability.score, valMode, seg.label, manufacturedEq, rentSupport);
  const score = dealScore(manufacturedEq, usableSelected, inputs.renoCost, confidence, stability.score, valMode);

  const captureableGapPct = Math.min(valuation.effective_capture ?? valuation.capture ?? 0, seg.max_capture);
  const captureableUpside = manufacturedEq * captureDiscount;
  const netHoldingCost = costs.interest - costs.rentIncome;
  const safetyLabel = safetyLabelFromTightness(tightness);

  return {
    purchase_price: purchasePrice,
    stamp_duty: costs.stamp,
    contingency: costs.contingency,
    interest: costs.interest,
    all_in_cost: costs.allIn,
    misc_costs: inputs.miscCosts,
    deposit_cash: costs.depositCash,
    reno_cash: costs.renoCash,
    rent_income_during_reno: costs.rentIncome,
    net_holding_cost: netHoldingCost,
    cash_in: costs.cashIn,
    total_debt_at_refi: costs.totalDebt,
    expected_bank_value: valuation.expected,
    manufactured_equity: manufacturedEq,
    usable_equity_80: usable80,
    usable_equity_90: usable90,
    usable_equity_selected_lvr: usableSelected,
    refi_confidence_score: confidence,
    capture_pct: valuation.capture,
    effective_capture: valuation.effective_capture,
    base_capture: valuation.base_capture,
    capture_discount: captureDiscount,
    captureable_gap_pct: captureableGapPct,
    captureable_upside: captureableUpside,
    valuation_safety_score: safetyScore,
    valuation_safety_label: safetyLabel,
    tightness_score: tightness,
    plausibility_score: plaus,
    land_proxy_score: landProxy,
    rent_support_score: rentSupport,
    deal_score: score * 100,
    repeatability_score: repeat,
    premium_stability: stability.score,
    premium_stability_label: stability.label,
    segmentation: seg.label,
    segmentation_ratio: seg.ratio,
    valuation_mode: valMode,
    dispersion_4br: disp,
    max_capture: seg.max_capture,
    uplift_factor_cap: seg.uplift_factor_cap,
    valuation_lower: valuation.lower,
    valuation_upper: valuation.upper,
    cash_recovered: recycle.cash_recovered,
    cash_tied_up: recycle.cash_tied_up,
    recycle_ratio: recycle.recycle_ratio,
  };
}

function dealRow(row){ const calc = computeDeal(row); if(!calc) return null; return Object.assign({}, row, calc); }

function applyPreset(){
  const name = document.getElementById('preset').value;
  const p = presets[name] || DEFAULTS;
  document.getElementById('purchaseDiscount').value = p.purchaseDiscount;
  document.getElementById('renoCost').value = p.renoCost;
  document.getElementById('contingencyPct').value = p.contingencyPct;
  document.getElementById('durationWeeks').value = p.durationWeeks;
  document.getElementById('interestRate').value = p.interestRate;
  document.getElementById('legalFees').value = p.legalFees;
  render();
}

function filterRows(){
  const search = (document.getElementById('search').value || '').toLowerCase();
  const state = document.getElementById('state').value;
  const minComps = num(document.getElementById('minComps').value) || 0;
  return data
    .map(dealRow)
    .filter(Boolean)
    .filter(r => {
      if(search && !(r.suburb || '').toLowerCase().includes(search)) return false;
      if(state && r.state !== state) return false;
      if(minComps > 0 && (r.comp_count_4br || 0) < minComps) return false;
      return true;
    });
}

function sortRows(rows){
  return rows.sort((a,b)=>{
    const av = a[sortKey];
    const bv = b[sortKey];
    if(av === undefined && bv === undefined) return 0;
    if(av === undefined) return 1;
    if(bv === undefined) return -1;
    return sortDir === 'desc' ? bv - av : av - bv;
  });
}

function render(){
  filtered = sortRows(filterRows());
  const body = document.getElementById('rows');
  body.innerHTML = '';
  filtered.forEach(r => {
    const tr = document.createElement('tr');
    tr.onclick = () => renderDetail(r);
    const cells = [
      {val:r.suburb},
      {val:r.state},
      {val:r.postcode},
      {val:money(r.sale_3br_house)},
      {val:money(r.sale_4br_house)},
      {val:money(r.price_delta_3_to_4)},
      {val:pct(r.pct_delta_3_to_4)},
      {val:r.premium_stability, fmt: v => v !== undefined && v !== null ? v.toFixed(2) : ''},
      {val:money(r.all_in_cost)},
      {val:money(r.expected_bank_value)},
      {val:money(r.manufactured_equity), cls: r.manufactured_equity < 0 ? 'score bad' : 'score good'},
      {val:money(r.usable_equity_80), cls: r.usable_equity_80 < 0 ? 'score bad' : 'score good'},
      {val:r.refi_confidence_score, fmt: v => pct(v)},
      {val:r.capture_pct ? r.capture_pct * 100 : null, fmt: pct},
      {val:r.valuation_safety_score, fmt: v => pct(v), cls: (r.valuation_safety_score || 0) >= 70 ? 'score good' : (r.valuation_safety_score || 0) >= 45 ? 'score mid' : 'score bad'},
      {val:r.capture_discount ? r.capture_discount * 100 : null, fmt: pct},
      {val:r.captureable_gap_pct ? r.captureable_gap_pct * 100 : null, fmt: pct},
      {val:money(r.captureable_upside)},
      {val:r.rent_support_score, fmt: v => pct(v)},
      {val:money(r.total_debt_at_refi)},
      {val:money(r.cash_in)},
      {val:money(r.net_holding_cost)},
      {val:money(r.usable_equity_selected_lvr), cls: r.usable_equity_selected_lvr < 0 ? 'score bad' : 'score good'},
      {val:money(r.cash_recovered)},
      {val:(r.recycle_ratio !== undefined && r.recycle_ratio !== null) ? r.recycle_ratio * 100 : null, fmt: pct},
      {val:r.repeatability_score, fmt: v => pct(v), cls: r.repeatability_score >= 65 ? 'score good' : r.repeatability_score >= 45 ? 'score mid' : 'score bad'},
      {val:r.deal_score, fmt: pct, cls: r.deal_score >= 65 ? 'score good' : r.deal_score >= 45 ? 'score mid' : 'score bad'},
    ];
    cells.forEach(c => {
      const td = document.createElement('td');
      if(c.cls) td.className = c.cls;
      if(c.fmt) td.textContent = c.fmt(c.val);
      else td.textContent = c.val === undefined || c.val === null ? '' : c.val;
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });
  document.getElementById('count').textContent = `${filtered.length} suburbs (sorted by ${sortKey}, ${sortDir})`;
  if(filtered.length > 0){ renderDetail(filtered[0]); }
  else { document.getElementById('detail').innerHTML = '<div class="small">No rows match filters</div>'; }
}

function renderDetail(row){
  const div = document.getElementById('detail');
  const stability = premiumStability(row);
  const seg = segment(row);
  const disp = row.dispersion_4br !== undefined ? row.dispersion_4br : dispersion4br(row);
  const valMode = row.valuation_mode || valuationMode(seg.ratio, disp, row.comp_count_4br);
  const inputs = getInputs();
  div.innerHTML = `
    <div class="card">
      <h3 style="margin:4px 0 6px;">${row.suburb} ${row.postcode} (${row.state})</h3>
      <div class="small">3→4 premium ${pct(row.pct_delta_3_to_4)} | stability ${stability.score.toFixed(2)} (${stability.label}) | segmentation ${seg.label} | val mode ${valMode}</div>
      <div class="small">Seg ratio ${seg.ratio !== null ? pct(seg.ratio * 100) : ''} | Dispersion ${disp !== null && disp !== undefined ? pct(disp * 100) : ''}</div>
      <div class="small">All-in ${money(row.all_in_cost)} | Expected bank ${money(row.expected_bank_value)} | Bounds ${money(row.valuation_lower)} → ${money(row.valuation_upper)}</div>
      <div class="small">Safety ${row.valuation_safety_score ? pct(row.valuation_safety_score) : ''} (${row.valuation_safety_label || ''}) | Tightness ${row.tightness_score !== undefined && row.tightness_score !== null ? pct(row.tightness_score * 100) : ''} | Plausibility ${row.plausibility_score !== undefined && row.plausibility_score !== null ? pct(row.plausibility_score * 100) : ''} | Land proxy ${row.land_proxy_score !== undefined && row.land_proxy_score !== null ? pct(row.land_proxy_score * 100) : ''} | Capture discount ${row.capture_discount ? pct(row.capture_discount * 100) : ''}</div>
      <div class="small">Manufactured ${money(row.manufactured_equity)} | Usable @${(inputs.refiLvr || 0.8)*100}% ${money(row.usable_equity_selected_lvr)} | Usable @80% ${money(row.usable_equity_80)} | @90% ${money(row.usable_equity_90)} | Capture ${row.capture_pct ? pct(row.capture_pct * 100) : ''}</div>
      <div class="small">Captureable gap ${row.captureable_gap_pct ? pct(row.captureable_gap_pct * 100) : ''} | Captureable upside ${money(row.captureable_upside)} | Rent support ${row.rent_support_score ? pct(row.rent_support_score) : ''} | Net holding ${money(row.net_holding_cost)}</div>
      <div class="small">Cash in ${money(row.cash_in)} | Cash recovered ${money(row.cash_recovered)} | Recycle ratio ${row.recycle_ratio ? pct(row.recycle_ratio * 100) : ''} | Cash tied ${money(row.cash_tied_up)} | Debt @refi ${money(row.total_debt_at_refi)}</div>
      <div class="small">Refi confidence ${row.refi_confidence_score ? pct(row.refi_confidence_score) : ''} | Repeatability ${row.repeatability_score ? pct(row.repeatability_score) : ''} | 4BR comps ${(row.comp_count_4br || 0)} | Dispersion ${disp !== null && disp !== undefined ? pct(disp * 100) : ''}</div>
      <div class="small">Assumptions: discount ${inputs.purchaseDiscount}% | deposit ${inputs.depositPct}% | LVR ${inputs.refiLvr*100}% | reno ${money(inputs.renoCost)} | contingency ${inputs.contingencyPct}% | duration ${inputs.durationWeeks} w | rate ${inputs.interestRate}% | legals ${money(inputs.legalFees)} | misc ${money(inputs.miscCosts)} | funding ${inputs.renoFunding} | rent ${inputs.includeRent ? 'on' : 'off'} (vacancy ${inputs.vacancyWeeks ?? inputs.durationWeeks} w)</div>
      <div class="small">As of ${row.as_of_date || 'n/a'} | Freshness ${row.data_freshness_label}</div>
    </div>`;
}

function bindControls(){
  ['search','state','purchaseDiscount','depositPct','renoCost','contingencyPct','durationWeeks','interestRate','legalFees','miscCosts','renoFunding','refiLvr','includeRent','vacancyWeeks','sortKey','minComps'].forEach(id => {
    const el = document.getElementById(id);
    el.addEventListener('input', render);
    el.addEventListener('change', render);
  });
  document.getElementById('applyPreset').onclick = applyPreset;
  document.querySelectorAll('th[data-key]').forEach(th => {
    th.onclick = () => {
      const key = th.getAttribute('data-key');
      if(sortKey === key){ sortDir = sortDir === 'desc' ? 'asc' : 'desc'; }
      else { sortKey = key; sortDir = 'desc'; }
      document.getElementById('sortKey').value = sortKey;
      render();
    };
  });
}

fetch('data.json')
  .then(r => r.json())
  .then(json => {
    data = json;
    bindControls();
    applyPreset();
  })
  .catch(err => {
    document.getElementById('count').textContent = 'Failed to load data';
    console.error(err);
  });
</script>
</body>
</html>
"""


def main():
  ap = argparse.ArgumentParser(description="Build static medians + manufactured equity explorer")
  ap.add_argument("--csv", required=True, help="Input enriched CSV (e.g. suburbs_enriched.csv)")
  ap.add_argument("--outdir", default="site", help="Output directory (default: site)")
  args = ap.parse_args()

  rows = enrich_rows(load_csv(args.csv))
  os.makedirs(args.outdir, exist_ok=True)

  with open(os.path.join(args.outdir, "data.json"), "w", encoding="utf-8") as f:
    json.dump(rows, f, ensure_ascii=False)

  with open(os.path.join(args.outdir, "index.html"), "w", encoding="utf-8") as f:
    f.write(build_main_html())

  with open(os.path.join(args.outdir, "manufactured-equity.html"), "w", encoding="utf-8") as f:
    f.write(build_manufactured_html())

  print(f"Wrote {len(rows)} rows to {args.outdir}/data.json and built index + manufactured-equity pages")


if __name__ == "__main__":
    main()
