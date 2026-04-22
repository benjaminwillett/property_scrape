#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import os
import random
import re
import time
from typing import Dict, Optional, List, Set, Tuple
import argparse

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PWTimeoutError,
    Error as PWError,
)

STATE_PATH = {
    "ACT": "act",
    "NSW": "nsw",
    "NT": "nt",
    "QLD": "qld",
    "SA": "sa",
    "TAS": "tas",
    "VIC": "vic",
    "WA": "wa",
}

NAV_TIMEOUT_MS = 60_000
POST_NAV_WAIT_MS = 900

MAX_CONSEC_ERRORS = 8

# 429 handling (hard backoff)
MAX_429_RETRIES_PER_SUBURB = 3
BACKOFF_429_SECONDS = [120, 300, 600]  # 2m, 5m, 10m

DEBUG_DIR = "debug_out"
SAVE_SCREENSHOT_ON_FAIL = True
SAVE_HTML_ON_FAIL = True
DEBUG_ON_404 = False

CHECKPOINT_EVERY = 25
FILTER_CONSOLE_ERR_FAILED = True

# Crawl-state / blacklist
MAX_FAILS_BEFORE_BLACKLIST = 3

SALE_KEYS = (
    "median_2br_house_price_aud",
    "median_3br_house_price_aud",
    "median_4br_house_price_aud",
    "median_2br_unit_price_aud",
    "median_3br_unit_price_aud",
    "median_4br_unit_price_aud",
)

REIV_RENT_KEYS = (
    "rent_2br_house_pw_aud",
    "rent_3br_house_pw_aud",
    "rent_4br_house_pw_aud",
    "rent_2br_unit_pw_aud",
    "rent_3br_unit_pw_aud",
    "rent_4br_unit_pw_aud",
)

STATE_KEYS = (
    "scrape_status",
    "last_attempt",
    "fail_count",
    "blacklisted",
)

# -----------------------------
# Utility helpers
# -----------------------------
def parse_csv_list(arg: Optional[str]) -> Optional[Set[str]]:
    if not arg:
        return None
    parts = [p.strip() for p in arg.split(",") if p.strip()]
    return {p.upper() for p in parts} if parts else None


def slugify(name: str) -> str:
    s = name.strip().lower()
    s = s.replace("’", "").replace("'", "")
    s = re.sub(r"[^a-z0-9\s-]", " ", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def build_sales_url(site: str, state: str, suburb: str, postcode: str) -> str:
    state = state.upper()
    if site == "rea":
        st = STATE_PATH[state]
        return f"https://www.realestate.com.au/{st}/{slugify(suburb)}-{postcode}/"
    if site == "domain":
        return f"https://www.domain.com.au/suburb-profile/{slugify(suburb)}-{state.lower()}-{postcode}"
    raise ValueError(f"Unknown site: {site}")


def build_reiv_url(suburb: str) -> str:
    return f"https://reiv.com.au/market-insights/suburb/{slugify(suburb)}"


def parse_price(text: str) -> Optional[int]:
    digits = re.sub(r"[^\d]", "", text or "")
    return int(digits) if digits else None


def fmt_price(p: Optional[int]) -> str:
    return f"${p:,.0f}" if isinstance(p, int) else "None"


def safe_fname(state: str, suburb: str, postcode: str) -> str:
    base = f"{state}_{postcode}_{slugify(suburb)}"
    return re.sub(r"[^a-zA-Z0-9_\-]", "_", base)


def row_key(r: dict) -> Tuple[str, str, str]:
    return (
        (r.get("postcode") or "").strip(),
        (r.get("state") or "").strip().upper(),
        (r.get("suburb") or "").strip(),
    )


def load_csv(path: str) -> Tuple[List[str], List[dict]]:
    if not os.path.exists(path):
        return [], []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return (reader.fieldnames or []), list(reader)


def ensure_columns(fieldnames: List[str], rows: List[dict]) -> List[str]:
    required = [
        *SALE_KEYS,
        *REIV_RENT_KEYS,
        "as_of_date",
        "sales_data_source",
        "reiv_rent_source",
        "last_error",
        # crawl-state
        *STATE_KEYS,
    ]
    existing = set(fieldnames or [])
    out = list(fieldnames or [])
    for col in required:
        if col not in existing:
            out.append(col)
            for r in rows:
                r[col] = ""
    # normalize defaults
    for r in rows:
        if (r.get("scrape_status") or "").strip() == "":
            r["scrape_status"] = "never"
        if (r.get("fail_count") or "").strip() == "":
            r["fail_count"] = "0"
        if (r.get("blacklisted") or "").strip() == "":
            r["blacklisted"] = "0"
    return out


def merge_progress(base_rows: List[dict], progress_rows: List[dict]) -> List[dict]:
    pmap = {row_key(r): r for r in progress_rows}
    return [pmap.get(row_key(r), r) for r in base_rows]


def write_csv(path: str, fieldnames: List[str], rows: List[dict]):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"[SAVE] {path}")


def dump_debug(page, state: str, suburb: str, postcode: str, note: str):
    os.makedirs(DEBUG_DIR, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"{safe_fname(state, suburb, postcode)}_{stamp}"

    print(f"[DEBUG] {note}")

    if SAVE_SCREENSHOT_ON_FAIL:
        try:
            shot_path = os.path.join(DEBUG_DIR, f"{base}.png")
            page.screenshot(path=shot_path, full_page=True)
            print(f"[DEBUG] Screenshot saved: {shot_path}")
        except Exception as e:
            print(f"[DEBUG] Screenshot failed: {e}")

    if SAVE_HTML_ON_FAIL:
        try:
            html_path = os.path.join(DEBUG_DIR, f"{base}.html")
            content = page.content()
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"[DEBUG] HTML saved: {html_path}")
        except Exception as e:
            print(f"[DEBUG] HTML save failed: {e}")


def is_blank(v: object) -> bool:
    if v is None:
        return True
    return str(v).strip() == ""


def has_any_sales(row: dict) -> bool:
    return any(not is_blank(row.get(k)) for k in SALE_KEYS)


def has_any_reiv_rent(row: dict) -> bool:
    return any(not is_blank(row.get(k)) for k in REIV_RENT_KEYS)


def get_status(row: dict) -> str:
    return (row.get("scrape_status") or "never").strip().lower()


def is_blacklisted(row: dict) -> bool:
    return str(row.get("blacklisted") or "0").strip() == "1"


def inc_fail(row: dict):
    try:
        fc = int(str(row.get("fail_count") or "0").strip() or "0")
    except Exception:
        fc = 0
    fc += 1
    row["fail_count"] = str(fc)
    if fc >= MAX_FAILS_BEFORE_BLACKLIST:
        row["blacklisted"] = "1"


def reset_fail(row: dict):
    row["fail_count"] = "0"
    # do not auto-unblacklist


# -----------------------------
# Mode logic
# -----------------------------
def should_process_row(row: dict, mode: str, retry_hard_fails: bool, ignore_blacklist: bool) -> bool:
    """
    mode:
      fresh   = scrape everything (respect blacklist unless ignore_blacklist)
      missing = only attempt rows that are missing data, BUT skip known-hard-fails (404/nodata) unless retry_hard_fails
      new     = only attempt rows never queried before
      failed  = retry partial/error only (optionally also 404/nodata with retry_hard_fails)
    """
    if is_blacklisted(row) and not ignore_blacklist:
        return False

    status = get_status(row)

    if mode == "new":
        return status == "never"

    if mode == "fresh":
        return True

    if mode == "failed":
        if status in ("partial", "error"):
            return True
        if retry_hard_fails and status in ("404", "nodata"):
            return True
        return False

    # mode == "missing"
    # treat never as eligible (haven't been tried)
    if status == "never":
        return True

    if status in ("404", "nodata") and not retry_hard_fails:
        return False

    # eligible if missing any target data (caller decides want_sales/want_rent)
    return True


# -----------------------------
# Polite pacing (jitter + occasional breaks)
# -----------------------------
def polite_delay(kind: str, idx: int, consecutive_errors: int):
    """
    Load-friendly pacing strategy:
      - random jitter
      - occasional longer pauses
      - bigger cooldown if errors stack
    """
    if kind == "between_suburbs":
        base = random.uniform(6.5, 14.0)
        if idx % random.randint(18, 25) == 0:
            base += random.uniform(25, 60)
        if idx % random.randint(110, 140) == 0:
            base += random.uniform(120, 240)
    else:
        base = random.uniform(2.0, 6.0)

    if consecutive_errors >= 3:
        base += random.uniform(20, 60)

    print(f"[WAIT] {kind}: sleeping {base:.1f}s")
    time.sleep(base)


def cooldown_if_needed(consecutive_errors: int) -> int:
    if consecutive_errors >= 5:
        cd = random.uniform(180, 420)
        print(f"[COOLDOWN] too many errors — sleeping {cd/60:.1f} minutes")
        time.sleep(cd)
        return 0
    return consecutive_errors


# -----------------------------
# Sales extractors (Domain/REA)
# -----------------------------
DOMAIN_ROW_RE = re.compile(r"^\s*(?P<bed>[2-4])\s+(?P<type>House|Unit)\s+(?P<median>\$[0-9.,]+[kKmM]?)\b")


def parse_domain_median(text: str) -> Optional[int]:
    if not text:
        return None
    t = text.strip().lower().replace("$", "").replace(",", "")
    m = re.match(r"^([0-9]*\.?[0-9]+)\s*([km])?$", t)
    if not m:
        return None
    val = float(m.group(1))
    suf = m.group(2)
    if suf == "k":
        return int(round(val * 1_000))
    if suf == "m":
        return int(round(val * 1_000_000))
    return int(round(val))


def extract_domain_medians_house_unit(page) -> Tuple[Dict[int, Optional[int]], Dict[int, Optional[int]]]:
    house: Dict[int, Optional[int]] = {2: None, 3: None, 4: None}
    unit: Dict[int, Optional[int]] = {2: None, 3: None, 4: None}

    page.get_by_text("Market trends", exact=False).first.wait_for(timeout=25_000)

    body = page.locator("body").inner_text()
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]

    for ln in lines:
        m = DOMAIN_ROW_RE.match(ln)
        if not m:
            continue
        bed = int(m.group("bed"))
        typ = m.group("type").lower()
        price = parse_domain_median(m.group("median"))
        if typ == "house":
            house[bed] = price
        else:
            unit[bed] = price

    return house, unit


def extract_rea_medians_house_unit(page) -> Tuple[Dict[int, Optional[int]], Dict[int, Optional[int]]]:
    house: Dict[int, Optional[int]] = {2: None, 3: None, 4: None}
    unit: Dict[int, Optional[int]] = {2: None, 3: None, 4: None}

    section = page.locator("xpath=//section[.//*[contains(., 'Median price snapshot')]]").first
    section.wait_for(state="visible", timeout=25_000)
    table = section.locator("xpath=.//table").first
    table.wait_for(state="visible", timeout=25_000)

    rows = table.locator("xpath=.//tbody/tr")
    for i in range(rows.count()):
        row = rows.nth(i)
        label = (row.locator("xpath=.//td[1]").inner_text() or "").strip().lower()
        value_text = (row.locator("xpath=.//td[2]//span").inner_text() or "").strip()
        price = parse_price(value_text)

        if "bed house" in label:
            if label.startswith("2 "):
                house[2] = price
            elif label.startswith("3 "):
                house[3] = price
            elif label.startswith("4 "):
                house[4] = price
        elif "bed unit" in label:
            if label.startswith("2 "):
                unit[2] = price
            elif label.startswith("3 "):
                unit[3] = price
            elif label.startswith("4 "):
                unit[4] = price

    return house, unit


# -----------------------------
# REIV rent extraction (from body text)
# -----------------------------
def _dismiss_common_overlays(page):
    candidates = [
        page.get_by_role("button", name=re.compile(r"accept|agree|got it|close", re.I)).first,
        page.locator("button:has-text('Accept')").first,
        page.locator("button:has-text('I agree')").first,
        page.locator("button:has-text('Close')").first,
    ]
    for loc in candidates:
        try:
            if loc.is_visible():
                loc.click(timeout=1500)
                page.wait_for_timeout(250)
        except Exception:
            continue


def _try_click_tab(page, label: str):
    locs = [
        page.get_by_role("tab", name=re.compile(label, re.I)).first,
        page.get_by_role("button", name=re.compile(label, re.I)).first,
        page.locator(f"button:has-text('{label}')").first,
        page.get_by_text(label, exact=True).first,
        page.get_by_text(label, exact=False).first,
    ]
    for loc in locs:
        try:
            if loc.count() == 0:
                continue
            if loc.is_visible():
                loc.click(timeout=2000)
                page.wait_for_timeout(500)
                return
        except Exception:
            continue


def _slice_rental_block(body_text: str) -> List[str]:
    lines = [ln.strip() for ln in body_text.splitlines() if ln.strip()]
    start = None
    for i, ln in enumerate(lines):
        if ln.upper() == "RENTAL DATA" or "RENTAL DATA" in ln.upper():
            start = i
            break
    if start is None:
        return []

    window = lines[start : start + 180]
    stop_words = ("Median data for the current quarter", "Median sale price", "Dwelling types", "Find a REIV member")
    out = []
    for ln in window:
        if any(sw.lower() in ln.lower() for sw in stop_words):
            break
        out.append(ln)
    return out


def _parse_reiv_bedroom_rows(rental_lines: List[str]) -> Dict[int, Optional[int]]:
    rents = {2: None, 3: None, 4: None}

    for ln in rental_lines:
        m = re.match(r"^([2-4])\s+(.*)$", ln)
        if not m:
            continue
        bed = int(m.group(1))
        if bed not in rents:
            continue

        toks = ln.split()
        if len(toks) < 2:
            continue

        suburb_val = toks[1].strip()
        if suburb_val in ("-", "—"):
            rents[bed] = None
        else:
            rents[bed] = parse_price(suburb_val)

    return rents


def extract_reiv_rents_variant(page, state: str, suburb: str, postcode: str, variant: str) -> Dict[int, Optional[int]]:
    _dismiss_common_overlays(page)
    try:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.65);")
        page.wait_for_timeout(700)
    except Exception:
        pass

    body_text = page.locator("body").inner_text()
    rental_lines = _slice_rental_block(body_text)

    os.makedirs(DEBUG_DIR, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    fn = f"{safe_fname(state, suburb, postcode)}_{stamp}_reiv_rental_{variant}.txt"
    with open(os.path.join(DEBUG_DIR, fn), "w", encoding="utf-8") as f:
        f.write("\n".join(rental_lines) if rental_lines else "(no RENTAL DATA block found)")

    if not rental_lines:
        return {2: None, 3: None, 4: None}
    return _parse_reiv_bedroom_rows(rental_lines)


def navigate_and_extract_reiv_rents(page, reiv_url: str, state: str, suburb: str, postcode: str) -> Tuple[int, Dict[int, Optional[int]], Dict[int, Optional[int]]]:
    resp = page.goto(reiv_url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
    status = int(resp.status) if resp is not None else 0
    page.wait_for_timeout(1200)

    _try_click_tab(page, "Houses")
    page.wait_for_timeout(450)
    house = extract_reiv_rents_variant(page, state, suburb, postcode, "houses")

    _try_click_tab(page, "Units")
    page.wait_for_timeout(450)
    unit = extract_reiv_rents_variant(page, state, suburb, postcode, "units")

    return status, house, unit


# -----------------------------
# Crawl status computation
# -----------------------------
def any_values(d: Dict[int, Optional[int]]) -> bool:
    return any(v is not None for v in d.values())


def compute_overall_status(
    want_sales: bool,
    want_rent: bool,
    sales_http: Optional[int],
    sales_house: Optional[Dict[int, Optional[int]]],
    sales_unit: Optional[Dict[int, Optional[int]]],
    rent_http: Optional[int],
    rent_house: Optional[Dict[int, Optional[int]]],
    rent_unit: Optional[Dict[int, Optional[int]]],
    had_exception: bool,
) -> str:
    if had_exception:
        return "error"

    # If we attempted sales and got 404, and rent wasn't attempted, that's 404.
    # If rent attempted and 404 but sales not, treat as nodata-ish (REIV 404 is common for suburbs).
    if want_sales and sales_http == 404 and not want_rent:
        return "404"

    found_sales = False
    found_rent = False

    if want_sales and sales_house is not None and sales_unit is not None:
        found_sales = any_values(sales_house) or any_values(sales_unit)

    if want_rent and rent_house is not None and rent_unit is not None:
        found_rent = any_values(rent_house) or any_values(rent_unit)

    if want_sales and sales_http == 404 and want_rent:
        # if REIV has something, call partial, else 404-ish
        if found_rent:
            return "partial"
        return "404"

    if not found_sales and not found_rent:
        # if any explicit 404 in sales-only context, mark 404, else nodata
        if want_sales and sales_http == 404:
            return "404"
        return "nodata"

    # Determine "ok" vs "partial" based on completeness for attempted parts.
    complete_sales = True
    complete_rent = True

    if want_sales and sales_house is not None and sales_unit is not None:
        complete_sales = all(sales_house.get(b) is not None for b in (2, 3, 4)) and all(
            sales_unit.get(b) is not None for b in (2, 3, 4)
        )
    if want_rent and rent_house is not None and rent_unit is not None:
        complete_rent = all(rent_house.get(b) is not None for b in (2, 3, 4)) and all(
            rent_unit.get(b) is not None for b in (2, 3, 4)
        )

    if (not want_sales or complete_sales) and (not want_rent or complete_rent):
        return "ok"
    return "partial"


def apply_status_and_failcount(row: dict, status: str):
    row["scrape_status"] = status
    row["last_attempt"] = dt.datetime.now().isoformat(timespec="seconds")

    # Reset failures on ok
    if status == "ok":
        reset_fail(row)
        return

    # Only auto-blacklist on repeated hard-fails (404/nodata).
    # partial/error can be transient, so don't blacklist by default.
    if status in ("404", "nodata"):
        inc_fail(row)
    else:
        # keep fail_count as-is for partial/error so it doesn't accidentally blacklist
        pass


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Input suburb dataset (never overwritten)")
    ap.add_argument("--out", required=True, help="Output CSV path (write enriched data here)")
    ap.add_argument("--site", choices=["domain", "rea"], default="domain", help="Sales source site")

    ap.add_argument(
        "--mode",
        choices=["fresh", "missing", "new", "failed"],
        default="missing",
        help=(
            "fresh = scrape everything; "
            "missing = fill missing fields but SKIP known 404/nodata unless --retry-hard-fails; "
            "new = ONLY suburbs never queried before; "
            "failed = retry partial/error (optionally also 404/nodata with --retry-hard-fails)"
        ),
    )

    ap.add_argument("--retry-hard-fails", action="store_true", help="Allow retrying rows marked 404/nodata")
    ap.add_argument("--ignore-blacklist", action="store_true", help="Process blacklisted rows anyway")

    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--headful", action="store_true")

    ap.add_argument("--state", help="Only process state(s). Example: VIC or VIC,NSW,QLD")
    ap.add_argument("--suburb", help="Only process suburbs containing this text (case-insensitive). Example: Mentone")
    ap.add_argument("--postcode", help="Only process postcode(s). Example: 3194 or 3194,3204")

    ap.add_argument("--reiv-rent", action="store_true", help="Also scrape weekly rent from REIV for VIC suburbs only")

    args = ap.parse_args()

    selected_states = parse_csv_list(args.state)
    selected_postcodes = parse_csv_list(args.postcode)
    suburb_contains = (args.suburb or "").strip().lower() if args.suburb else None

    print(f"[OUT] {args.out}")
    print(f"[MODE] {args.mode}")
    if args.reiv_rent:
        print("[REIV] Enabled for VIC rents")

    base_fieldnames, base_rows = load_csv(args.csv)

    # Load progress if it exists (regardless of mode) so state is persistent
    _, prog_rows = load_csv(args.out)

    base_fieldnames = ensure_columns(base_fieldnames, base_rows)
    merged_rows = merge_progress(base_rows, prog_rows) if prog_rows else base_rows
    fieldnames = ensure_columns(base_fieldnames, merged_rows)

    # Filter rows (state/suburb/postcode selectors)
    candidate_rows: List[dict] = []
    for r in merged_rows:
        st = (r.get("state") or "").strip().upper()
        sb = (r.get("suburb") or "").strip()
        pc = (r.get("postcode") or "").strip()

        if st not in STATE_PATH:
            continue
        if selected_states and st not in selected_states:
            continue
        if selected_postcodes and pc.upper() not in selected_postcodes:
            continue
        if suburb_contains and suburb_contains not in sb.lower():
            continue

        # mode gate + blacklist gate
        if not should_process_row(r, args.mode, args.retry_hard_fails, args.ignore_blacklist):
            continue

        candidate_rows.append(r)

    if args.limit and args.limit > 0:
        candidate_rows = candidate_rows[: args.limit]

    print(f"[RUN] rows: {len(candidate_rows)}")

    consecutive_errors = 0
    processed_since_checkpoint = 0

    # REIV cache: suburb_slug -> (status_code, house_rents, unit_rents, source_url)
    reiv_cache: Dict[str, Tuple[int, Dict[int, Optional[int]], Dict[int, Optional[int]], str]] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful)

        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
        context = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent=user_agent,
            locale="en-AU",
        )

        # Reduce load but keep scripts/xhr
        def route_handler(route, request):
            rt = request.resource_type
            if rt in {"image", "media", "font"}:
                return route.abort()
            return route.continue_()

        context.route("**/*", route_handler)
        page = context.new_page()

        def on_console(msg):
            try:
                txt = msg.text
                if FILTER_CONSOLE_ERR_FAILED and "net::ERR_FAILED" in txt:
                    return
                print(f"[BROWSER CONSOLE] {msg.type}: {txt}")
            except Exception:
                pass

        page.on("console", on_console)

        for idx, row in enumerate(candidate_rows, 1):
            state = (row.get("state") or "").strip().upper()
            suburb = (row.get("suburb") or "").strip()
            postcode = (row.get("postcode") or "").strip()
            if not state or not suburb or not postcode:
                continue

            if is_blacklisted(row) and not args.ignore_blacklist:
                print(f"[SKIP] blacklisted: {state} {suburb} ({postcode})")
                continue

            # Decide what to attempt
            want_sales = True
            want_rent = bool(args.reiv_rent and state == "VIC")

            # In "missing" mode we still do per-field gating, but we do NOT treat 404/nodata as missing unless retry-hard-fails
            if args.mode == "missing":
                status = get_status(row)
                if status in ("404", "nodata") and not args.retry_hard_fails:
                    # don't keep retrying known-bad rows
                    continue

                if has_any_sales(row):
                    want_sales = False
                if want_rent and has_any_reiv_rent(row):
                    want_rent = False

                if not want_sales and not want_rent:
                    continue

            # In "failed" mode, attempt only what is missing (but row itself already filtered)
            if args.mode == "failed":
                if has_any_sales(row):
                    want_sales = False
                if want_rent and has_any_reiv_rent(row):
                    want_rent = False
                if not want_sales and not want_rent:
                    # already have everything we care about
                    continue

            # In "new" mode, attempt whatever flags request (sales + optional rent)
            # In "fresh" mode, attempt according to flags too.
            print(f"\n[{idx:5}/{len(candidate_rows)}] {state} {suburb} ({postcode}) | sales={want_sales} rent={want_rent}")

            sales_http: Optional[int] = None
            rent_http: Optional[int] = None
            sales_house: Optional[Dict[int, Optional[int]]] = None
            sales_unit: Optional[Dict[int, Optional[int]]] = None
            rent_house: Optional[Dict[int, Optional[int]]] = None
            rent_unit: Optional[Dict[int, Optional[int]]] = None
            had_exception = False

            # SALES
            if want_sales:
                url = build_sales_url(args.site, state, suburb, postcode)
                print(f"[SALES] URL: {url}")

                suburb_429_attempts = 0
                while True:
                    debug_dumped = False
                    try:
                        resp = page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
                        page.wait_for_timeout(POST_NAV_WAIT_MS)
                        sales_http = resp.status if resp is not None else None
                        print(f"[SALES] status: {sales_http}")

                        if sales_http == 429:
                            row["last_error"] = "HTTP 429"
                            dump_debug(page, state, suburb, postcode, "HTTP 429 rate limited")
                            debug_dumped = True

                            if suburb_429_attempts >= MAX_429_RETRIES_PER_SUBURB:
                                raise RuntimeError("HTTP 429 (max retries exceeded)")

                            backoff = BACKOFF_429_SECONDS[min(suburb_429_attempts, len(BACKOFF_429_SECONDS) - 1)]
                            suburb_429_attempts += 1
                            print(f"[BACKOFF] Sleeping {backoff}s then retrying sales")
                            time.sleep(backoff)
                            continue

                        if sales_http == 404:
                            row["last_error"] = "HTTP 404"
                            row["as_of_date"] = dt.date.today().isoformat()
                            row["sales_data_source"] = url
                            print("[SALES] 404 — skipping sales for this suburb")
                            try:
                                page.goto("about:blank", timeout=5_000)
                            except Exception:
                                pass
                            if DEBUG_ON_404:
                                dump_debug(page, state, suburb, postcode, "SALES 404")
                            break

                        if sales_http is not None and sales_http >= 400:
                            row["last_error"] = f"HTTP {sales_http}"
                            dump_debug(page, state, suburb, postcode, f"SALES HTTP {sales_http}")
                            debug_dumped = True
                            raise RuntimeError(f"HTTP {sales_http}")

                        if args.site == "rea":
                            house, unit = extract_rea_medians_house_unit(page)
                        else:
                            house, unit = extract_domain_medians_house_unit(page)

                        sales_house, sales_unit = house, unit

                        row["median_2br_house_price_aud"] = house[2] if house[2] is not None else ""
                        row["median_3br_house_price_aud"] = house[3] if house[3] is not None else ""
                        row["median_4br_house_price_aud"] = house[4] if house[4] is not None else ""

                        row["median_2br_unit_price_aud"] = unit[2] if unit[2] is not None else ""
                        row["median_3br_unit_price_aud"] = unit[3] if unit[3] is not None else ""
                        row["median_4br_unit_price_aud"] = unit[4] if unit[4] is not None else ""

                        row["as_of_date"] = dt.date.today().isoformat()
                        row["sales_data_source"] = url
                        row["last_error"] = ""

                        print(f"[SALES] HOUSE 2/3/4: {fmt_price(house[2])} | {fmt_price(house[3])} | {fmt_price(house[4])}")
                        print(f"[SALES] UNIT  2/3/4: {fmt_price(unit[2])} | {fmt_price(unit[3])} | {fmt_price(unit[4])}")

                        consecutive_errors = 0
                        break

                    except (PWTimeoutError, PWError) as e:
                        consecutive_errors += 1
                        had_exception = True
                        row["last_error"] = f"Playwright: {type(e).__name__}"
                        print(f"[SALES] Playwright error: {e}")
                        dump_debug(page, state, suburb, postcode, f"SALES Playwright error: {type(e).__name__}")
                        break

                    except Exception as e:
                        consecutive_errors += 1
                        had_exception = True
                        if not row.get("last_error"):
                            row["last_error"] = str(e)
                        print(f"[SALES] ERROR: {e}")
                        if not debug_dumped:
                            dump_debug(page, state, suburb, postcode, "SALES General exception")
                        break

                consecutive_errors = cooldown_if_needed(consecutive_errors)

            if want_sales and want_rent:
                polite_delay("between_requests", idx, consecutive_errors)

            # REIV RENT (VIC only)
            if want_rent:
                sub_slug = slugify(suburb)
                reiv_url = build_reiv_url(suburb)

                if sub_slug in reiv_cache:
                    cached_status, rh, ru, src = reiv_cache[sub_slug]
                    rent_http = cached_status
                    rent_house, rent_unit = rh, ru
                    print(f"[REIV] cache hit: status={cached_status} url={src}")
                    if cached_status == 200:
                        row["rent_2br_house_pw_aud"] = rh[2] if rh[2] is not None else ""
                        row["rent_3br_house_pw_aud"] = rh[3] if rh[3] is not None else ""
                        row["rent_4br_house_pw_aud"] = rh[4] if rh[4] is not None else ""
                        row["rent_2br_unit_pw_aud"] = ru[2] if ru[2] is not None else ""
                        row["rent_3br_unit_pw_aud"] = ru[3] if ru[3] is not None else ""
                        row["rent_4br_unit_pw_aud"] = ru[4] if ru[4] is not None else ""
                    row["reiv_rent_source"] = src
                    row["as_of_date"] = dt.date.today().isoformat()

                else:
                    print(f"[REIV] URL: {reiv_url}")
                    try:
                        rstatus, rh, ru = navigate_and_extract_reiv_rents(page, reiv_url, state, suburb, postcode)
                        rent_http = rstatus
                        rent_house, rent_unit = rh, ru

                        print(f"[REIV] status: {rstatus}")
                        print(f"[REIV] HOUSE rent 2/3/4: {fmt_price(rh[2])}/w | {fmt_price(rh[3])}/w | {fmt_price(rh[4])}/w")
                        print(f"[REIV] UNIT  rent 2/3/4: {fmt_price(ru[2])}/w | {fmt_price(ru[3])}/w | {fmt_price(ru[4])}/w")

                        if rstatus == 200:
                            row["rent_2br_house_pw_aud"] = rh[2] if rh[2] is not None else ""
                            row["rent_3br_house_pw_aud"] = rh[3] if rh[3] is not None else ""
                            row["rent_4br_house_pw_aud"] = rh[4] if rh[4] is not None else ""
                            row["rent_2br_unit_pw_aud"] = ru[2] if ru[2] is not None else ""
                            row["rent_3br_unit_pw_aud"] = ru[3] if ru[3] is not None else ""
                            row["rent_4br_unit_pw_aud"] = ru[4] if ru[4] is not None else ""

                        row["reiv_rent_source"] = reiv_url
                        row["as_of_date"] = dt.date.today().isoformat()

                        reiv_cache[sub_slug] = (rstatus, rh, ru, reiv_url)

                        if all(v is None for v in rh.values()) and all(v is None for v in ru.values()):
                            dump_debug(page, state, suburb, postcode, "REIV: no rent values found (check *_reiv_rental_*.txt)")

                        consecutive_errors = 0

                    except (PWTimeoutError, PWError) as e:
                        consecutive_errors += 1
                        had_exception = True
                        row["last_error"] = f"Playwright: {type(e).__name__}"
                        print(f"[REIV] Playwright error: {e}")
                        dump_debug(page, state, suburb, postcode, f"REIV Playwright error: {type(e).__name__}")

                    except Exception as e:
                        consecutive_errors += 1
                        had_exception = True
                        row["last_error"] = str(e)
                        print(f"[REIV] ERROR: {e}")
                        dump_debug(page, state, suburb, postcode, "REIV exception")

                consecutive_errors = cooldown_if_needed(consecutive_errors)

            # ---- Update crawl state (this is the key change) ----
            status = compute_overall_status(
                want_sales=want_sales,
                want_rent=want_rent,
                sales_http=sales_http,
                sales_house=sales_house,
                sales_unit=sales_unit,
                rent_http=rent_http,
                rent_house=rent_house,
                rent_unit=rent_unit,
                had_exception=had_exception,
            )
            apply_status_and_failcount(row, status)
            print(f"[STATE] scrape_status={row['scrape_status']} fail_count={row['fail_count']} blacklisted={row['blacklisted']}")

            processed_since_checkpoint += 1
            if processed_since_checkpoint >= CHECKPOINT_EVERY:
                write_csv(args.out, fieldnames, merged_rows)
                processed_since_checkpoint = 0

            if consecutive_errors >= MAX_CONSEC_ERRORS:
                print("\nToo many consecutive errors. Stopping to avoid stressing production.\n")
                break

            polite_delay("between_suburbs", idx, consecutive_errors)

        browser.close()

    write_csv(args.out, fieldnames, merged_rows)
    print(f"\nDone. Output CSV: {args.out}")
    print(f"Debug artefacts (if any) in: {DEBUG_DIR}/\n")


if __name__ == "__main__":
    main()
