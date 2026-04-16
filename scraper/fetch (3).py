"""
Dallas County Motivated Seller Lead Scraper
Targets: Lis Pendens (LP), Notice of Foreclosure (NOFC), Probate (PRO)
Sources: dallas.tx.publicsearch.us  +  dallascad.org bulk parcel DBF
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from dbfread import DBF

# ── playwright (async) ──────────────────────────────────────────────────────
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
except ImportError:
    print("playwright not installed – run: pip install playwright && python -m playwright install chromium")
    sys.exit(1)

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("dallas_scraper")

# ── constants ────────────────────────────────────────────────────────────────
CLERK_URL   = "https://dallas.tx.publicsearch.us/"
CAD_URL     = "https://www.dallascad.org/DataProducts.aspx"
LOOK_BACK   = 7          # days
MAX_RETRIES = 3
RETRY_WAIT  = 4          # seconds

DOC_CATS = {
    "LP":   {"label": "Lis Pendens",           "search_terms": ["LIS PENDENS", "LP"]},
    "NOFC": {"label": "Notice of Foreclosure",  "search_terms": ["NOTICE OF FORECLOSURE", "FORECLOSURE", "NOFC", "NOF"]},
    "PRO":  {"label": "Probate / Estate",       "search_terms": ["PROBATE", "LETTERS TESTAMENTARY", "LETTERS OF ADMINISTRATION",
                                                                   "WILL", "ESTATE", "DECEDENT", "PRO"]},
}

# ── output dirs ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
for d in ["dashboard", "data", "scraper/tmp"]:
    (ROOT / d).mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 – PARCEL DATA (Dallas CAD bulk DBF)
# ═══════════════════════════════════════════════════════════════════════════

def _session_with_retries() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"})
    return sess


def _download_with_retry(sess: requests.Session, url: str, **kwargs) -> requests.Response | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = sess.get(url, timeout=120, **kwargs)
            r.raise_for_status()
            return r
        except Exception as exc:
            log.warning("Download attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT * attempt)
    return None


def download_parcel_dbf() -> dict[str, dict]:
    """
    Download Dallas CAD bulk parcel data and return owner-name-keyed lookup dict.
    Falls back gracefully if the site is unreachable.
    """
    log.info("=== Downloading Dallas CAD parcel DBF ===")
    sess = _session_with_retries()

    # ── 1. Fetch the DataProducts page and grab the DBF/ZIP download link ──
    resp = _download_with_retry(sess, CAD_URL)
    if resp is None:
        log.warning("Cannot reach Dallas CAD; parcel lookup will be empty.")
        return {}

    soup = BeautifulSoup(resp.text, "lxml")

    # Look for __doPostBack link or direct href containing "APPRAISAL" / "PARCEL" / ".zip" / ".dbf"
    dbf_url = None
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        text = a.get_text(strip=True).lower()
        if any(k in href or k in text for k in ["parcel", "appraisal", "res_", "residential", "all_prop"]):
            if any(ext in href for ext in [".zip", ".dbf", ".exe"]):
                dbf_url = a["href"] if a["href"].startswith("http") else "https://www.dallascad.org/" + a["href"].lstrip("/")
                break

    # Fallback: look for __doPostBack buttons/links
    if not dbf_url:
        for a in soup.find_all("a", href=True):
            if "__doPostBack" in a.get("href", ""):
                text = a.get_text(strip=True).lower()
                if any(k in text for k in ["parcel", "appraisal", "property", "res"]):
                    # Extract event target
                    m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", a["href"])
                    if m:
                        viewstate = soup.find("input", {"id": "__VIEWSTATE"})
                        eventval  = soup.find("input", {"id": "__EVENTVALIDATION"})
                        post_data = {
                            "__EVENTTARGET":   m.group(1),
                            "__EVENTARGUMENT": m.group(2),
                            "__VIEWSTATE":     viewstate["value"] if viewstate else "",
                            "__EVENTVALIDATION": eventval["value"] if eventval else "",
                        }
                        log.info("Attempting __doPostBack download for: %s", m.group(1))
                        for attempt in range(1, MAX_RETRIES + 1):
                            try:
                                pr = sess.post(CAD_URL, data=post_data, timeout=180, stream=True)
                                pr.raise_for_status()
                                content_type = pr.headers.get("Content-Type", "")
                                if "zip" in content_type or "octet" in content_type or len(pr.content) > 10_000:
                                    zip_bytes = pr.content
                                    break
                            except Exception as exc:
                                log.warning("PostBack attempt %d failed: %s", attempt, exc)
                                if attempt < MAX_RETRIES:
                                    time.sleep(RETRY_WAIT * attempt)
                        else:
                            zip_bytes = None
                        if zip_bytes:
                            return _parse_parcel_zip(zip_bytes)

    if dbf_url:
        log.info("Downloading parcel ZIP from: %s", dbf_url)
        r = _download_with_retry(sess, dbf_url, stream=True)
        if r:
            return _parse_parcel_zip(r.content)

    log.warning("Could not locate parcel DBF download; parcel lookup will be empty.")
    return {}


def _parse_parcel_zip(raw_bytes: bytes) -> dict[str, dict]:
    """Extract DBF from ZIP bytes and build owner-name → address lookup."""
    lookup: dict[str, dict] = {}
    tmp_dir = ROOT / "scraper/tmp"

    try:
        buf = io.BytesIO(raw_bytes)
        if zipfile.is_zipfile(buf):
            buf.seek(0)
            with zipfile.ZipFile(buf) as zf:
                dbf_names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                if not dbf_names:
                    log.warning("No .dbf found inside ZIP archive.")
                    return {}
                dbf_name = dbf_names[0]
                log.info("Extracting DBF: %s", dbf_name)
                zf.extract(dbf_name, tmp_dir)
                dbf_path = tmp_dir / dbf_name
        else:
            # Might be a raw DBF
            dbf_path = tmp_dir / "parcels.dbf"
            dbf_path.write_bytes(raw_bytes)

        log.info("Parsing DBF: %s", dbf_path)
        lookup = _build_lookup_from_dbf(str(dbf_path))
        log.info("Parcel lookup built: %d entries", len(lookup))

    except Exception as exc:
        log.error("Failed to parse parcel ZIP/DBF: %s", exc)

    return lookup


def _build_lookup_from_dbf(dbf_path: str) -> dict[str, dict]:
    """
    Read DBF and build a dict keyed by normalised owner name variants.
    """
    lookup: dict[str, dict] = {}

    # Column name mappings (handle different CAD export schemas)
    OWNER_COLS  = ["OWNER_NAME1", "OWNERNAME1", "OWNER1", "OWNER"]
    MAIL_ADDR   = ["OWNER_ADDRESS_LINE2", "MAIL_ADDR", "MAILADDR", "OWNER_ADDR"]
    MAIL_CITY   = ["OWNER_CITY", "MAIL_CITY", "MAILCITY", "CITY"]
    MAIL_STATE  = ["OWNER_STATE", "MAIL_STATE", "MAILSTATE", "STATE"]
    MAIL_ZIP    = ["OWNER_ZIPCODE", "MAIL_ZIP", "MAILZIP", "ZIP"]
    PROP_NUM    = ["STREET_NUM", "SITUS_NUM", "PROP_NUM", "STRNUM"]
    PROP_STREET = ["FULL_STREET_NAME", "SITUS_STREET", "PROP_STREET", "STREET"]
    PROP_CITY   = ["PROPERTY_CITY", "PROP_CITY", "SITUSCITY", "PCITY"]
    PROP_ZIP    = ["PROPERTY_ZIPCODE", "PROP_ZIP", "SITUSZIP", "PZIP"]

    def pick(record: dict, candidates: list[str]) -> str:
        for c in candidates:
            v = record.get(c) or record.get(c.lower()) or ""
            if v and str(v).strip():
                return str(v).strip()
        return ""

    try:
        table = DBF(dbf_path, encoding="latin-1", ignore_missing_memofile=True)
        for row in table:
            try:
                rec = {k.upper(): v for k, v in row.items()}
                owner_raw = pick(rec, OWNER_COLS)
                if not owner_raw:
                    continue

                entry = {
                    "mail_address": pick(rec, MAIL_ADDR),
                    "mail_city":    pick(rec, MAIL_CITY),
                    "mail_state":   pick(rec, MAIL_STATE),
                    "mail_zip":     pick(rec, MAIL_ZIP),
                    "prop_num":     pick(rec, PROP_NUM),
                    "prop_street":  pick(rec, PROP_STREET),
                    "prop_city":    pick(rec, PROP_CITY),
                    "prop_zip":     pick(rec, PROP_ZIP),
                }
                # Build full property address
                if entry["prop_num"] and entry["prop_street"]:
                    entry["prop_address"] = f"{entry['prop_num']} {entry['prop_street']}".strip()
                else:
                    entry["prop_address"] = ""

                # Index by multiple name variants
                for variant in _name_variants(owner_raw):
                    lookup[variant] = entry
            except Exception:
                continue
    except Exception as exc:
        log.error("DBF read error: %s", exc)

    return lookup


def _name_variants(name: str) -> list[str]:
    """Return normalised name variants for fuzzy matching."""
    name = name.strip().upper()
    variants = {name}

    # Strip common suffixes / LLC markers for person name guessing
    clean = re.sub(r"\s+(LLC|INC|CORP|LTD|L\.L\.C\.|TRUST|ETAL|ET AL|ET UX)\.?$", "", name).strip()
    variants.add(clean)

    # Try "LAST, FIRST" <-> "FIRST LAST" flips
    if "," in clean:
        parts = [p.strip() for p in clean.split(",", 1)]
        variants.add(f"{parts[0]}, {parts[1]}")   # LAST, FIRST
        variants.add(f"{parts[1]} {parts[0]}")     # FIRST LAST
    else:
        parts = clean.split()
        if len(parts) >= 2:
            variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")  # LAST, FIRST
            variants.add(f"{parts[-1]} {' '.join(parts[:-1])}")   # LAST FIRST

    return [v for v in variants if v]


def lookup_owner(name: str, parcel_lookup: dict) -> dict:
    """Try to find parcel record for an owner name."""
    if not name or not parcel_lookup:
        return {}
    for variant in _name_variants(name):
        if variant in parcel_lookup:
            return parcel_lookup[variant]
    return {}


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 – CLERK PORTAL (Playwright async scraper)
# ═══════════════════════════════════════════════════════════════════════════

async def scrape_clerk_portal(doc_types: list[str], date_from: str, date_to: str) -> list[dict]:
    """
    Use Playwright to search dallas.tx.publicsearch.us for each doc type.
    Returns raw record dicts.
    """
    all_records: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
        )
        page = await ctx.new_page()

        for cat_key, cat_info in DOC_CATS.items():
            for term in cat_info["search_terms"]:
                log.info("Clerk search: cat=%s term='%s' dates=%s–%s", cat_key, term, date_from, date_to)
                records = await _clerk_search(page, term, cat_key, cat_info["label"], date_from, date_to)
                all_records.extend(records)
                if records:
                    log.info("  → %d records found for '%s'", len(records), term)
                    break  # Got results for this category, skip remaining terms
                await asyncio.sleep(1.5)

        await browser.close()

    # Deduplicate by doc_num
    seen = set()
    deduped = []
    for r in all_records:
        key = r.get("doc_num", "")
        if key and key not in seen:
            seen.add(key)
            deduped.append(r)
        elif not key:
            deduped.append(r)

    log.info("Clerk total (deduped): %d records", len(deduped))
    return deduped


async def _clerk_search(page, term: str, cat_key: str, cat_label: str,
                         date_from: str, date_to: str) -> list[dict]:
    records: list[dict] = []

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await page.goto(CLERK_URL, wait_until="networkidle", timeout=30_000)
            await asyncio.sleep(2)

            # ── Navigate to document search ──────────────────────────────
            # Try to find a "Search" or "Document Search" link
            search_nav = page.locator("a, button").filter(has_text=re.compile(r"search|document", re.I))
            if await search_nav.count() > 0:
                await search_nav.first.click()
                await page.wait_for_load_state("networkidle", timeout=15_000)
                await asyncio.sleep(1)

            # ── Fill document type field ─────────────────────────────────
            # The portal uses various input patterns; try them all
            filled = False
            for sel in ['input[placeholder*="document type" i]',
                        'input[placeholder*="doc type" i]',
                        'input[name*="docType" i]',
                        'input[id*="docType" i]',
                        '#docType', '.docTypeInput']:
                try:
                    loc = page.locator(sel).first
                    if await loc.is_visible(timeout=2000):
                        await loc.fill(term)
                        filled = True
                        break
                except Exception:
                    continue

            # ── Date range ───────────────────────────────────────────────
            for (sel_list, value) in [
                (['input[placeholder*="start date" i]', 'input[id*="startDate" i]', '#startDate', 'input[name*="startDate"]'], date_from),
                (['input[placeholder*="end date" i]',   'input[id*="endDate" i]',   '#endDate',   'input[name*="endDate"]'],   date_to),
            ]:
                for sel in sel_list:
                    try:
                        loc = page.locator(sel).first
                        if await loc.is_visible(timeout=2000):
                            await loc.fill(value)
                            break
                    except Exception:
                        continue

            # ── Submit ───────────────────────────────────────────────────
            for sel in ['button[type="submit"]', 'input[type="submit"]',
                        'button:has-text("Search")', 'a:has-text("Search")']:
                try:
                    loc = page.locator(sel).first
                    if await loc.is_visible(timeout=2000):
                        await loc.click()
                        break
                except Exception:
                    continue

            await page.wait_for_load_state("networkidle", timeout=20_000)
            await asyncio.sleep(2)

            # ── Try REST API approach (many publicsearch.us portals expose JSON) ──
            api_records = await _try_api_search(page, term, cat_key, cat_label, date_from, date_to)
            if api_records:
                return api_records

            # ── Parse HTML results table ─────────────────────────────────
            content = await page.content()
            records = _parse_clerk_html(content, cat_key, cat_label)

            # ── Paginate ─────────────────────────────────────────────────
            while True:
                next_btn = page.locator("a:has-text('Next'), button:has-text('Next'), [aria-label='Next page']").first
                try:
                    if await next_btn.is_visible(timeout=2000) and await next_btn.is_enabled(timeout=2000):
                        await next_btn.click()
                        await page.wait_for_load_state("networkidle", timeout=15_000)
                        await asyncio.sleep(1.5)
                        content = await page.content()
                        records.extend(_parse_clerk_html(content, cat_key, cat_label))
                    else:
                        break
                except Exception:
                    break

            return records

        except PWTimeout as exc:
            log.warning("Playwright timeout on attempt %d for '%s': %s", attempt, term, exc)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_WAIT * attempt)
        except Exception as exc:
            log.warning("Playwright error on attempt %d for '%s': %s", attempt, term, exc)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_WAIT * attempt)

    return records


async def _try_api_search(page, term: str, cat_key: str, cat_label: str,
                           date_from: str, date_to: str) -> list[dict]:
    """
    publicsearch.us portals often expose a REST/JSON search API.
    Try common endpoint patterns.
    """
    base = CLERK_URL.rstrip("/")
    endpoints = [
        f"{base}/api/instrument/search",
        f"{base}/api/search",
        f"{base}/result/search",
    ]
    params = {
        "docTypeCode": term,
        "startDate": date_from,
        "endDate": date_to,
        "countyId": "dallas",
        "page": 1,
        "pageSize": 200,
    }
    sess = _session_with_retries()

    for ep in endpoints:
        try:
            r = sess.get(ep, params=params, timeout=20)
            if r.status_code == 200 and "application/json" in r.headers.get("Content-Type", ""):
                data = r.json()
                return _parse_api_results(data, cat_key, cat_label)
        except Exception:
            continue

    return []


def _parse_api_results(data: Any, cat_key: str, cat_label: str) -> list[dict]:
    """Parse JSON API response from publicsearch.us."""
    records = []
    items = []

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for key in ["results", "instruments", "records", "data", "items"]:
            if key in data and isinstance(data[key], list):
                items = data[key]
                break

    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            doc_num  = str(item.get("instrumentNumber") or item.get("docNumber") or item.get("recordingNumber") or "")
            doc_type = str(item.get("documentType") or item.get("docType") or item.get("instrumentType") or "")
            filed    = str(item.get("recordedDate") or item.get("filedDate") or item.get("instrumentDate") or "")
            grantor  = str(item.get("grantor") or item.get("grantorName") or "")
            grantee  = str(item.get("grantee") or item.get("granteeName") or "")
            legal    = str(item.get("legalDescription") or item.get("legal") or "")
            amount   = _parse_amount(item.get("amount") or item.get("consideration") or item.get("loanAmount") or "")
            link     = str(item.get("url") or item.get("imageUrl") or item.get("link") or "")

            if not link and doc_num:
                link = f"{CLERK_URL}results/index?doc={doc_num}"

            records.append({
                "doc_num":   doc_num,
                "doc_type":  doc_type or cat_label,
                "filed":     _normalise_date(filed),
                "cat":       cat_key,
                "cat_label": cat_label,
                "owner":     grantor,
                "grantee":   grantee,
                "amount":    amount,
                "legal":     legal,
                "clerk_url": link,
            })
        except Exception:
            continue

    return records


def _parse_clerk_html(html: str, cat_key: str, cat_label: str) -> list[dict]:
    """Parse search results from HTML page."""
    records = []
    soup = BeautifulSoup(html, "lxml")

    # Try standard results table
    table = soup.find("table", class_=re.compile(r"result|search|instrument", re.I))
    if not table:
        table = soup.find("table")
    if not table:
        return records

    headers = []
    for th in table.find_all("th"):
        headers.append(th.get_text(strip=True).lower())

    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if not cells:
            continue
        try:
            row: dict[str, str] = {}
            for i, td in enumerate(cells):
                key = headers[i] if i < len(headers) else str(i)
                row[key] = td.get_text(strip=True)

            # Try to extract link
            link = ""
            a_tag = tr.find("a", href=True)
            if a_tag:
                href = a_tag["href"]
                link = href if href.startswith("http") else CLERK_URL.rstrip("/") + "/" + href.lstrip("/")

            # Map flexible column names
            def col(*keys):
                for k in keys:
                    for hk in row:
                        if k in hk:
                            return row[hk]
                return ""

            doc_num  = col("instrument", "doc number", "record", "number")
            doc_type = col("type", "document type", "doc type")
            filed    = col("date", "filed", "recorded")
            grantor  = col("grantor", "owner", "seller")
            grantee  = col("grantee", "buyer")
            legal    = col("legal", "description")
            amount   = _parse_amount(col("amount", "consideration", "value"))

            records.append({
                "doc_num":   doc_num,
                "doc_type":  doc_type or cat_label,
                "filed":     _normalise_date(filed),
                "cat":       cat_key,
                "cat_label": cat_label,
                "owner":     grantor,
                "grantee":   grantee,
                "amount":    amount,
                "legal":     legal,
                "clerk_url": link,
            })
        except Exception:
            continue

    return records


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 – SCORING & FLAG LOGIC
# ═══════════════════════════════════════════════════════════════════════════

def build_flags(rec: dict, week_ago: str) -> list[str]:
    flags: list[str] = []
    cat   = rec.get("cat", "")
    owner = (rec.get("owner") or "").upper()
    amt   = rec.get("amount") or 0

    if cat == "LP":
        flags.append("Lis pendens")
    if cat == "NOFC":
        flags.append("Pre-foreclosure")
    if cat == "PRO":
        flags.append("Probate / estate")
    if amt and float(amt) > 0:
        flags.append("Tax lien")
    if re.search(r"\b(LLC|INC|CORP|LTD|L\.L\.C\.|TRUST)\b", owner):
        flags.append("LLC / corp owner")
    filed = rec.get("filed", "")
    if filed and filed >= week_ago:
        flags.append("New this week")

    return flags


def calc_score(rec: dict, flags: list[str]) -> int:
    score = 30  # base
    score += len(flags) * 10
    score = min(score, 30 + len(flags) * 10)  # don't double-count combos yet

    # LP + FC combo
    has_lp = any("lis pendens" in f.lower() for f in flags)
    has_fc = any("pre-foreclosure" in f.lower() for f in flags)
    if has_lp and has_fc:
        score += 20

    amt = float(rec.get("amount") or 0)
    if amt > 100_000:
        score += 15
    elif amt > 50_000:
        score += 10

    if "New this week" in flags:
        score += 5
    if rec.get("prop_address"):
        score += 5

    return min(score, 100)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 – RECORD ENRICHMENT & OUTPUT
# ═══════════════════════════════════════════════════════════════════════════

def enrich_record(raw: dict, parcel_lookup: dict, week_ago: str) -> dict:
    """Merge parcel data, build flags/score, return final record."""
    owner   = (raw.get("owner") or "").strip()
    parcel  = lookup_owner(owner, parcel_lookup)

    prop_addr   = parcel.get("prop_address", "")
    prop_city   = parcel.get("prop_city", "")
    prop_zip    = parcel.get("prop_zip", "")
    mail_addr   = parcel.get("mail_address", "")
    mail_city   = parcel.get("mail_city", "")
    mail_state  = parcel.get("mail_state", "")
    mail_zip    = parcel.get("mail_zip", "")

    # Extract address from legal description if parcel lookup failed
    if not prop_addr:
        prop_addr = _extract_address_from_legal(raw.get("legal", ""))

    rec = {
        "doc_num":    raw.get("doc_num", ""),
        "doc_type":   raw.get("doc_type", ""),
        "filed":      raw.get("filed", ""),
        "cat":        raw.get("cat", ""),
        "cat_label":  raw.get("cat_label", ""),
        "owner":      owner,
        "grantee":    (raw.get("grantee") or "").strip(),
        "amount":     raw.get("amount") or 0,
        "legal":      (raw.get("legal") or "").strip(),
        "prop_address": prop_addr,
        "prop_city":    prop_city or "DALLAS",
        "prop_state":   "TX",
        "prop_zip":     prop_zip,
        "mail_address": mail_addr,
        "mail_city":    mail_city,
        "mail_state":   mail_state or "TX",
        "mail_zip":     mail_zip,
        "clerk_url":    raw.get("clerk_url", ""),
        "flags":        [],
        "score":        0,
    }

    flags       = build_flags(rec, week_ago)
    rec["flags"] = flags
    rec["score"] = calc_score(rec, flags)
    return rec


def _extract_address_from_legal(legal: str) -> str:
    """Best-effort address extraction from legal description string."""
    if not legal:
        return ""
    m = re.search(r"\b(\d{1,5}\s+[A-Z][A-Z0-9 ,\.]+(?:ST|AVE|DR|RD|BLVD|LN|CT|WAY|PL|TRL|HWY)\b)", legal, re.I)
    return m.group(1).strip() if m else ""


def _parse_amount(val: Any) -> float | None:
    if val is None:
        return None
    s = re.sub(r"[^\d\.]", "", str(val))
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _normalise_date(raw: str) -> str:
    """Try to normalise various date formats to YYYY-MM-DD."""
    if not raw:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y",
                "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(raw.strip()[:10], fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return raw.strip()[:10]


def split_owner_name(name: str) -> tuple[str, str]:
    """Split owner name into (first, last) best-effort."""
    name = re.sub(r"\s+(LLC|INC|CORP|LTD|TRUST|ETAL|ET AL|ET UX)\.?$", "", name.strip(), flags=re.I).strip()
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        return parts[1], parts[0]   # FIRST, LAST
    parts = name.split()
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return name, ""


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5 – GHL CSV EXPORT
# ═══════════════════════════════════════════════════════════════════════════

def export_ghl_csv(records: list[dict], path: Path) -> None:
    GHL_COLS = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
        "Motivated Seller Flags", "Source", "Public Records URL",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=GHL_COLS)
        writer.writeheader()
        for rec in records:
            fname, lname = split_owner_name(rec.get("owner", ""))
            writer.writerow({
                "First Name":            fname,
                "Last Name":             lname,
                "Mailing Address":       rec.get("mail_address", ""),
                "Mailing City":          rec.get("mail_city", ""),
                "Mailing State":         rec.get("mail_state", ""),
                "Mailing Zip":           rec.get("mail_zip", ""),
                "Property Address":      rec.get("prop_address", ""),
                "Property City":         rec.get("prop_city", ""),
                "Property State":        rec.get("prop_state", "TX"),
                "Property Zip":          rec.get("prop_zip", ""),
                "Lead Type":             rec.get("cat_label", ""),
                "Document Type":         rec.get("doc_type", ""),
                "Date Filed":            rec.get("filed", ""),
                "Document Number":       rec.get("doc_num", ""),
                "Amount/Debt Owed":      rec.get("amount", ""),
                "Seller Score":          rec.get("score", 0),
                "Motivated Seller Flags": "; ".join(rec.get("flags", [])),
                "Source":                "Dallas County Clerk",
                "Public Records URL":    rec.get("clerk_url", ""),
            })
    log.info("GHL CSV saved: %s (%d rows)", path, len(records))


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6 – MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════

async def main() -> None:
    today     = datetime.now()
    week_ago  = today - timedelta(days=LOOK_BACK)
    date_from = week_ago.strftime("%m/%d/%Y")
    date_to   = today.strftime("%m/%d/%Y")
    week_ago_iso = week_ago.strftime("%Y-%m-%d")

    log.info("━━━ Dallas County Motivated Seller Scraper ━━━")
    log.info("Date range: %s → %s", date_from, date_to)

    # ── 1. Parcel lookup ────────────────────────────────────────────────────
    parcel_lookup = download_parcel_dbf()

    # ── 2. Clerk portal scraping ────────────────────────────────────────────
    raw_records = await scrape_clerk_portal(list(DOC_CATS.keys()), date_from, date_to)

    # ── 3. Enrich each record ───────────────────────────────────────────────
    enriched: list[dict] = []
    for raw in raw_records:
        try:
            enriched.append(enrich_record(raw, parcel_lookup, week_ago_iso))
        except Exception as exc:
            log.warning("Enrich failed for record %s: %s", raw.get("doc_num", "?"), exc)

    # Sort by score desc
    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)

    with_address = sum(1 for r in enriched if r.get("prop_address"))

    output = {
        "fetched_at":  today.isoformat(),
        "source":      "Dallas County Clerk + Dallas CAD",
        "date_range":  {"from": date_from, "to": date_to},
        "total":       len(enriched),
        "with_address": with_address,
        "records":     enriched,
    }

    # ── 4. Save JSON ────────────────────────────────────────────────────────
    for dest in [ROOT / "dashboard/records.json", ROOT / "data/records.json"]:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
        log.info("Saved: %s", dest)

    # ── 5. GHL CSV export ───────────────────────────────────────────────────
    csv_path = ROOT / "data" / f"ghl_export_{today.strftime('%Y%m%d')}.csv"
    export_ghl_csv(enriched, csv_path)

    log.info("━━━ Complete: %d records (%d with address) ━━━", len(enriched), with_address)


if __name__ == "__main__":
    asyncio.run(main())
