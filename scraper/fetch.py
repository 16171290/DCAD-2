"""
Dallas County Motivated Seller Lead Scraper
Targets: Lis Pendens (LP), Notice of Foreclosure (NOFC), Probate (PRO)
Sources: dallas.tx.publicsearch.us  +  dallascad.org bulk parcel DBF

v2 – rewrote Playwright logic to match actual publicsearch.us UI:
  - Tries REST API first (no browser needed)
  - Playwright navigates directly via URL params — skips broken nav click
  - Falls back to form-fill using force=True clicks
"""

import asyncio
import csv
import io
import json
import logging
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

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
except ImportError:
    print("playwright not installed")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("dallas_scraper")

CLERK_BASE = "https://dallas.tx.publicsearch.us"
CAD_URL    = "https://www.dallascad.org/DataProducts.aspx"
LOOK_BACK  = 7
MAX_RETRIES = 3
RETRY_WAIT  = 4

DOC_TYPE_CODES = {
    "LP": {
        "label": "Lis Pendens",
        "codes": ["LP", "LISP", "LIS PENDENS"],
    },
    "NOFC": {
        "label": "Notice of Foreclosure",
        "codes": ["NOFC", "NOF", "NOTS", "FORECLOSURE NOTICE"],
    },
    "PRO": {
        "label": "Probate / Estate",
        "codes": ["PROB", "PRO", "WILL", "LTTR", "LTAD"],
    },
}

ROOT = Path(__file__).resolve().parent.parent
for d in ["dashboard", "data", "scraper/tmp"]:
    (ROOT / d).mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 – REST API (primary — no browser needed)
# ═══════════════════════════════════════════════════════════════════════════

def scrape_via_api(date_from: str, date_to: str) -> list:
    """Try the publicsearch.us JSON REST API directly with requests."""
    all_records = []
    sess = _session_with_retries()

    try:
        dt_from = datetime.strptime(date_from, "%m/%d/%Y").strftime("%Y-%m-%d")
        dt_to   = datetime.strptime(date_to,   "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        dt_from, dt_to = date_from, date_to

    api_endpoints = [
        f"{CLERK_BASE}/api/instrument/search",
        f"{CLERK_BASE}/api/search/instrument",
        f"{CLERK_BASE}/search/api/instrument",
        f"{CLERK_BASE}/api/search",
    ]

    for cat_key, cat_info in DOC_TYPE_CODES.items():
        for code in cat_info["codes"]:
            found = False
            for endpoint in api_endpoints:
                for params in [
                    {"countyId":"dallas","state":"TX","docTypeCode":code,
                     "startDate":dt_from,"endDate":dt_to,"page":1,"pageSize":500},
                    {"county":"dallas","documentType":code,
                     "recordedDateFrom":dt_from,"recordedDateTo":dt_to,"page":1,"size":500},
                ]:
                    try:
                        r = sess.get(endpoint, params=params, timeout=30)
                        if r.status_code == 200 and "json" in r.headers.get("Content-Type",""):
                            data = r.json()
                            records = _parse_api_results(data, cat_key, cat_info["label"])
                            if records:
                                log.info("API hit: cat=%s code=%s → %d records", cat_key, code, len(records))
                                all_records.extend(records)
                                found = True
                                break
                    except Exception as exc:
                        log.debug("API attempt failed: %s", exc)
                if found:
                    break
            if found:
                break

    log.info("API total: %d records", len(all_records))
    return all_records


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 – PLAYWRIGHT (fallback — direct URL navigation)
# ═══════════════════════════════════════════════════════════════════════════

async def scrape_via_playwright(date_from: str, date_to: str) -> list:
    """Navigate directly to search results — bypasses the broken nav click."""
    all_records = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"]
        )
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width":1280,"height":900},
        )
        page = await ctx.new_page()

        # Intercept XHR/fetch responses to capture API calls the portal makes
        captured: list = []

        async def handle_response(response):
            try:
                if "instrument" in response.url.lower() or "search" in response.url.lower():
                    ct = response.headers.get("content-type","")
                    if "json" in ct:
                        try:
                            data = await response.json()
                            captured.append(data)
                        except Exception:
                            pass
            except Exception:
                pass

        page.on("response", handle_response)

        # Load homepage to get session cookies
        try:
            await page.goto(CLERK_BASE, wait_until="domcontentloaded", timeout=25_000)
            await asyncio.sleep(2)
        except Exception as exc:
            log.warning("Homepage load failed: %s", exc)

        for cat_key, cat_info in DOC_TYPE_CODES.items():
            for code in cat_info["codes"]:
                captured.clear()
                records = await _playwright_search(
                    page, code, cat_key, cat_info["label"], date_from, date_to, captured
                )
                if records:
                    log.info("Playwright: cat=%s code=%s → %d records", cat_key, code, len(records))
                    all_records.extend(records)
                    break
                await asyncio.sleep(1)

        await browser.close()

    # Deduplicate
    seen, deduped = set(), []
    for r in all_records:
        k = r.get("doc_num","")
        if k and k not in seen:
            seen.add(k)
            deduped.append(r)
        elif not k:
            deduped.append(r)

    log.info("Playwright total (deduped): %d records", len(deduped))
    return deduped


async def _playwright_search(page, code, cat_key, cat_label, date_from, date_to, captured):
    """Try multiple search strategies."""

    # Strategy 1: direct URL with query params
    search_urls = [
        f"{CLERK_BASE}/results/index?docTypeCode={code}&startDate={date_from}&endDate={date_to}",
        f"{CLERK_BASE}/search?docType={code}&startDate={date_from}&endDate={date_to}",
        f"{CLERK_BASE}/instrument/search?documentType={code}&recordedDateFrom={date_from}&recordedDateTo={date_to}",
    ]

    for url in search_urls:
        try:
            log.info("  Playwright URL: %s", url[:90])
            await page.goto(url, wait_until="networkidle", timeout=20_000)
            await asyncio.sleep(2)

            if "login" in page.url.lower():
                break

            content = await page.content()

            # Check captured XHR calls first
            for data in captured:
                records = _parse_api_results(data, cat_key, cat_label)
                if records:
                    return records

            # Parse HTML table
            records = _parse_clerk_html(content, cat_key, cat_label)
            if records:
                return records

            # Check for embedded JSON
            records = _extract_json_from_page(content, cat_key, cat_label)
            if records:
                return records

        except Exception as exc:
            log.debug("URL strategy failed: %s", exc)
            continue

    # Strategy 2: use the search form — click workspace tab with force
    try:
        await page.goto(CLERK_BASE, wait_until="domcontentloaded", timeout=20_000)
        await asyncio.sleep(2)

        # Click the search/document workspace tab using JS to bypass overlay
        await page.evaluate("""
            () => {
                const tabs = document.querySelectorAll('li[data-tourid], li.workspaces__tab, [class*="workspaces__tab"]');
                for (const tab of tabs) {
                    if (tab.innerText && /search|document/i.test(tab.innerText)) {
                        tab.click();
                        return;
                    }
                }
                // Try any tab
                if (tabs.length > 0) tabs[0].click();
            }
        """)
        await asyncio.sleep(2)

        # Fill document type via JS
        await page.evaluate(f"""
            () => {{
                const inputs = document.querySelectorAll('input');
                for (const inp of inputs) {{
                    const id = (inp.id || '').toLowerCase();
                    const name = (inp.name || '').toLowerCase();
                    const ph = (inp.placeholder || '').toLowerCase();
                    if (id.includes('doc') || name.includes('doc') || ph.includes('type')) {{
                        inp.value = '{code}';
                        inp.dispatchEvent(new Event('input', {{bubbles:true}}));
                        inp.dispatchEvent(new Event('change', {{bubbles:true}}));
                        return;
                    }}
                }}
            }}
        """)
        await asyncio.sleep(1)

        # Fill dates via JS
        await page.evaluate(f"""
            () => {{
                const inputs = document.querySelectorAll('input[type="text"], input[type="date"]');
                let startDone = false;
                for (const inp of inputs) {{
                    const id = (inp.id || '').toLowerCase();
                    const name = (inp.name || '').toLowerCase();
                    const ph = (inp.placeholder || '').toLowerCase();
                    if (!startDone && (id.includes('start') || name.includes('start') || ph.includes('start') || ph.includes('from'))) {{
                        inp.value = '{date_from}';
                        inp.dispatchEvent(new Event('input', {{bubbles:true}}));
                        inp.dispatchEvent(new Event('change', {{bubbles:true}}));
                        startDone = true;
                    }} else if (startDone && (id.includes('end') || name.includes('end') || ph.includes('end') || ph.includes('to'))) {{
                        inp.value = '{date_to}';
                        inp.dispatchEvent(new Event('input', {{bubbles:true}}));
                        inp.dispatchEvent(new Event('change', {{bubbles:true}}));
                        return;
                    }}
                }}
            }}
        """)
        await asyncio.sleep(1)

        # Submit via JS
        await page.evaluate("""
            () => {
                const btns = document.querySelectorAll('button[type="submit"], input[type="submit"], button');
                for (const btn of btns) {
                    if (/search/i.test(btn.innerText || btn.value || '')) {
                        btn.click();
                        return;
                    }
                }
            }
        """)
        await page.wait_for_load_state("networkidle", timeout=15_000)
        await asyncio.sleep(2)

        content = await page.content()

        for data in captured:
            records = _parse_api_results(data, cat_key, cat_label)
            if records:
                return records

        records = _parse_clerk_html(content, cat_key, cat_label)
        if records:
            return records

        records = _extract_json_from_page(content, cat_key, cat_label)
        if records:
            return records

    except Exception as exc:
        log.warning("Form strategy failed for %s: %s", code, exc)

    return []


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 – PARCEL DATA (Dallas CAD)
# ═══════════════════════════════════════════════════════════════════════════

def _session_with_retries() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return sess


def _download_with_retry(sess, url, **kwargs):
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


def download_parcel_dbf() -> dict:
    log.info("=== Downloading Dallas CAD parcel DBF ===")
    sess = _session_with_retries()

    resp = _download_with_retry(sess, CAD_URL)
    if resp is None:
        log.warning("Cannot reach Dallas CAD; parcel lookup will be empty.")
        return {}

    soup = BeautifulSoup(resp.text, "lxml")
    dbf_url = None

    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        text = a.get_text(strip=True).lower()
        if any(k in href or k in text for k in ["parcel","appraisal","res_","residential","all_prop"]):
            if any(ext in href for ext in [".zip",".dbf"]):
                dbf_url = a["href"] if a["href"].startswith("http") \
                    else "https://www.dallascad.org/" + a["href"].lstrip("/")
                break

    if not dbf_url:
        for a in soup.find_all("a", href=True):
            if "__doPostBack" in (a.get("href") or ""):
                text = a.get_text(strip=True).lower()
                if any(k in text for k in ["parcel","appraisal","property","res","download"]):
                    m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", a["href"])
                    if m:
                        viewstate = soup.find("input", {"id":"__VIEWSTATE"})
                        eventval  = soup.find("input", {"id":"__EVENTVALIDATION"})
                        post_data = {
                            "__EVENTTARGET":     m.group(1),
                            "__EVENTARGUMENT":   m.group(2),
                            "__VIEWSTATE":       viewstate["value"] if viewstate else "",
                            "__EVENTVALIDATION": eventval["value"] if eventval else "",
                        }
                        log.info("Trying __doPostBack: %s", m.group(1))
                        for attempt in range(1, MAX_RETRIES + 1):
                            try:
                                pr = sess.post(CAD_URL, data=post_data, timeout=180)
                                pr.raise_for_status()
                                ct = pr.headers.get("Content-Type","")
                                if "zip" in ct or "octet" in ct or len(pr.content) > 10_000:
                                    return _parse_parcel_zip(pr.content)
                            except Exception as exc:
                                log.warning("PostBack attempt %d failed: %s", attempt, exc)
                                if attempt < MAX_RETRIES:
                                    time.sleep(RETRY_WAIT * attempt)

    if dbf_url:
        log.info("Downloading parcel ZIP: %s", dbf_url)
        r = _download_with_retry(sess, dbf_url, stream=True)
        if r:
            return _parse_parcel_zip(r.content)

    log.warning("Could not locate parcel DBF; parcel lookup will be empty.")
    return {}


def _parse_parcel_zip(raw_bytes: bytes) -> dict:
    lookup = {}
    tmp_dir = ROOT / "scraper/tmp"
    try:
        buf = io.BytesIO(raw_bytes)
        if zipfile.is_zipfile(buf):
            buf.seek(0)
            with zipfile.ZipFile(buf) as zf:
                dbf_names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                if not dbf_names:
                    return {}
                zf.extract(dbf_names[0], tmp_dir)
                dbf_path = tmp_dir / dbf_names[0]
        else:
            dbf_path = tmp_dir / "parcels.dbf"
            dbf_path.write_bytes(raw_bytes)

        log.info("Parsing DBF: %s", dbf_path)
        lookup = _build_lookup_from_dbf(str(dbf_path))
        log.info("Parcel lookup: %d entries", len(lookup))
    except Exception as exc:
        log.error("DBF parse error: %s", exc)
    return lookup


def _build_lookup_from_dbf(dbf_path: str) -> dict:
    lookup = {}
    OWNER_COLS  = ["OWNER_NAME1","OWNERNAME1","OWNER1","OWNER"]
    MAIL_ADDR   = ["OWNER_ADDRESS_LINE2","MAIL_ADDR","MAILADDR","OWNER_ADDR"]
    MAIL_CITY   = ["OWNER_CITY","MAIL_CITY","MAILCITY","CITY"]
    MAIL_STATE  = ["OWNER_STATE","MAIL_STATE","MAILSTATE","STATE"]
    MAIL_ZIP    = ["OWNER_ZIPCODE","MAIL_ZIP","MAILZIP","ZIP"]
    PROP_NUM    = ["STREET_NUM","SITUS_NUM","PROP_NUM","STRNUM"]
    PROP_STREET = ["FULL_STREET_NAME","SITUS_STREET","PROP_STREET","STREET"]
    PROP_CITY   = ["PROPERTY_CITY","PROP_CITY","SITUSCITY","PCITY"]
    PROP_ZIP    = ["PROPERTY_ZIPCODE","PROP_ZIP","SITUSZIP","PZIP"]

    def pick(rec, cols):
        for c in cols:
            v = rec.get(c) or rec.get(c.lower()) or ""
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
                pnum = pick(rec, PROP_NUM)
                pstr = pick(rec, PROP_STREET)
                entry = {
                    "mail_address": pick(rec, MAIL_ADDR),
                    "mail_city":    pick(rec, MAIL_CITY),
                    "mail_state":   pick(rec, MAIL_STATE),
                    "mail_zip":     pick(rec, MAIL_ZIP),
                    "prop_address": f"{pnum} {pstr}".strip() if pnum and pstr else "",
                    "prop_city":    pick(rec, PROP_CITY),
                    "prop_zip":     pick(rec, PROP_ZIP),
                }
                for variant in _name_variants(owner_raw):
                    lookup[variant] = entry
            except Exception:
                continue
    except Exception as exc:
        log.error("DBF read error: %s", exc)
    return lookup


def _name_variants(name: str) -> list:
    name = name.strip().upper()
    variants = {name}
    clean = re.sub(r"\s+(LLC|INC|CORP|LTD|L\.L\.C\.|TRUST|ETAL|ET AL|ET UX)\.?$","",name).strip()
    variants.add(clean)
    if "," in clean:
        parts = [p.strip() for p in clean.split(",",1)]
        variants.add(f"{parts[0]}, {parts[1]}")
        variants.add(f"{parts[1]} {parts[0]}")
    else:
        parts = clean.split()
        if len(parts) >= 2:
            variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")
            variants.add(f"{parts[-1]} {' '.join(parts[:-1])}")
    return [v for v in variants if v]


def lookup_owner(name: str, parcel_lookup: dict) -> dict:
    if not name or not parcel_lookup:
        return {}
    for variant in _name_variants(name):
        if variant in parcel_lookup:
            return parcel_lookup[variant]
    return {}


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 – PARSERS
# ═══════════════════════════════════════════════════════════════════════════

def _parse_api_results(data: Any, cat_key: str, cat_label: str) -> list:
    records = []
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for key in ["results","instruments","records","data","items","hits","content"]:
            if key in data and isinstance(data[key], list):
                items = data[key]
                break

    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            doc_num  = str(item.get("instrumentNumber") or item.get("docNumber") or
                           item.get("recordingNumber") or item.get("id") or "")
            doc_type = str(item.get("documentType") or item.get("docType") or
                           item.get("instrumentType") or "")
            filed    = str(item.get("recordedDate") or item.get("filedDate") or
                           item.get("instrumentDate") or item.get("date") or "")
            grantor  = str(item.get("grantor") or item.get("grantorName") or
                           item.get("party1") or "")
            grantee  = str(item.get("grantee") or item.get("granteeName") or
                           item.get("party2") or "")
            legal    = str(item.get("legalDescription") or item.get("legal") or
                           item.get("description") or "")
            amount   = _parse_amount(item.get("amount") or item.get("consideration") or
                                     item.get("loanAmount") or "")
            link     = str(item.get("url") or item.get("imageUrl") or item.get("link") or "")
            if not link and doc_num:
                link = f"{CLERK_BASE}/results/index?doc={doc_num}"
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


def _extract_json_from_page(html: str, cat_key: str, cat_label: str) -> list:
    records = []
    soup = BeautifulSoup(html, "lxml")
    for script in soup.find_all("script"):
        text = script.string or ""
        state_match = re.search(
            r'(?:window\.__(?:INITIAL_STATE|DATA|RESULTS|STATE)__|var\s+\w+Data)\s*=\s*({.*?});',
            text, re.DOTALL
        )
        if state_match:
            try:
                data = json.loads(state_match.group(1))
                parsed = _parse_api_results(data, cat_key, cat_label)
                records.extend(parsed)
            except Exception:
                pass
    return records


def _parse_clerk_html(html: str, cat_key: str, cat_label: str) -> list:
    records = []
    soup = BeautifulSoup(html, "lxml")

    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not headers:
            continue
        if not any(k in " ".join(headers) for k in ["doc","instrument","grantor","type","date","record"]):
            continue

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue
            try:
                row = {}
                for i, td in enumerate(cells):
                    key = headers[i] if i < len(headers) else str(i)
                    row[key] = td.get_text(strip=True)

                link = ""
                a_tag = tr.find("a", href=True)
                if a_tag:
                    href = a_tag["href"]
                    link = href if href.startswith("http") else CLERK_BASE + "/" + href.lstrip("/")

                def col(*keys):
                    for k in keys:
                        for hk in row:
                            if k in hk:
                                return row[hk]
                    return ""

                doc_num  = col("instrument","doc number","record","number")
                doc_type = col("type","document","doc type")
                filed    = col("date","filed","recorded")
                grantor  = col("grantor","owner","seller","party1")
                grantee  = col("grantee","buyer","party2")
                legal    = col("legal","description")
                amount   = _parse_amount(col("amount","consideration","value"))

                if not doc_num and not grantor:
                    continue

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
# SECTION 5 – SCORING & ENRICHMENT
# ═══════════════════════════════════════════════════════════════════════════

def build_flags(rec: dict, week_ago: str) -> list:
    flags = []
    cat   = rec.get("cat","")
    owner = (rec.get("owner") or "").upper()
    amt   = float(rec.get("amount") or 0)

    if cat == "LP":   flags.append("Lis pendens")
    if cat == "NOFC": flags.append("Pre-foreclosure")
    if cat == "PRO":  flags.append("Probate / estate")
    if amt > 0:       flags.append("Tax lien")
    if re.search(r"\b(LLC|INC|CORP|LTD|L\.L\.C\.|TRUST)\b", owner):
        flags.append("LLC / corp owner")
    if rec.get("filed","") >= week_ago:
        flags.append("New this week")
    return flags


def calc_score(rec: dict, flags: list) -> int:
    score = 30 + len(flags) * 10
    has_lp = any("lis pendens" in f.lower() for f in flags)
    has_fc = any("pre-foreclosure" in f.lower() for f in flags)
    if has_lp and has_fc: score += 20
    amt = float(rec.get("amount") or 0)
    if amt > 100_000:   score += 15
    elif amt > 50_000:  score += 10
    if "New this week" in flags: score += 5
    if rec.get("prop_address"):  score += 5
    return min(score, 100)


def enrich_record(raw: dict, parcel_lookup: dict, week_ago: str) -> dict:
    owner  = (raw.get("owner") or "").strip()
    parcel = lookup_owner(owner, parcel_lookup)
    prop_addr = parcel.get("prop_address","") or _extract_address_from_legal(raw.get("legal",""))

    rec = {
        "doc_num":      raw.get("doc_num",""),
        "doc_type":     raw.get("doc_type",""),
        "filed":        raw.get("filed",""),
        "cat":          raw.get("cat",""),
        "cat_label":    raw.get("cat_label",""),
        "owner":        owner,
        "grantee":      (raw.get("grantee") or "").strip(),
        "amount":       raw.get("amount") or 0,
        "legal":        (raw.get("legal") or "").strip(),
        "prop_address": prop_addr,
        "prop_city":    parcel.get("prop_city","") or "DALLAS",
        "prop_state":   "TX",
        "prop_zip":     parcel.get("prop_zip",""),
        "mail_address": parcel.get("mail_address",""),
        "mail_city":    parcel.get("mail_city",""),
        "mail_state":   parcel.get("mail_state","") or "TX",
        "mail_zip":     parcel.get("mail_zip",""),
        "clerk_url":    raw.get("clerk_url",""),
        "flags":        [],
        "score":        0,
    }
    flags        = build_flags(rec, week_ago)
    rec["flags"] = flags
    rec["score"] = calc_score(rec, flags)
    return rec


def _extract_address_from_legal(legal: str) -> str:
    if not legal:
        return ""
    m = re.search(r"\b(\d{1,5}\s+[A-Z][A-Z0-9 ,\.]+(?:ST|AVE|DR|RD|BLVD|LN|CT|WAY|PL|TRL|HWY)\b)", legal, re.I)
    return m.group(1).strip() if m else ""


def _parse_amount(val: Any):
    if val is None:
        return None
    s = re.sub(r"[^\d\.]","",str(val))
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _normalise_date(raw: str) -> str:
    if not raw:
        return ""
    for fmt in ("%m/%d/%Y","%Y-%m-%d","%m-%d-%Y","%d/%m/%Y","%m/%d/%y","%B %d, %Y","%b %d, %Y"):
        try:
            return datetime.strptime(raw.strip()[:10], fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return raw.strip()[:10]


def split_owner_name(name: str) -> tuple:
    name = re.sub(r"\s+(LLC|INC|CORP|LTD|TRUST|ETAL|ET AL|ET UX)\.?$","",name.strip(),flags=re.I).strip()
    if "," in name:
        parts = [p.strip() for p in name.split(",",1)]
        return parts[1], parts[0]
    parts = name.split()
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return name, ""


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6 – GHL CSV
# ═══════════════════════════════════════════════════════════════════════════

def export_ghl_csv(records: list, path: Path) -> None:
    GHL_COLS = [
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Motivated Seller Flags","Source","Public Records URL",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path,"w",newline="",encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=GHL_COLS)
        writer.writeheader()
        for rec in records:
            fname, lname = split_owner_name(rec.get("owner",""))
            writer.writerow({
                "First Name":             fname,
                "Last Name":              lname,
                "Mailing Address":        rec.get("mail_address",""),
                "Mailing City":           rec.get("mail_city",""),
                "Mailing State":          rec.get("mail_state",""),
                "Mailing Zip":            rec.get("mail_zip",""),
                "Property Address":       rec.get("prop_address",""),
                "Property City":          rec.get("prop_city",""),
                "Property State":         rec.get("prop_state","TX"),
                "Property Zip":           rec.get("prop_zip",""),
                "Lead Type":              rec.get("cat_label",""),
                "Document Type":          rec.get("doc_type",""),
                "Date Filed":             rec.get("filed",""),
                "Document Number":        rec.get("doc_num",""),
                "Amount/Debt Owed":       rec.get("amount",""),
                "Seller Score":           rec.get("score",0),
                "Motivated Seller Flags": "; ".join(rec.get("flags",[])),
                "Source":                 "Dallas County Clerk",
                "Public Records URL":     rec.get("clerk_url",""),
            })
    log.info("GHL CSV saved: %s (%d rows)", path, len(records))


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 7 – MAIN
# ═══════════════════════════════════════════════════════════════════════════

async def main() -> None:
    today        = datetime.now()
    week_ago     = today - timedelta(days=LOOK_BACK)
    date_from    = week_ago.strftime("%m/%d/%Y")
    date_to      = today.strftime("%m/%d/%Y")
    week_ago_iso = week_ago.strftime("%Y-%m-%d")

    log.info("━━━ Dallas County Motivated Seller Scraper v2 ━━━")
    log.info("Date range: %s → %s", date_from, date_to)

    # 1. Parcel lookup
    parcel_lookup = download_parcel_dbf()

    # 2. Try REST API first
    raw_records = scrape_via_api(date_from, date_to)

    # 3. Fall back to Playwright if needed
    if not raw_records:
        log.info("API returned 0 — falling back to Playwright...")
        raw_records = await scrape_via_playwright(date_from, date_to)

    # 4. Enrich
    enriched = []
    for raw in raw_records:
        try:
            enriched.append(enrich_record(raw, parcel_lookup, week_ago_iso))
        except Exception as exc:
            log.warning("Enrich failed for %s: %s", raw.get("doc_num","?"), exc)

    enriched.sort(key=lambda r: r.get("score",0), reverse=True)
    with_address = sum(1 for r in enriched if r.get("prop_address"))

    output = {
        "fetched_at":   today.isoformat(),
        "source":       "Dallas County Clerk + Dallas CAD",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(enriched),
        "with_address": with_address,
        "records":      enriched,
    }

    for dest in [ROOT/"dashboard/records.json", ROOT/"data/records.json"]:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
        log.info("Saved: %s", dest)

    csv_path = ROOT / "data" / f"ghl_export_{today.strftime('%Y%m%d')}.csv"
    export_ghl_csv(enriched, csv_path)

    log.info("━━━ Complete: %d records (%d with address) ━━━", len(enriched), with_address)


if __name__ == "__main__":
    asyncio.run(main())
