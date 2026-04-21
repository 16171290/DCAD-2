"""
Dallas County Motivated Seller Lead Scraper  v3
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Sources:
  LP   – dallas.tx.publicsearch.us  (REST API + Playwright, doc type "LP")
  NOFC – dallascounty.org/foreclosures PDF files by city/month  +
         dallas.tx.publicsearch.us dropdown "Foreclosure" (post Feb 24 2026)
  PRO  – courtsportal.dallascounty.org probate court search
  Parcel – dallascad.org bulk DBF for owner→address lookup
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

CLERK_BASE      = "https://dallas.tx.publicsearch.us"
FORECLOSURE_URL = "https://www.dallascounty.org/government/county-clerk/recording/foreclosures.php"
COURTS_BASE     = "https://courtsportal.dallascounty.org/DALLASPROD"
PROBATE_SEARCH  = "https://courtsportal.dallascounty.org/DALLASPROD/Home/Dashboard/29"
CAD_URL         = "https://www.dallascad.org/DataProducts.aspx"

LOOK_BACK   = 7
MAX_RETRIES = 3
RETRY_WAIT  = 4

ROOT = Path(__file__).resolve().parent.parent
for d in ["dashboard", "data", "scraper/tmp"]:
    (ROOT / d).mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 – LIS PENDENS  (dallas.tx.publicsearch.us)
# ═══════════════════════════════════════════════════════════════════════════

def scrape_lis_pendens(date_from: str, date_to: str) -> list:
    """
    Try REST API first; fall back to Playwright.
    LP is reliably on the publicsearch.us portal.
    """
    log.info("=== Scraping Lis Pendens ===")
    sess = _session_with_retries()

    try:
        dt_from = datetime.strptime(date_from, "%m/%d/%Y").strftime("%Y-%m-%d")
        dt_to   = datetime.strptime(date_to,   "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        dt_from, dt_to = date_from, date_to

    # Try known API endpoints
    for endpoint in [
        f"{CLERK_BASE}/api/instrument/search",
        f"{CLERK_BASE}/api/search/instrument",
        f"{CLERK_BASE}/search/api/instrument",
    ]:
        for code in ["LP", "LISP", "LIS PENDENS"]:
            for params in [
                {"countyId":"dallas","state":"TX","docTypeCode":code,
                 "startDate":dt_from,"endDate":dt_to,"page":1,"pageSize":500},
                {"county":"dallas","documentType":code,
                 "recordedDateFrom":dt_from,"recordedDateTo":dt_to,"page":1,"size":500},
            ]:
                try:
                    r = sess.get(endpoint, params=params, timeout=30)
                    if r.status_code == 200 and "json" in r.headers.get("Content-Type",""):
                        records = _parse_api_results(r.json(), "LP", "Lis Pendens")
                        if records:
                            log.info("LP API: %d records via %s code=%s", len(records), endpoint, code)
                            return records
                except Exception as exc:
                    log.debug("LP API attempt: %s", exc)

    log.info("LP: API returned 0, using Playwright...")
    return []   # Playwright fallback handled in main


async def scrape_lis_pendens_playwright(date_from: str, date_to: str) -> list:
    records = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage"]
        )
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width":1280,"height":900},
        )
        page = await ctx.new_page()
        captured: list = []

        async def on_response(resp):
            try:
                if any(k in resp.url for k in ["instrument","search","result"]):
                    if "json" in resp.headers.get("content-type",""):
                        data = await resp.json()
                        captured.append(data)
            except Exception:
                pass

        page.on("response", on_response)

        try:
            await page.goto(CLERK_BASE, wait_until="domcontentloaded", timeout=25_000)
            await asyncio.sleep(2)
        except Exception:
            pass

        for code in ["LP", "LISP"]:
            captured.clear()

            # Direct URL approach
            url = f"{CLERK_BASE}/results/index?docTypeCode={code}&startDate={date_from}&endDate={date_to}"
            try:
                await page.goto(url, wait_until="networkidle", timeout=20_000)
                await asyncio.sleep(2)

                for data in captured:
                    r = _parse_api_results(data, "LP", "Lis Pendens")
                    if r:
                        records.extend(r)

                if not records:
                    content = await page.content()
                    records = _parse_clerk_html(content, "LP", "Lis Pendens")
                    if not records:
                        records = _extract_json_from_page(content, "LP", "Lis Pendens")

                if records:
                    break
            except Exception as exc:
                log.warning("LP Playwright URL failed: %s", exc)

            # JS-injection form fill
            if not records:
                records = await _js_form_search(page, code, "LP", "Lis Pendens",
                                                 date_from, date_to, captured)
            if records:
                break

        await browser.close()

    log.info("LP Playwright: %d records", len(records))
    return records


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 – FORECLOSURES
# Two sources:
#   A) dallascounty.org PDF files (current month + next month)
#   B) dallas.tx.publicsearch.us "Foreclosure" dropdown (post Feb 24 2026)
# ═══════════════════════════════════════════════════════════════════════════

def scrape_foreclosures(date_from: str, date_to: str) -> list:
    log.info("=== Scraping Foreclosures ===")
    records = []

    # Source A: PDF files from dallascounty.org
    records.extend(_scrape_foreclosure_pdfs())

    # Source B: publicsearch.us "Foreclosure" search
    records.extend(_scrape_foreclosure_portal(date_from, date_to))

    # Deduplicate
    seen, deduped = set(), []
    for r in records:
        k = r.get("doc_num","") or r.get("owner","") + r.get("prop_address","")
        if k and k not in seen:
            seen.add(k)
            deduped.append(r)
        elif not k:
            deduped.append(r)

    log.info("Foreclosures total: %d", len(deduped))
    return deduped


def _scrape_foreclosure_pdfs() -> list:
    """
    Scrape dallascounty.org foreclosure PDF listing page.
    PDFs are organised by city and month. We grab current + next month PDFs,
    then parse each PDF for owner name and address using pdfplumber if available,
    otherwise store the PDF URL as the record link.
    """
    records = []
    sess = _session_with_retries()

    try:
        r = sess.get(FORECLOSURE_URL, timeout=30)
        r.raise_for_status()
    except Exception as exc:
        log.warning("Foreclosure PDF page fetch failed: %s", exc)
        return records

    soup = BeautifulSoup(r.text, "lxml")

    # Current month name and next month
    today = datetime.now()
    target_months = {today.strftime("%B"), (today + timedelta(days=32)).strftime("%B")}

    # Find all PDF links
    pdf_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower() and "foreclosure" in href.lower():
            full_url = href if href.startswith("http") else "https://www.dallascounty.org" + href
            # Check if it's for a target month
            for month in target_months:
                if month.lower() in full_url.lower() or month.lower() in a.get_text("").lower():
                    pdf_links.append(full_url)
                    break

    log.info("Foreclosure PDFs found: %d", len(pdf_links))

    for pdf_url in pdf_links:
        # Extract city name from URL
        city = re.sub(r'[-_]\d+\.pdf$', '', pdf_url.split("/")[-1]).replace("-", " ").replace("_", " ").strip()

        # Try to parse PDF text if pdfplumber is available
        pdf_records = _parse_foreclosure_pdf(pdf_url, city, sess)
        if pdf_records:
            records.extend(pdf_records)
        else:
            # At minimum store a placeholder record with the PDF link
            records.append({
                "doc_num":   pdf_url.split("/")[-1].replace(".pdf",""),
                "doc_type":  "Notice of Substitute Trustee Sale",
                "filed":     datetime.now().strftime("%Y-%m-%d"),
                "cat":       "NOFC",
                "cat_label": "Notice of Foreclosure",
                "owner":     "",
                "grantee":   "Substitute Trustee",
                "amount":    None,
                "legal":     f"See PDF: {city}",
                "prop_address": "",
                "clerk_url": pdf_url,
            })

    return records


def _parse_foreclosure_pdf(pdf_url: str, city: str, sess) -> list:
    """Download and parse a foreclosure notice PDF."""
    records = []

    try:
        import pdfplumber
    except ImportError:
        log.debug("pdfplumber not installed; storing PDF URL only")
        return records

    try:
        r = sess.get(pdf_url, timeout=60)
        r.raise_for_status()
        pdf_bytes = io.BytesIO(r.content)

        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                # Each notice in the PDF follows a pattern:
                # Grantor name, property address, legal description, trustee sale date
                # Split by common separators
                blocks = re.split(r'\n{2,}', text)
                for block in blocks:
                    if not block.strip():
                        continue
                    try:
                        lines = [l.strip() for l in block.split("\n") if l.strip()]
                        if len(lines) < 2:
                            continue

                        # Look for an address line
                        addr_line = ""
                        owner_line = ""
                        amount = None
                        for line in lines:
                            if re.search(r'\b\d{1,5}\s+[A-Z]', line) and not owner_line:
                                addr_line = line
                            elif re.search(r'^\s*[A-Z][A-Z\s,\.]+$', line) and not owner_line:
                                owner_line = line
                            amt_m = re.search(r'\$[\d,]+(?:\.\d{2})?', line)
                            if amt_m:
                                amount = _parse_amount(amt_m.group(0))

                        if not owner_line and lines:
                            owner_line = lines[0]

                        prop_city = city.title()
                        prop_addr = addr_line or ""

                        records.append({
                            "doc_num":    f"FC-{pdf_url.split('/')[-1].replace('.pdf','')}-{len(records)}",
                            "doc_type":   "Notice of Substitute Trustee Sale",
                            "filed":      datetime.now().strftime("%Y-%m-%d"),
                            "cat":        "NOFC",
                            "cat_label":  "Notice of Foreclosure",
                            "owner":      owner_line,
                            "grantee":    "Substitute Trustee",
                            "amount":     amount,
                            "legal":      " ".join(lines[:3]),
                            "prop_address": prop_addr,
                            "prop_city":  prop_city,
                            "clerk_url":  pdf_url,
                        })
                    except Exception:
                        continue

    except Exception as exc:
        log.warning("PDF parse error for %s: %s", pdf_url, exc)

    return records


def _scrape_foreclosure_portal(date_from: str, date_to: str) -> list:
    """
    dallas.tx.publicsearch.us has a 'Foreclosure' option in its dropdown
    (for records filed after Feb 24, 2026).
    """
    records = []
    sess = _session_with_retries()

    try:
        dt_from = datetime.strptime(date_from, "%m/%d/%Y").strftime("%Y-%m-%d")
        dt_to   = datetime.strptime(date_to,   "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        dt_from, dt_to = date_from, date_to

    # Try API with foreclosure-specific codes
    for endpoint in [f"{CLERK_BASE}/api/instrument/search", f"{CLERK_BASE}/api/search"]:
        for code in ["NSTS", "FORECLOSURE", "STS", "NOTS", "SUBTS", "TRUSTEE SALE"]:
            try:
                r = sess.get(endpoint, params={
                    "countyId":"dallas","docTypeCode":code,
                    "startDate":dt_from,"endDate":dt_to,"page":1,"pageSize":500,
                }, timeout=30)
                if r.status_code == 200 and "json" in r.headers.get("Content-Type",""):
                    parsed = _parse_api_results(r.json(), "NOFC", "Notice of Foreclosure")
                    if parsed:
                        log.info("Foreclosure portal API: %d records (code=%s)", len(parsed), code)
                        records.extend(parsed)
                        break
            except Exception:
                continue
        if records:
            break

    return records


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 – PROBATE
# Two sources:
#   A) courtsportal.dallascounty.org  – Odyssey "Smart Search" (Dashboard/29)
#      Uses Playwright to load the page, intercept the Odyssey API XHR calls,
#      and parse whatever JSON the portal fires when searching by date range
#      and court type = Probate.
#   B) dallas.tx.publicsearch.us  – recorded probate documents (Affidavit of
#      Heirship, Muniment of Title, etc.) searched by keyword because exact
#      Odyssey doc-type codes are not publicly documented.
# ═══════════════════════════════════════════════════════════════════════════

async def scrape_probate(date_from: str, date_to: str) -> list:
    log.info("=== Scraping Probate (re:SearchTX API) ===")

    try:
        dt_from = datetime.strptime(date_from, "%m/%d/%Y").strftime("%Y-%m-%d")
        dt_to   = datetime.strptime(date_to,   "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        dt_from, dt_to = date_from, date_to

    court_records    = _scrape_probate_researchTX(dt_from, dt_to)
    recorded_records = _scrape_probate_recorded(dt_from, dt_to)
    records = court_records + recorded_records

    log.info("Probate total: %d (%d court cases, %d recorded docs)",
             len(records), len(court_records), len(recorded_records))
    return records


def _scrape_probate_researchTX(dt_from: str, dt_to: str) -> list:
    """
    Call the re:SearchTX API directly.
    Endpoint: POST https://research.txcourts.gov/CourtRecordsSearch/search?timeZoneOffsetInMinutes=-300
    Auth: FedAuth cookie from RESEARCH_TX_COOKIE environment variable (GitHub Secret).
    Facet: Location = "dallas county - county clerk"
    We search a wildcard query and filter by filed date client-side since the API
    doesn't expose a date range param — instead we page through results sorted
    newest-first and stop when we're past our date window.
    """
    cookie = os.environ.get("RESEARCH_TX_COOKIE", "")
    if not cookie:
        log.warning("Probate: RESEARCH_TX_COOKIE secret not set — skipping re:SearchTX")
        return []

    records = []
    sess = _session_with_retries()
    sess.headers.update({
        "Content-Type":          "application/json",
        "Accept":                "application/json, text/plain, */*",
        "Cookie":                cookie,
        "Origin":                "https://research.txcourts.gov",
        "Referer":               "https://research.txcourts.gov/CourtRecordsSearch/ui/search?q=",
        "x-show-loading-spinner": "true",
        "User-Agent":            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                 "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    })

    url = "https://research.txcourts.gov/CourtRecordsSearch/search?timeZoneOffsetInMinutes=-300"

    # Probate case types filed in Dallas County clerk
    # We search using wildcard and filter to probate courts via facets
    # Then filter results by filed date
    probate_court_buckets = [
        "dallas county - county clerk",
    ]

    page_index = 0
    page_size  = 100
    found_old  = False  # flag to stop paging once we're past our date window

    while not found_old:
        payload = {
            "queryString":            "*",
            "pageSize":               page_size,
            "pageIndex":              page_index,
            "SearchIndexType":        "Cases",
            "hearingListView":        True,
            "isCalendarView":         False,
            "isNewestFirst":          True,
            "isReturningFromDetails": False,
            "sortFieldOrder":         0,
            "sortFields":             3,
            "selectedFacets": [
                {
                    "facetKey":        "Location",
                    "excludeSelf":     False,
                    "selectedBuckets": [{"bucketKey": k} for k in probate_court_buckets],
                },
                # NOTE: Case Category facet removed — bucket key was incorrect.
                # Location filter alone scopes to county clerk probate cases.
            ],
        }

        try:
            r = sess.post(url, json=payload, timeout=30)
            log.info("re:SearchTX page %d status: %d content-type: %s",
                     page_index, r.status_code, r.headers.get("Content-Type",""))

            if r.status_code == 401 or r.status_code == 403:
                log.warning("Probate: re:SearchTX cookie expired — update RESEARCH_TX_COOKIE secret")
                break
            if r.status_code != 200:
                log.warning("Probate: re:SearchTX returned %d body: %s",
                            r.status_code, r.text[:300])
                break

            if not r.text or not r.text.strip():
                log.warning("Probate: re:SearchTX returned empty body — cookie likely expired — update RESEARCH_TX_COOKIE secret")
                break

            try:
                data = r.json()
            except Exception as json_exc:
                log.warning("Probate: re:SearchTX JSON parse failed: %s — body snippet: %s",
                            json_exc, r.text[:400])
                break

            # Log top-level keys to understand response structure
            if isinstance(data, dict):
                keys = list(data.keys())
                log.info("re:SearchTX response keys: %s", keys)
                for k in keys:
                    v = data[k]
                    if isinstance(v, list):
                        log.info("  key='%s' list len=%d", k, len(v))
                    elif isinstance(v, dict):
                        log.info("  key='%s' dict keys=%s", k, list(v.keys())[:5])
                    else:
                        log.info("  key='%s' value=%s", k, str(v)[:80])
            elif isinstance(data, list):
                log.info("re:SearchTX response is list len=%d", len(data))

            cases = (data.get("cases") or data.get("results") or
                     data.get("data") or data.get("items") or
                     data.get("searchResults") or
                     (data.get("result") or {}).get("searchResults") or [])
            if isinstance(data, list):
                cases = data

            if not cases:
                log.info("Probate: no more cases at page %d", page_index)
                break

            log.info("Probate: page %d returned %d cases", page_index, len(cases))

            # Log the first case on page 0 so we can see field names
            if page_index == 0 and cases:
                try:
                    first = cases[0]
                    log.info("Probate case[0] type: %s  value: %s",
                             type(first).__name__, str(first)[:400])
                except Exception as e:
                    log.info("Probate case[0] log error: %s", e)

            for item in cases:
                if not isinstance(item, dict):
                    continue
                try:
                    filed = str(
                        item.get("filedDate") or item.get("fileDate") or
                        item.get("caseFiledDate") or item.get("date") or
                        item.get("FiledDate") or item.get("FileDate") or ""
                    )
                    filed_norm = _normalise_date(filed)

                    # Stop paging if we've gone past our date window
                    if filed_norm and filed_norm < dt_from:
                        log.info("Probate: reached case older than window (%s < %s), stopping",
                                 filed_norm, dt_from)
                        found_old = True
                        break

                    # Skip if outside our window — but only if we have a valid date
                    if filed_norm and filed_norm > dt_to:
                        continue
                    # If no date found, include the record anyway
                    if not filed_norm:
                        log.debug("Probate: case has no date, including anyway")

                    # Extract case details
                    case_num = str(
                        item.get("caseNumber") or item.get("causeNumber") or
                        item.get("id") or ""
                    )
                    style = str(
                        item.get("caseStyle") or item.get("style") or
                        item.get("name") or item.get("description") or ""
                    )
                    case_type = str(
                        item.get("caseType") or item.get("type") or "Probate"
                    )
                    court = str(item.get("court") or item.get("location") or "")

                    # Extract party name
                    owner = ""
                    parties = (item.get("parties") or item.get("partyList") or [])
                    if isinstance(parties, list):
                        for p in parties:
                            if isinstance(p, dict):
                                ptype = (p.get("partyType") or "").lower()
                                pname = (p.get("name") or p.get("partyName") or "")
                                if any(k in ptype for k in
                                       ["decedent","petitioner","applicant","testator","deceased"]):
                                    owner = pname
                                    break
                                elif not owner and pname:
                                    owner = pname
                    if not owner:
                        owner = style

                    link = str(item.get("url") or item.get("link") or "")
                    if not link and case_num:
                        link = (f"https://research.txcourts.gov/CourtRecordsSearch/"
                                f"ui/case/{case_num}/details")

                    if not case_num and not owner:
                        continue

                    records.append({
                        "doc_num":      case_num,
                        "doc_type":     case_type,
                        "filed":        filed_norm,
                        "cat":          "PRO",
                        "cat_label":    "Probate / Estate",
                        "owner":        owner,
                        "grantee":      court,
                        "amount":       _parse_amount(
                            item.get("amount") or item.get("estateValue") or ""
                        ),
                        "legal":        style,
                        "prop_address": "",
                        "clerk_url":    link,
                    })

                except Exception as exc:
                    log.debug("Probate case parse error: %s", exc)
                    continue

            page_index += 1
            # Safety cap — don't page forever
            if page_index > 20:
                break

        except Exception as exc:
            log.warning("Probate re:SearchTX request failed: %s", exc)
            break

    log.info("Probate re:SearchTX: %d records in date range", len(records))
    return records


def _parse_probate_html(html: str) -> list:
    """Parse probate case results from HTML table — Odyssey portal layout."""
    records = []
    soup = BeautifulSoup(html, "lxml")

    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        joined  = " ".join(headers)
        if not any(k in joined for k in ["case","party","filed","estate","probate","style","cause"]):
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
                    link = href if href.startswith("http") \
                        else COURTS_BASE + "/" + href.lstrip("/")

                def col(*keys):
                    for k in keys:
                        for hk in row:
                            if k in hk:
                                return row[hk]
                    return ""

                case_num = col("case","number","cause")
                filed    = col("filed","date","file")
                party    = col("style","party","petitioner","decedent","name","applicant")

                if not case_num and not party:
                    continue

                records.append({
                    "doc_num":      case_num,
                    "doc_type":     col("type","case type") or "Probate",
                    "filed":        _normalise_date(filed),
                    "cat":          "PRO",
                    "cat_label":    "Probate / Estate",
                    "owner":        party,
                    "grantee":      col("attorney","executor","admin","court"),
                    "amount":       _parse_amount(col("amount","value","estate")),
                    "legal":        col("style","description","legal"),
                    "prop_address": "",
                    "clerk_url":    link,
                })
            except Exception:
                continue
    return records


def _scrape_probate_recorded(dt_from: str, dt_to: str) -> list:
    """
    Search dallas.tx.publicsearch.us for probate-related RECORDED documents.
    Dallas County confirmed: Affidavit of Heirship is filed with the Recording
    Division (not probate court). We search by keyword since exact Odyssey
    doc-type codes are undocumented.

    Keywords tried against the portal's keyword/name search endpoint.
    """
    records = []
    sess = _session_with_retries()

    # Keyword searches against the recording portal
    # Each tuple: (search_keyword, human_label)
    keyword_searches = [
        ("HEIRSHIP",          "Affidavit of Heirship"),
        ("MUNIMENT",          "Muniment of Title"),
        ("LETTERS TESTAMENTARY", "Letters Testamentary"),
        ("LETTERS OF ADMINISTRATION", "Letters of Administration"),
        ("AFFIDAVIT OF HEIRSHIP", "Affidavit of Heirship"),
    ]

    # Also try direct doc-type code guesses for the recording portal
    # (Odyssey recording uses short codes; these are common TX county variants)
    code_searches = [
        ("AH",    "Affidavit of Heirship"),
        ("AOH",   "Affidavit of Heirship"),
        ("AFFH",  "Affidavit of Heirship"),
        ("MT",    "Muniment of Title"),
        ("MNT",   "Muniment of Title"),
        ("LT",    "Letters Testamentary"),
        ("LA",    "Letters of Administration"),
        ("PROB",  "Probate"),
    ]

    endpoints = [
        f"{CLERK_BASE}/api/instrument/search",
        f"{CLERK_BASE}/api/search/instrument",
    ]

    # Try doc-type codes first
    for code, label in code_searches:
        found = False
        for endpoint in endpoints:
            try:
                r = sess.get(endpoint, params={
                    "countyId":  "dallas",
                    "docTypeCode": code,
                    "startDate": dt_from,
                    "endDate":   dt_to,
                    "page":      1,
                    "pageSize":  500,
                }, timeout=20)
                if r.status_code == 200 and "json" in r.headers.get("Content-Type", ""):
                    parsed = _parse_api_results(r.json(), "PRO", f"Probate / Estate ({label})")
                    if parsed:
                        log.info("Probate recording portal: %d records (code=%s)", len(parsed), code)
                        records.extend(parsed)
                        found = True
                        break
            except Exception as exc:
                log.debug("Probate code search %s: %s", code, exc)
        if found:
            break  # One working code is enough

    # Try keyword searches if codes yielded nothing
    if not records:
        for keyword, label in keyword_searches:
            for endpoint in endpoints:
                try:
                    # Some publicsearch.us instances support a 'keyword' or 'name' param
                    for param_name in ["keyword", "name", "grantorName", "searchTerm", "q"]:
                        r = sess.get(endpoint, params={
                            "countyId":  "dallas",
                            param_name:  keyword,
                            "startDate": dt_from,
                            "endDate":   dt_to,
                            "page":      1,
                            "pageSize":  200,
                        }, timeout=20)
                        if r.status_code == 200 and "json" in r.headers.get("Content-Type", ""):
                            parsed = _parse_api_results(r.json(), "PRO", f"Probate / Estate ({label})")
                            if parsed:
                                log.info("Probate keyword '%s': %d records", keyword, len(parsed))
                                records.extend(parsed)
                                break
                except Exception as exc:
                    log.debug("Probate keyword search %s: %s", keyword, exc)

    log.info("Probate recorded docs: %d", len(records))
    return records


def _scrape_probate_recording_portal(dt_from: str, dt_to: str) -> list:
    """Legacy shim — calls the new function."""
    return _scrape_probate_recorded(dt_from, dt_to)


def _parse_probate_api(data: Any) -> list:
    """Legacy shim — try both Odyssey and generic parsers."""
    result = _parse_odyssey_response(data)
    if not result:
        result = _parse_api_results(data, "PRO", "Probate / Estate")
    return result


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 – PARCEL DATA (Dallas CAD)
# ═══════════════════════════════════════════════════════════════════════════

def _session_with_retries() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
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
                if any(k in text for k in ["parcel","appraisal","property","download"]):
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
                        for attempt in range(1, MAX_RETRIES + 1):
                            try:
                                pr = sess.post(CAD_URL, data=post_data, timeout=180)
                                pr.raise_for_status()
                                ct = pr.headers.get("Content-Type","")
                                if "zip" in ct or "octet" in ct or len(pr.content) > 10_000:
                                    return _parse_parcel_zip(pr.content)
                            except Exception as exc:
                                log.warning("PostBack attempt %d: %s", attempt, exc)
                                if attempt < MAX_RETRIES:
                                    time.sleep(RETRY_WAIT * attempt)

    if dbf_url:
        log.info("Downloading parcel ZIP: %s", dbf_url)
        r = _download_with_retry(sess, dbf_url, stream=True)
        if r:
            return _parse_parcel_zip(r.content)

    log.warning("Could not locate parcel DBF.")
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
# SECTION 5 – PARSERS (shared)
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
        m = re.search(
            r'(?:window\.__(?:INITIAL_STATE|DATA|RESULTS|STATE)__|var\s+\w+Data)\s*=\s*({.*?});',
            text, re.DOTALL
        )
        if m:
            try:
                data = json.loads(m.group(1))
                records.extend(_parse_api_results(data, cat_key, cat_label))
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

                doc_num = col("instrument","doc number","record","number")
                grantor = col("grantor","owner","seller","party1")
                if not doc_num and not grantor:
                    continue
                records.append({
                    "doc_num":   doc_num,
                    "doc_type":  col("type","document","doc type") or cat_label,
                    "filed":     _normalise_date(col("date","filed","recorded")),
                    "cat":       cat_key,
                    "cat_label": cat_label,
                    "owner":     grantor,
                    "grantee":   col("grantee","buyer","party2"),
                    "amount":    _parse_amount(col("amount","consideration","value")),
                    "legal":     col("legal","description"),
                    "clerk_url": link,
                })
            except Exception:
                continue
    return records


async def _js_form_search(page, code, cat_key, cat_label, date_from, date_to, captured) -> list:
    """Fill search form via JavaScript injection."""
    records = []
    try:
        await page.evaluate(f"""
            () => {{
                const tabs = document.querySelectorAll(
                    'li[data-tourid], li.workspaces__tab, [class*="workspaces__tab"]'
                );
                for (const tab of tabs) {{
                    if (/search|document/i.test(tab.innerText||'')) {{ tab.click(); return; }}
                }}
                if (tabs.length) tabs[0].click();
            }}
        """)
        await asyncio.sleep(2)

        await page.evaluate(f"""
            () => {{
                for (const inp of document.querySelectorAll('input')) {{
                    const id = (inp.id||'').toLowerCase();
                    const name = (inp.name||'').toLowerCase();
                    const ph = (inp.placeholder||'').toLowerCase();
                    if (id.includes('doc')||name.includes('doc')||ph.includes('type')) {{
                        inp.value = '{code}';
                        inp.dispatchEvent(new Event('input',{{bubbles:true}}));
                        inp.dispatchEvent(new Event('change',{{bubbles:true}}));
                        return;
                    }}
                }}
            }}
        """)
        await asyncio.sleep(0.5)

        await page.evaluate(f"""
            () => {{
                const inputs = document.querySelectorAll('input[type="text"],input[type="date"]');
                let startDone = false;
                for (const inp of inputs) {{
                    const id = (inp.id||'').toLowerCase();
                    const name = (inp.name||'').toLowerCase();
                    const ph = (inp.placeholder||'').toLowerCase();
                    if (!startDone && (id.includes('start')||name.includes('start')||ph.includes('start')||ph.includes('from'))) {{
                        inp.value = '{date_from}';
                        inp.dispatchEvent(new Event('input',{{bubbles:true}}));
                        inp.dispatchEvent(new Event('change',{{bubbles:true}}));
                        startDone = true;
                    }} else if (startDone && (id.includes('end')||name.includes('end')||ph.includes('end')||ph.includes('to'))) {{
                        inp.value = '{date_to}';
                        inp.dispatchEvent(new Event('input',{{bubbles:true}}));
                        inp.dispatchEvent(new Event('change',{{bubbles:true}}));
                        return;
                    }}
                }}
            }}
        """)
        await asyncio.sleep(0.5)

        await page.evaluate("""
            () => {
                for (const btn of document.querySelectorAll('button,input[type=submit]')) {
                    if (/search/i.test(btn.innerText||btn.value||'')) { btn.click(); return; }
                }
            }
        """)
        await page.wait_for_load_state("networkidle", timeout=15_000)
        await asyncio.sleep(2)

        for data in captured:
            r = _parse_api_results(data, cat_key, cat_label)
            if r:
                records.extend(r)

        if not records:
            content = await page.content()
            records = _parse_clerk_html(content, cat_key, cat_label)
            if not records:
                records = _extract_json_from_page(content, cat_key, cat_label)

    except Exception as exc:
        log.warning("JS form search failed for %s: %s", code, exc)

    return records


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6 – SCORING & ENRICHMENT
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
    if amt > 100_000:  score += 15
    elif amt > 50_000: score += 10
    if "New this week" in flags: score += 5
    if rec.get("prop_address"):  score += 5
    return min(score, 100)


def enrich_record(raw: dict, parcel_lookup: dict, week_ago: str) -> dict:
    owner  = (raw.get("owner") or "").strip()
    parcel = lookup_owner(owner, parcel_lookup)
    prop_addr = (raw.get("prop_address","") or
                 parcel.get("prop_address","") or
                 _extract_address_from_legal(raw.get("legal","")))

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
        "prop_city":    raw.get("prop_city","") or parcel.get("prop_city","") or "DALLAS",
        "prop_state":   "TX",
        "prop_zip":     raw.get("prop_zip","") or parcel.get("prop_zip",""),
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
# SECTION 7 – GHL CSV
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
    log.info("GHL CSV: %s (%d rows)", path, len(records))


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 8 – MAIN
# ═══════════════════════════════════════════════════════════════════════════

async def main() -> None:
    today        = datetime.now()
    week_ago     = today - timedelta(days=LOOK_BACK)
    date_from    = week_ago.strftime("%m/%d/%Y")
    date_to      = today.strftime("%m/%d/%Y")
    week_ago_iso = week_ago.strftime("%Y-%m-%d")

    log.info("━━━ Dallas County Motivated Seller Scraper v3 ━━━")
    log.info("Date range: %s → %s", date_from, date_to)

    # 1. Parcel lookup
    parcel_lookup = download_parcel_dbf()

    # 2. LP – try REST API; Playwright if needed
    lp_records = scrape_lis_pendens(date_from, date_to)
    if not lp_records:
        lp_records = await scrape_lis_pendens_playwright(date_from, date_to)

    # 3. NOFC – PDF scraper + portal
    nofc_records = scrape_foreclosures(date_from, date_to)

    # 4. PRO – courts portal + recorded docs
    pro_records = await scrape_probate(date_from, date_to)

    # 5. Combine & deduplicate
    all_raw = lp_records + nofc_records + pro_records
    seen, deduped = set(), []
    for r in all_raw:
        k = r.get("doc_num","")
        if k and k not in seen:
            seen.add(k)
            deduped.append(r)
        elif not k:
            deduped.append(r)

    log.info("Combined: LP=%d  NOFC=%d  PRO=%d  Total=%d",
             len(lp_records), len(nofc_records), len(pro_records), len(deduped))

    # 6. Enrich
    enriched = []
    for raw in deduped:
        try:
            enriched.append(enrich_record(raw, parcel_lookup, week_ago_iso))
        except Exception as exc:
            log.warning("Enrich failed for %s: %s", raw.get("doc_num","?"), exc)

    enriched.sort(key=lambda r: r.get("score",0), reverse=True)
    with_address = sum(1 for r in enriched if r.get("prop_address"))

    output = {
        "fetched_at":   today.isoformat(),
        "source":       "Dallas County Clerk + Dallas CAD + Courts Portal",
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
