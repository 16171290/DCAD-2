"""
Dallas CAD Parcel Export by Zip Code
Downloads the parcel CSV from Google Drive and exports a clean list
of all residential properties in the target zip codes.
Run: python scraper/export_parcels.py
Output: data/parcel_export_YYYYMMDD.csv
"""

import csv
import io
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("parcel_export")

ROOT = Path(__file__).resolve().parent.parent

# ── Target zip codes ─────────────────────────────────────────────────────────
TARGET_ZIPS = {
    "75006", "75040", "75041", "75042", "75043", "75044", "75048",
    "75080", "75081", "75220", "75228", "75229", "75234", "75238",
    "75240", "75243", "75248", "75252", "75254",
}

# ── Google Drive file ─────────────────────────────────────────────────────────
GDRIVE_FILE_ID = "1oQlyyab02U5kFJhsOZKE4esukfzSgY_D"
GDRIVE_URL     = f"https://drive.google.com/uc?export=download&id={GDRIVE_FILE_ID}"

# ── Output columns ────────────────────────────────────────────────────────────
OUTPUT_COLS = [
    "Owner Name",
    "Mailing Address",
    "Mailing City",
    "Mailing State",
    "Mailing Zip",
    "Property Address",
    "Property City",
    "Property Zip",
    "Last Transfer Date",
]


def clean_zip(z: str) -> str:
    """Trim 9-digit zip to 5 digits."""
    if not z:
        return ""
    z = z.strip().replace("-", "")
    return z[:5]


def download_parcel_csv() -> bytes:
    """Download parcel CSV from Google Drive."""
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    })

    for attempt in range(1, 4):
        try:
            log.info("Downloading parcel CSV attempt %d...", attempt)
            r = sess.get(GDRIVE_URL, timeout=120, allow_redirects=True)

            # Handle Google Drive virus scan confirmation for large files
            if "confirm=" in r.url or b"confirm=" in r.content[:500]:
                token = re.search(r'confirm=([0-9A-Za-z_\-]+)',
                                  r.text if hasattr(r, 'text') else "")
                if token:
                    r = sess.get(f"{GDRIVE_URL}&confirm={token.group(1)}",
                                 timeout=120)

            if b"download_warning" in r.content[:1000]:
                confirm = r.cookies.get("download_warning", "")
                if not confirm:
                    m = re.search(rb'confirm=([^&"]+)', r.content[:2000])
                    confirm = m.group(1).decode() if m else "t"
                r = sess.get(f"{GDRIVE_URL}&confirm={confirm}", timeout=120)

            if r.status_code == 200 and len(r.content) > 10_000:
                log.info("Downloaded %d bytes", len(r.content))
                return r.content
            else:
                log.warning("Bad response: status=%d size=%d",
                            r.status_code, len(r.content))

        except Exception as exc:
            log.warning("Attempt %d failed: %s", attempt, exc)
            if attempt < 3:
                time.sleep(4 * attempt)

    raise RuntimeError("Failed to download parcel CSV after 3 attempts")


def export_parcels():
    # Download
    raw = download_parcel_csv()

    # Parse CSV
    today     = datetime.now().strftime("%Y%m%d")
    out_path  = ROOT / "data" / f"parcel_export_{today}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    total_rows    = 0
    matched_rows  = 0
    zip_counts    = {z: 0 for z in TARGET_ZIPS}

    log.info("Filtering to target zip codes: %s", sorted(TARGET_ZIPS))

    with open(out_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=OUTPUT_COLS)
        writer.writeheader()

        text    = raw.decode("latin-1")
        reader  = csv.DictReader(io.StringIO(text))

        for row in reader:
            total_rows += 1

            # Check property zip against target list
            prop_zip = clean_zip(row.get("PROPERTY_ZIPCODE") or "")
            if prop_zip not in TARGET_ZIPS:
                continue

            matched_rows += 1
            zip_counts[prop_zip] = zip_counts.get(prop_zip, 0) + 1

            # Build property address
            street_num  = (row.get("STREET_NUM") or "").strip()
            street_name = (row.get("FULL_STREET_NAME") or "").strip()
            prop_addr   = f"{street_num} {street_name}".strip()

            writer.writerow({
                "Owner Name":        (row.get("OWNER_NAME1") or "").strip(),
                "Mailing Address":   (row.get("OWNER_ADDRESS_LINE2") or "").strip(),
                "Mailing City":      (row.get("OWNER_CITY") or "").strip(),
                "Mailing State":     (row.get("OWNER_STATE") or "").strip(),
                "Mailing Zip":       clean_zip(row.get("OWNER_ZIPCODE") or ""),
                "Property Address":  prop_addr,
                "Property City":     (row.get("PROPERTY_CITY") or "").strip(),
                "Property Zip":      prop_zip,
                "Last Transfer Date": (row.get("DEED_TXFR_DATE") or "").strip(),
            })

    log.info("━━━ Export Complete ━━━")
    log.info("Total rows scanned:  %d", total_rows)
    log.info("Rows in target zips: %d", matched_rows)
    log.info("Output: %s", out_path)
    log.info("Breakdown by zip:")
    for z in sorted(TARGET_ZIPS):
        if zip_counts.get(z, 0) > 0:
            log.info("  %s: %d", z, zip_counts[z])


if __name__ == "__main__":
    export_parcels()
