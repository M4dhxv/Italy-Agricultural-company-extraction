"""
discovery/europages_csv.py — Loads Europages data from a local CSV export.

Column mapping (actual CSV headers):
  company_name       ← "line-clamp-2"
  europages_profile  ← "flex href"
  location           ← "truncate 2"   (city is also available as "city")
  company_type_raw   ← "flex 4"
  description        ← "font-copy-400"
  website            ← empty string (not in CSV)
  source             ← "europages"

All rows are kept — no manufacturer filtering applied here.
Validation is delegated to the Gemini intelligence layer.
"""
from __future__ import annotations

import csv
import logging
import os
from pathlib import Path

from src.failure_logger import log_failure

logger = logging.getLogger(__name__)

_SOURCE_NAME = "europages"

# Default path — can be overridden via EUROPAGES_CSV_PATH env var
_DEFAULT_CSV_PATH = Path.home() / "Downloads" / "europages.csv"

# Exact header names as they appear in the CSV
_COL_NAME        = "line-clamp-2"
_COL_PROFILE     = "flex href"
_COL_LOCATION    = "truncate 2"
_COL_CITY        = "city"
_COL_TYPE        = "flex 4"
_COL_DESCRIPTION = "font-copy-400"


def discover() -> list[dict]:
    """
    Load Europages companies from local CSV export.

    Returns list of:
        {name, website, location, description, europages_profile,
         company_type_raw, source, source_url}
    """
    csv_path = Path(os.getenv("EUROPAGES_CSV_PATH", str(_DEFAULT_CSV_PATH)))

    if not csv_path.exists():
        log_failure(
            company_name="N/A",
            stage="discovery",
            source=_SOURCE_NAME,
            error_type="FileNotFound",
            error_message=f"Europages CSV not found at: {csv_path}",
        )
        logger.error("Europages CSV not found at: %s", csv_path)
        return []

    companies: list[dict] = []
    skipped = 0

    try:
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get(_COL_NAME) or "").strip()
                if not name:
                    skipped += 1
                    continue

                # Prefer "truncate 2" for location, fall back to "city"
                location = (row.get(_COL_LOCATION) or "").strip()
                if not location:
                    location = (row.get(_COL_CITY) or "").strip()

                # Clean up location — strip leading commas/spaces from raw value
                location = location.lstrip(", ").strip()

                profile = (row.get(_COL_PROFILE) or "").strip()
                company_type_raw = (row.get(_COL_TYPE) or "").strip()
                description = (row.get(_COL_DESCRIPTION) or "").strip()

                companies.append({
                    "name": name,
                    "website": "",                  # not available in CSV
                    "location": location,
                    "description": description,
                    "europages_profile": profile,
                    "company_type_raw": company_type_raw,
                    "source": _SOURCE_NAME,
                    "source_url": profile,          # EP profile as source_url
                })

    except Exception as exc:
        log_failure(
            company_name="N/A",
            stage="discovery",
            source=_SOURCE_NAME,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        logger.error("Europages CSV load failed: %s", exc)
        return []

    logger.info(
        "Europages CSV: loaded %d companies (%d skipped, no name)", len(companies), skipped
    )
    return companies
