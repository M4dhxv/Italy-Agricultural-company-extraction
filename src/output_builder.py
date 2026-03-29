"""
output_builder.py — Build final_dataset.csv from raw_master.json.

Reads the lossless JSON and flattens each record into a CSV row.
Missing values are represented as empty strings (never null/NaN).
"""
from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any

from src.config import RAW_MASTER_PATH, FINAL_CSV_PATH

logger = logging.getLogger(__name__)

_FIELDNAMES = [
    "company_name",
    "location",
    "website",
    "description",
    "phone",
    "is_manufacturer",
    "is_agri_manufacturer",
    "confidence",
    "scrape_source",
    "primary_category",
    "secondary_categories",
    "category_counts_flat",
    "weak_product_signal",
    "products_detected_raw",
    "products_mapped",
    "products_mapped_count",
    "discovery_sources",
    "vat",
    "address",
]

def build_csv() -> int:
    """
    Read raw_master.json and write final_dataset.csv.

    Returns number of rows written.
    """
    raw_path = Path(RAW_MASTER_PATH)
    if not raw_path.exists():
        logger.error("raw_master.json not found — cannot build CSV")
        return 0

    with raw_path.open("r", encoding="utf-8") as f:
        records: list[dict] = json.load(f)

    rows: list[dict] = []
    skipped = 0
    for record in records:
        if record.get("rejected_reason"):
            skipped += 1
            continue
            
        row = _flatten(record)
        rows.append(row)

    logger.info("Output built. Excluded %d rejected records.", skipped)
    
    csv_path = Path(FINAL_CSV_PATH)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Built final_dataset.csv: %d rows", len(rows))
    return len(rows)

def _flatten(r: dict[str, Any]) -> dict[str, str]:
    """Flatten one raw JSON record to a CSV-ready dict."""

    sources = r.get("discovery_sources", [])
    products_raw = r.get("products_detected_raw", [])
    if isinstance(products_raw, str):
        products_raw = [products_raw]
    products_mapped = r.get("products_mapped", [])
    if isinstance(products_mapped, str):
        products_mapped = [products_mapped]
        
    sec_cats = r.get("secondary_categories", [])
    if isinstance(sec_cats, str):
        sec_cats = [sec_cats]
        
    counts = r.get("category_counts", {})
    counts_str = " | ".join(f"{k}:{v}" for k, v in counts.items()) if counts else ""

    return {
        "company_name": r.get("name") or r.get("company_name", ""),
        "location": r.get("location", ""),
        "website": r.get("website", ""),
        "description": r.get("description", "")[:500],
        "phone": r.get("phone", ""),
        "is_manufacturer": str(r.get("is_manufacturer", "")),
        "is_agri_manufacturer": str(r.get("is_agri_manufacturer", "")),
        "confidence": str(r.get("confidence", "")),
        "scrape_source": r.get("scrape_source", ""),
        "primary_category": r.get("primary_category", ""),
        "secondary_categories": "; ".join(sec_cats),
        "category_counts_flat": counts_str,
        "weak_product_signal": str(r.get("weak_product_signal", "")),
        "products_detected_raw": "; ".join(products_raw),
        "products_mapped": "; ".join(products_mapped),
        "products_mapped_count": str(len(products_mapped)),
        "discovery_sources": "; ".join(sources),
        "vat": r.get("vat", ""),
        "address": r.get("address", ""),
    }
