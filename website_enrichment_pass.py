"""
website_enrichment_pass.py — Controlled Google Maps website enrichment.

Loads the deduplicated discovery dataset, selects companies with no website,
and enriches them via Google Maps Text Search + Place Details (ALL fields).

Usage:
    python website_enrichment_pass.py              # first 240 companies (default)
    python website_enrichment_pass.py --limit 100  # custom batch size
    python website_enrichment_pass.py --limit 0    # all missing-website companies

Outputs:
    raw_master.json                  — full enriched records (with raw Maps data)
    discovery_snapshot_updated.csv   — analyst-readable flat view
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("website_enrichment_pass")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DISCOVERY_JSON   = Path("discovery_output.json")
RAW_MASTER_JSON  = Path("raw_master.json")
SNAPSHOT_CSV     = Path("discovery_snapshot_updated.csv")
DEFAULT_LIMIT    = 240

GOOGLE_PLACES_SEARCH_URL  = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# All available Places Details field groups
PLACE_DETAILS_FIELDS = ",".join([
    "name",
    "place_id",
    "formatted_address",
    "formatted_phone_number",
    "international_phone_number",
    "website",
    "rating",
    "user_ratings_total",
    "price_level",
    "types",
    "business_status",
    "opening_hours",
    "url",
    "vicinity",
    "geometry",
    "address_components",
    "adr_address",
    "utc_offset",
    "editorial_summary",
    "photos",
])


def _get_api_key() -> str:
    key = os.getenv("GOOGLE_MAPS_API_KEY", "")
    if not key:
        logger.error("GOOGLE_MAPS_API_KEY not set in .env")
        sys.exit(1)
    return key


# ---------------------------------------------------------------------------
# Google Maps helpers
# ---------------------------------------------------------------------------

def maps_text_search(client: httpx.Client, api_key: str, query: str) -> dict | None:
    """Run a Text Search and return the top result dict, or None."""
    try:
        resp = client.get(
            GOOGLE_PLACES_SEARCH_URL,
            params={"query": query, "region": "it", "key": api_key},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("Text Search failed for %r: %s", query, exc)
        return None

    results = data.get("results", [])
    if not results:
        return None
    return results[0]   # top hit only


def maps_place_details(client: httpx.Client, api_key: str, place_id: str) -> dict:
    """Fetch full Place Details for a place_id. Returns raw result dict."""
    try:
        resp = client.get(
            GOOGLE_PLACES_DETAILS_URL,
            params={"place_id": place_id, "fields": PLACE_DETAILS_FIELDS, "key": api_key},
            timeout=20,
        )
        resp.raise_for_status()
        return resp.json()   # full response, not just .result
    except Exception as exc:
        logger.warning("Place Details failed for %s: %s", place_id, exc)
        return {}


# ---------------------------------------------------------------------------
# JSON read / write helpers (atomic)
# ---------------------------------------------------------------------------

def load_json(path: Path) -> list[dict] | dict:
    if not path.exists():
        return []
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logger.error("Failed to load %s: %s", path, exc)
        return []


def save_json(path: Path, data) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# CSV snapshot
# ---------------------------------------------------------------------------

SNAPSHOT_FIELDS = [
    "company_name",
    "website",
    "location",
    "description",
    "europages_profile",
    "company_type_raw",
    "phone",
    "maps_rating",
    "maps_types",
    "maps_place_id",
    "source_list",
    "maps_enriched",
]


def save_snapshot(records: list[dict], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SNAPSHOT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            gm = r.get("google_maps", {})
            parsed = gm.get("parsed", {})
            sources = r.get("discovery_sources", [r.get("source", "")])
            writer.writerow({
                "company_name":      r.get("name", "") or r.get("company_name", ""),
                "website":           r.get("website", ""),
                "location":          r.get("location", ""),
                "description":       r.get("description", ""),
                "europages_profile": r.get("europages_profile", ""),
                "company_type_raw":  r.get("company_type_raw", ""),
                "phone":             parsed.get("phone", "") or r.get("phone", ""),
                "maps_rating":       parsed.get("rating", ""),
                "maps_types":        "|".join(parsed.get("types", [])),
                "maps_place_id":     gm.get("place_id", ""),
                "source_list":       "|".join(dict.fromkeys(s for s in sources if s)),
                "maps_enriched":     "yes" if gm else "no",
            })
    logger.info("Saved snapshot → %s (%d rows)", path, len(records))


# ---------------------------------------------------------------------------
# Main pass
# ---------------------------------------------------------------------------

def run(limit: int) -> None:
    api_key = _get_api_key()

    # 1. Load discovery dataset
    discovery: list[dict] = load_json(DISCOVERY_JSON)  # type: ignore
    if not discovery:
        logger.error("discovery_output.json is empty or missing. Run discovery first.")
        sys.exit(1)
    logger.info("Loaded %d companies from discovery_output.json", len(discovery))

    # 2. Filter: companies with no website
    missing = [c for c in discovery if not c.get("website")]
    logger.info("%d companies have no website", len(missing))

    if limit > 0:
        batch = missing[:limit]
    else:
        batch = missing
    logger.info("Processing batch of %d companies", len(batch))

    # 3. Load (or init) raw_master
    raw_master: list[dict] = load_json(RAW_MASTER_JSON)  # type: ignore
    existing_names = {r.get("company_name", r.get("name", "")) for r in raw_master}

    # 4. Stats counters
    maps_matched   = 0
    websites_found = 0
    processed      = 0

    with httpx.Client(timeout=25) as client:
        pbar = tqdm(batch, desc="Enriching websites", ncols=100, file=sys.stdout, mininterval=2.0)
        for idx, company in enumerate(pbar, 1):
            name     = (company.get("name") or "").strip()
            if name in existing_names:
                logger.debug("[%d/%d] Skipping %s (already in raw_master)", idx, len(batch), name)
                continue
            location = (company.get("location") or "").strip()

            query = f"{name} {location} Italy".strip()
            logger.info("[%d/%d] %s | query: %r", idx, len(batch), name, query)

            # --- Text Search ---
            time.sleep(0.4)
            top = maps_text_search(client, api_key, query)

            if not top:
                logger.info("  → No Maps result")
                record = _build_record(company, query, None, {})
                _upsert(raw_master, existing_names, record)
                processed += 1
                continue

            place_id = top.get("place_id", "")
            maps_matched += 1
            logger.info("  → place_id: %s", place_id)

            # --- Place Details (ALL fields) ---
            time.sleep(0.4)
            details_resp = maps_place_details(client, api_key, place_id)
            result       = details_resp.get("result", {})

            # --- Extract website ---
            website = result.get("website", "")
            if website:
                company["website"] = website
                websites_found += 1
                logger.info("  → Website found: %s", website)
            else:
                logger.info("  → No website in Place Details")

            # --- Build record ---
            record = _build_record(company, query, place_id, details_resp)
            _upsert(raw_master, existing_names, record)
            processed += 1

            # Persist after every company (append-only safety)
            save_json(RAW_MASTER_JSON, raw_master)

    # Final save
    save_json(RAW_MASTER_JSON, raw_master)

    # 5. Update discovery_output.json with resolved websites
    save_json(DISCOVERY_JSON, discovery)

    # 6. Save snapshot CSV (full dataset, not just batch)
    save_snapshot(discovery, SNAPSHOT_CSV)

    # 7. Summary
    still_missing = len(missing) - websites_found
    logger.info("=== WEBSITE ENRICHMENT PASS COMPLETE ===")
    logger.info("  Total processed:       %d", processed)
    logger.info("  Google Maps matches:   %d", maps_matched)
    logger.info("  Websites found:        %d", websites_found)
    logger.info("  Still missing website: %d", still_missing)
    logger.info("Outputs:")
    logger.info("  %s", RAW_MASTER_JSON)
    logger.info("  %s", SNAPSHOT_CSV)

    print("\n" + "="*50)
    print(f"  Total processed:       {processed}")
    print(f"  Google Maps matches:   {maps_matched}")
    print(f"  Websites found:        {websites_found}")
    print(f"  Still missing website: {still_missing}")
    print("="*50)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_record(
    company: dict,
    query: str,
    place_id: str | None,
    details_resp: dict,
) -> dict:
    """Build a raw_master record for this company."""
    result = details_resp.get("result", {})
    parsed = {
        "website":      result.get("website", ""),
        "primary_type": result.get("types", [None])[0] if result.get("types") else "",
        "address":      result.get("formatted_address", ""),
        "phone":        result.get("formatted_phone_number", ""),
        "rating":       result.get("rating"),
        "types":        result.get("types", []),
        "business_status": result.get("business_status", ""),
        "url":          result.get("url", ""),
        "vicinity":     result.get("vicinity", ""),
    }
    return {
        "company_name":  company.get("name", ""),
        "location":      company.get("location", ""),
        "website":       company.get("website", ""),
        "existing_data": {k: v for k, v in company.items()
                          if k not in ("discovery_hits",)},
        "google_maps": {
            "query":        query,
            "place_id":     place_id or "",
            "raw_response": details_resp,   # FULL — not truncated
            "parsed":       parsed,
        },
        "enrichment_pass": "website_enrichment_pass",
        "enrichment_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _upsert(master: list[dict], existing_names: set[str], record: dict) -> None:
    """Add record to master; skip if already present."""
    name = record.get("company_name", "")
    if name not in existing_names:
        master.append(record)
        existing_names.add(name)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Google Maps website enrichment pass for discovery dataset"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Max companies to process (default: {DEFAULT_LIMIT}; 0 = all)",
    )
    args = parser.parse_args()
    run(limit=args.limit)


if __name__ == "__main__":
    main()
