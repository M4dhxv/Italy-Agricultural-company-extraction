"""
run_pipeline.py — Main orchestrator for the ATECO 28.30 Italian agricultural
machinery pipeline.

Usage:
    python run_pipeline.py --restart    # Fresh run (wipes state)
    python run_pipeline.py --resume     # Resume from last checkpoint
    python run_pipeline.py --csv-only   # Rebuild CSV from existing raw_master.json

The pipeline follows the 16-step execution flow defined in the spec:
  1. Discover companies (multi-source)
  2. Resolve websites
  3. Scrape targeted pages
  4. Extract products
  5. Filter (manufacturer + product match)
  6. Extract financials
  7. Extract directors
  8. Extract ownership
  9. Apply employee flag
  10. Save raw JSON (append-only, after each company)
  11. Update state checkpoint
  12. Generate CSV at end
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

from dotenv import load_dotenv
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

# ---------------------------------------------------------------------------
# Load .env before any src imports that read env vars
# ---------------------------------------------------------------------------
load_dotenv()

from src.config import (
    RAW_MASTER_PATH,
    FINAL_CSV_PATH,
    FAILURES_CSV_PATH,
    SMALL_COMPANY_THRESHOLD,
)
from src.state_manager import load_state, save_state, reset_state
from src.failure_logger import log_failure
from src.http_client import close_client

# Discovery
from src.discovery import europages_csv
from src.discovery import google_maps, google_search

from src.website_resolver import resolve_website
from src.scraper import scrape_company
from src.intelligence_layer import analyze_company
from src.product_extractor import extract_products

# Output
from src.output_builder import build_csv

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# Raw master JSON helpers (append-only)
# ---------------------------------------------------------------------------

def _load_raw_master() -> list[dict]:
    path = Path(RAW_MASTER_PATH)
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        logger.warning("raw_master.json is corrupt or unreadable — starting fresh")
        return []


def _save_raw_master(records: list[dict]) -> None:
    """Atomic overwrite of raw_master.json."""
    path = Path(RAW_MASTER_PATH)
    tmp_path = path.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _append_to_raw_master(record: dict) -> None:
    """Load → append → save.  Called after each company."""
    records = _load_raw_master()
    records.append(record)
    _save_raw_master(records)


# ---------------------------------------------------------------------------
# Discovery: gather all candidate companies
# ---------------------------------------------------------------------------

def _discover_all() -> list[dict]:
    """
    Run all discovery sources, union-merge duplicates, enrich missing websites,
    and save discovery_output.json + discovery_snapshot.csv.
    """
    from urllib.parse import urlparse

    logger.info("=== DISCOVERY PHASE ===")

    # ------------------------------------------------------------------ #
    # 1. Collect raw results from each source
    # ------------------------------------------------------------------ #
    logger.info("  Source: Europages (CSV)")
    ep_results = europages_csv.discover()
    ep_count = len(ep_results)
    logger.info("  Europages loaded: %d", ep_count)

    logger.info("  Source: Google Maps (Places API — city expansion)")
    maps_results = google_maps.discover()
    maps_count = len(maps_results)
    logger.info("  Google Maps: %d", maps_count)

    logger.info("  Source: Google Search (via Apify)")
    search_results = google_search.discover()
    search_count = len(search_results)
    logger.info("  Google Search: %d", search_count)

    all_raw = ep_results + maps_results + search_results

    # ------------------------------------------------------------------ #
    # 2. Union-merge deduplication
    #    Primary key:  website domain (if available)
    #    Secondary key: normalised company name
    #    On match → merge ALL fields, never overwrite existing value with ""
    # ------------------------------------------------------------------ #
    def _domain(url: str) -> str:
        try:
            return urlparse(url).netloc.lower().replace("www.", "").strip()
        except Exception:
            return ""

    unique: list[dict] = []
    domain_index: dict[str, int] = {}   # domain  → index in unique
    name_index:   dict[str, int] = {}   # nname   → index in unique

    for c in all_raw:
        nname  = c.get("name", "").lower().strip()
        raw_web = c.get("website", "")
        dom    = _domain(raw_web) if raw_web else ""

        # Find match
        idx = -1
        if dom and dom in domain_index:
            idx = domain_index[dom]
        elif nname and nname in name_index:
            idx = name_index[nname]

        hit = _make_hit(c)

        if idx >= 0:
            # Union-merge: fill any empty fields from incoming record
            existing = unique[idx]
            for key, val in c.items():
                if key in ("discovery_hits", "discovery_sources"):
                    continue
                if val and not existing.get(key):
                    existing[key] = val
            existing.setdefault("discovery_hits", []).append(hit)
            if c.get("source") not in existing.get("discovery_sources", []):
                existing.setdefault("discovery_sources", []).append(c.get("source", ""))
            # Update indexes if we just acquired a domain
            if dom and dom not in domain_index:
                domain_index[dom] = idx
        else:
            # New entry
            new_idx = len(unique)
            c["discovery_hits"]    = [hit]
            c["discovery_sources"] = [c.get("source", "")]
            unique.append(c)
            if dom:
                domain_index[dom] = new_idx
            if nname:
                name_index[nname] = new_idx

    logger.info("After deduplication: %d unique companies", len(unique))

    # ------------------------------------------------------------------ #
    # 3. Summary stats  (website enrichment handled by website_enrichment_pass.py)
    # ------------------------------------------------------------------ #
    with_website    = sum(1 for c in unique if c.get("website"))
    without_website = len(unique) - with_website

    logger.info("=== DISCOVERY SUMMARY ===")
    logger.info("  Europages loaded:          %d", ep_count)
    logger.info("  Google Maps discovered:    %d", maps_count)
    logger.info("  Google Search discovered:  %d", search_count)
    logger.info("  Final unique companies:    %d", len(unique))
    logger.info("  With website:              %d", with_website)
    logger.info("  Without website:           %d", without_website)

    # ------------------------------------------------------------------ #
    # 5. Save outputs
    # ------------------------------------------------------------------ #
    # discovery_output.json (full records for enrichment phase)
    with open("discovery_output.json", "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)
    logger.info("Saved discovery_output.json")

    # discovery_snapshot.csv (analyst-readable flat view)
    _save_discovery_snapshot(unique)
    logger.info("Saved discovery_snapshot.csv")

    return unique


def _save_discovery_snapshot(companies: list[dict]) -> None:
    """Write a flat CSV snapshot of discovery results."""
    fieldnames = [
        "company_name",
        "website",
        "location",
        "description",
        "europages_profile",
        "source_list",
    ]
    with open("discovery_snapshot.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for c in companies:
            sources = c.get("discovery_sources", [c.get("source", "")])
            writer.writerow({
                "company_name":      c.get("name", ""),
                "website":           c.get("website", ""),
                "location":          c.get("location", ""),
                "description":       c.get("description", ""),
                "europages_profile": c.get("europages_profile", ""),
                "source_list":       "|" .join(dict.fromkeys(s for s in sources if s)),
            })


def _make_hit(c: dict) -> dict:
    return {
        "source": c.get("source", ""),
        "source_url": c.get("source_url", ""),
    }


# ---------------------------------------------------------------------------
# Per-company enrichment pipeline
# ---------------------------------------------------------------------------

def _enrich_company(company: dict, idx: int) -> dict | None:
    """
    Run all enrichment stages for one company.

    Returns enriched record dict, or None if the company should be discarded.
    """
    name = company.get("name", "").strip()
    website = company.get("website", "").strip()

    logger.info("[%d] Processing: %s", idx, name)

    # ------------------------------------------------------------------ #
    # STAGE 2: Website resolution (mandatory)
    # ------------------------------------------------------------------ #
    if not website:
        logger.info("  → Resolving website for %r", name)
        website = resolve_website(name) or ""
        company["website"] = website
        
    rejected_reason = None
    if not website:
        log_failure(
            company_name=name,
            stage="website_resolution",
            source="multi_search",
            error_type="NoWebsite",
            error_message="No official website found - skipping enrichment",
        )
        logger.warning("  → SKIPPED ENRICHMENT (no website): %s", name)
        rejected_reason = "no_website"
    
    scraped_pages: dict[str, str] = {}
    scrape_status = "unattempted"
    scrape_source = "none"
    
    if not rejected_reason:
        logger.info("  → Scraping website: %s", website)
        try:
            scraped_pages, scrape_source = scrape_company(website, company_name=name)
            if scrape_source == "none":
                scrape_status = "failed"
            else:
                scrape_status = "success"
        except Exception as exc:
            scrape_source = "none"
            scrape_status = "failed"
            log_failure(
                company_name=name,
                stage="scraping",
                source=website,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            logger.warning("  → Scraping failed for %s: %s", name, exc)
    
    ai_result = {
        "is_manufacturer": False,
        "is_agri_manufacturer": False,
        "products_detected_raw": [],
        "confidence": 0.0,
        "ai_input_text": "",
        "evidence": "",
    }
    
    if not rejected_reason and scrape_status == "success":
        logger.info("  → Gemini Intelligence Layer Analysis")
        ai_result = analyze_company(name, website, scraped_pages)
        scrape_status = ai_result.get("scrape_status", scrape_status)
        
        if scrape_status in ["failed", "skipped_directory"]:
            rejected_reason = "scrape_failed"
        elif not ai_result.get("is_manufacturer", False):
            rejected_reason = "not_manufacturer"
            logger.info("  → REJECTED (not manufacturer): %s", name)
        elif not ai_result.get("is_agri_manufacturer", False):
            rejected_reason = "not_agri_manufacturer"
            logger.info("  → REJECTED (not agri manufacturer): %s", name)
        elif ai_result.get("confidence", 0.0) < 0.4:
            rejected_reason = "low_confidence"
            logger.info("  → REJECTED (low confidence %.2f): %s", ai_result.get("confidence", 0.0), name)
    
    products_detected_raw = []
    products_mapped = []
    category_counts = {}
    primary_category = ""
    secondary_categories = []
    weak_product_signal = False
    unmapped_products_count = 0
    
    if not rejected_reason:
        norm_result = extract_products(ai_result.get("products_detected_raw", []))
        products_detected_raw = norm_result["products_detected_raw"]
        products_mapped = norm_result["products_mapped"]
        category_counts = norm_result["category_counts"]
        primary_category = norm_result["primary_category"]
        secondary_categories = norm_result["secondary_categories"]
        weak_product_signal = norm_result["weak_product_signal"]
        unmapped_products_count = norm_result["unmapped_products_count"]

        logger.info("  → Products Raw: %s", products_detected_raw[:5])
        logger.info("  → Primary Cat: %s (Weak: %s)", primary_category, weak_product_signal)

    # ------------------------------------------------------------------ #
    # Build enriched record (lossless)
    # ------------------------------------------------------------------ #
    enriched = {
        # Core identity
        "name": name,
        "website": website,
        "location": company.get("location", ""),
        "address": company.get("address", ""),
        "phone": company.get("phone", ""),
        "description": company.get("description", ""),
        "vat": company.get("vat", ""),
        "place_id": company.get("place_id", ""),

        # Discovery provenance
        "discovery_hits": company.get("discovery_hits", []),
        "discovery_sources": company.get("discovery_sources", []),

        "products_detected_raw": products_detected_raw,
        "products_mapped": products_mapped,
        "category_counts": category_counts,
        "primary_category": primary_category,
        "secondary_categories": secondary_categories,
        "weak_product_signal": weak_product_signal,
        "unmapped_products_count": unmapped_products_count,
        
        "scrape_status": scrape_status,
        "scrape_source": scrape_source,
        "ai_input_text": ai_result.get("ai_input_text", ""),
        "rejected_reason": rejected_reason,

        "scraped_pages": scraped_pages,

        "pipeline_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return enriched


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    mode: str,
    auto_continue: bool = False,
    resume_mode: bool = False,
    limit: int = 0,
    skip_no_website: bool = False,
    retry_failures: bool = False,
) -> None:
    logger.info("=== ATECO 28.30 Pipeline (Mode: %s) ===", mode)

    if mode in ["discovery", "full"]:
        if not resume_mode:
            reset_state()
        companies = _discover_all()
        if not companies:
            logger.error("Discovery returned no companies")
            sys.exit(1)
        if mode == "discovery" and not auto_continue:
            logger.info("Discovery mode complete. Run with --mode enrichment to process discovery_output.json.")
            return

    if mode in ["enrichment", "full"]:
        state = load_state()
        start_index = state["index"] + 1

        try:
            with open("discovery_output.json", "r", encoding="utf-8") as f:
                companies = json.load(f)
        except Exception as e:
            logger.error("Failed to load discovery_output.json. Run --mode discovery first. %s", e)
            sys.exit(1)

        logger.info("=== ENRICHMENT PHASE: %d companies from index %d ===",
                    len(companies), start_index)

        # A company is strictly "processed" by the main pipeline only if it has a pipeline result.
        processed_names: set[str] = {
            r.get("name") or r.get("company_name", "") for r in _load_raw_master()
            if "products_detected_raw" in r or "rejected_reason" in r
        }

        passed = 0
        discarded = 0
        skipped_no_web = 0
        batch_count = 0
        
        stats = {
            "total_analyzed": 0,
            "scrape_success": 0,
            "manufacturers": 0,
            "agri_manufacturers": 0,
            "rejected": 0
        }

        # If --retry-failures is on, we force start_index to 0 so we scan everything for skips
        if retry_failures:
            start_index = 0

        pbar = tqdm(companies, desc="Enriching", ncols=100, file=sys.stdout, mininterval=2.0)
        with logging_redirect_tqdm():
            for idx, company in enumerate(pbar):
                if idx < start_index:
                    continue

                name = company.get("name", "").strip()
                if not name:
                    continue

                # In resume mode (or retry-failures), check if already processed
                if name in processed_names and (resume_mode or retry_failures):
                    logger.debug("[%d] Skipping (already pipelined): %s", idx, name)
                    continue

                # Skip companies with no website if requested
                if skip_no_website and not company.get("website", "").strip():
                    skipped_no_web += 1
                    logger.info("[%d] Skipping (no website): %s", idx, name)
                    continue

                # Honour batch limit
                if limit > 0 and batch_count >= limit:
                    logger.info("Reached batch limit of %d — stopping.", limit)
                    break
                batch_count += 1

                try:
                    record = _enrich_company(company, idx)
                except Exception as exc:
                    log_failure(
                        company_name=name,
                        stage="enrichment",
                        source="pipeline",
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    logger.error("[%d] Unhandled error for %s: %s", idx, name, exc, exc_info=True)
                    save_state(name, idx)
                    continue

                if record is not None:
                    stats["total_analyzed"] += 1
                    if record.get("scrape_status") != "failed":
                        stats["scrape_success"] += 1
                    if record.get("is_manufacturer"):
                        stats["manufacturers"] += 1
                    if record.get("is_agri_manufacturer"):
                        stats["agri_manufacturers"] += 1
                    if record.get("rejected_reason"):
                        stats["rejected"] += 1

                    _append_to_raw_master(record)
                    processed_names.add(name)
                    passed += 1
                    logger.info("  → SAVED [%d passed / %d discarded]", passed, discarded)
                else:
                    discarded += 1

                pbar.set_postfix({
                    "Agri": stats["agri_manufacturers"],
                    "Rej": stats["rejected"]
                })

                save_state(name, idx)

        if skipped_no_web:
            logger.info("Skipped %d companies with no website.", skipped_no_web)

    # ------------------------------------------------------------------ #
    # Generate outputs
    # ------------------------------------------------------------------ #
    if mode in ["enrichment", "full"]:
        t = stats["total_analyzed"]
        if t > 0:
            print("\n")
            print("="*50)
            print("PIPELINE EXECUTION SNAPSHOT")
            print("="*50)
            print(f"Total analyzed:                {t:<4}")
            print(f"Scrape success:               {stats['scrape_success']:>4}   ({stats['scrape_success']/t*100:.1f}%)")
            print(f"Manufacturers (all):          {stats['manufacturers']:>4}   ({stats['manufacturers']/t*100:.1f}%)")
            print(f"Agri manufacturers (TRUE):     {stats['agri_manufacturers']:>4}   ({stats['agri_manufacturers']/t*100:.1f}%)")
            print(f"Rejected:                      {stats['rejected']:>4}   ({stats['rejected']/t*100:.1f}%)")
            print("="*50)
            print("\n")

    logger.info("=== OUTPUT PHASE ===")
    n_rows = build_csv()

    logger.info(
        "Pipeline complete. Passed=%d Discarded=%d CSV rows=%d",
        passed, discarded, n_rows,
    )
    logger.info("Outputs:")
    logger.info("  %s", RAW_MASTER_PATH)
    logger.info("  %s", FINAL_CSV_PATH)
    logger.info("  %s", FAILURES_CSV_PATH)

    close_client()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="ATECO 28.30 Italian Agricultural Machinery Pipeline"
    )
    parser.add_argument("--mode", choices=["discovery", "enrichment", "full"], required=True)
    parser.add_argument("--auto-continue", action="store_true")
    parser.add_argument("--restart", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="Max companies to enrich (0 = all)")
    parser.add_argument("--skip-no-website", action="store_true", dest="skip_no_website",
                        help="Skip companies with no website")
    parser.add_argument("--skip-financials", action="store_true", 
                        help="Skip Apify extraction for financials, directors, ownership")
    parser.add_argument("--csv-only", action="store_true", dest="csv_only")
    parser.add_argument("--retry-failures", action="store_true", 
                        help="Smartly retry companies that crashed due to API exhaustion or parsing errors")
    args = parser.parse_args()

    if args.csv_only:
        logger.info("CSV-only mode: rebuilding final_dataset.csv")
        n = build_csv()
        logger.info("Done: %d rows written", n)
        return

    resume = args.resume and not args.restart
    run_pipeline(
        mode=args.mode,
        auto_continue=args.auto_continue,
        resume_mode=resume,
        limit=args.limit,
        skip_no_website=args.skip_no_website,
        retry_failures=args.retry_failures,
    )

if __name__ == "__main__":
    main()
