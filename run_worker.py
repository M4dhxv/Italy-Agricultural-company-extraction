"""
run_worker.py — Runs the enrichment pipeline on a single shard file.

Each worker operates completely independently with its own:
- Input: shards/discovery_shard_N.json
- Output: shards/raw_shard_N.json
- State: shards/state_shard_N.json
- Log: shards/worker_N.log

Usage (called automatically by run_parallel.py):
    python run_worker.py --shard-id 0
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from src.scraper import scrape_company
from src.intelligence_layer import analyze_company
from src.product_extractor import extract_products
from src.failure_logger import log_failure
from src.http_client import close_client
from src.website_resolver import resolve_website


def setup_logging(shard_id: int) -> logging.Logger:
    log_file = f"shards/worker_{shard_id}.log"
    logging.basicConfig(
        level=logging.INFO,
        format=f"[W{shard_id}] %(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    return logging.getLogger(f"worker_{shard_id}")


def load_shard_state(shard_id: int) -> dict:
    path = f"shards/state_shard_{shard_id}.json"
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"index": -1, "last": ""}


def save_shard_state(shard_id: int, name: str, idx: int):
    path = f"shards/state_shard_{shard_id}.json"
    with open(path, "w") as f:
        json.dump({"index": idx, "last": name}, f)


def append_to_shard(shard_id: int, record: dict):
    path = f"shards/raw_shard_{shard_id}.json"
    records = []
    if os.path.exists(path):
        with open(path) as f:
            try:
                records = json.load(f)
            except Exception:
                records = []
    records.append(record)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def enrich_company(company: dict, logger: logging.Logger) -> dict | None:
    name = company.get("name", "").strip()
    website = company.get("website", "").strip()

    if not website:
        logger.info("  → Resolving website for %r", name)
        website = resolve_website(name) or ""

    if not website:
        logger.warning("  → No website found: %s", name)
        return {
            **company,
            "rejected_reason": "no_website",
            "scrape_status": "unattempted",
            "scrape_source": "none",
            "products_detected_raw": [],
            "products_mapped": [],
            "primary_category": "",
            "pipeline_timestamp": datetime.now(timezone.utc).isoformat(),
        }

    logger.info("  → Scraping: %s", website)
    try:
        scraped_pages, scrape_source = scrape_company(website, company_name=name)
    except Exception as e:
        logger.error("  → Scrape exception: %s", e)
        scraped_pages, scrape_source = {}, "none"

    scrape_status = "success" if scrape_source != "none" else "failed"

    logger.info("  → Gemini analysis")
    ai = analyze_company(name, website, scraped_pages)

    is_manufacturer = ai.get("is_manufacturer", False)
    is_agri = ai.get("is_agri_manufacturer", False)
    products_raw = ai.get("products_detected_raw", [])

    rejected_reason = None
    if not is_manufacturer:
        rejected_reason = "not_manufacturer"
    elif not is_agri:
        rejected_reason = "not_agri_manufacturer"
    elif scrape_status == "failed":
        rejected_reason = "scrape_failed"

    products_mapped, category_counts, primary, secondary, weak = extract_products(products_raw)

    if rejected_reason:
        logger.info("  → REJECTED (%s): %s", rejected_reason, name)
    else:
        logger.info("  → PASSED: %s | %s | %s", name, primary, products_raw[:2])

    return {
        **company,
        "is_manufacturer": is_manufacturer,
        "is_agri_manufacturer": is_agri,
        "scrape_status": scrape_status,
        "scrape_source": scrape_source,
        "rejected_reason": rejected_reason,
        "products_detected_raw": products_raw,
        "products_mapped": products_mapped,
        "category_counts": category_counts,
        "primary_category": primary,
        "secondary_categories": secondary,
        "weak_product_signal": weak,
        "confidence": ai.get("confidence", 0.0),
        "evidence": ai.get("evidence", ""),
        "ai_used": ai.get("ai_used", False),
        "pipeline_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-id", type=int, required=True)
    args = parser.parse_args()

    shard_id = args.shard_id
    os.makedirs("shards", exist_ok=True)

    logger = setup_logging(shard_id)

    shard_file = f"shards/discovery_shard_{shard_id}.json"
    if not os.path.exists(shard_file):
        logger.error("Shard file not found: %s", shard_file)
        sys.exit(1)

    with open(shard_file) as f:
        companies = json.load(f)

    state = load_shard_state(shard_id)
    start_index = state["index"] + 1
    logger.info("Worker %d: %d companies, resuming from index %d", shard_id, len(companies), start_index)

    passed = 0
    pbar = tqdm(
        companies,
        desc=f"W{shard_id}",
        ncols=80,
        file=sys.stdout,
        mininterval=2.0,
        position=shard_id,
        leave=True,
    )

    with logging_redirect_tqdm():
        for idx, company in enumerate(pbar):
            if idx < start_index:
                continue

            name = company.get("name", "").strip()
            if not name:
                continue

            try:
                record = enrich_company(company, logger)
            except Exception as exc:
                logger.error("Unhandled error for %s: %s", name, exc)
                save_shard_state(shard_id, name, idx)
                continue

            if record is not None:
                append_to_shard(shard_id, record)
                passed += 1
                pbar.set_postfix({"saved": passed})

            save_shard_state(shard_id, name, idx)

    logger.info("Worker %d done. Saved %d records.", shard_id, passed)
    close_client()


if __name__ == "__main__":
    main()
