"""
salvage_apify_websites.py — Salvage websites from cancelled Apify runs.

Extracts organic results from all Apify runs today, matches the search query 
back to companies missing a website in discovery_output.json, and updates 
discovery_output.json / discovery_snapshot.csv.
"""
from __future__ import annotations

import csv
import json
import logging
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv
from tqdm import tqdm

from src.website_resolver import _is_skip_domain, _is_plausible_match, _clean_url

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("salvage")

DISCOVERY_JSON = Path("discovery_output.json")
SNAPSHOT_CSV   = Path("discovery_snapshot.csv")

def main() -> None:
    token = os.getenv("APIFY_API_TOKEN", "")
    if not token:
        logger.error("APIFY_API_TOKEN missing")
        sys.exit(1)
        
    discovery = json.loads(DISCOVERY_JSON.read_text(encoding="utf-8"))
    missing = [c for c in discovery if not c.get("website")]
    logger.info(f"Loaded {len(discovery)} companies. {len(missing)} missing websites.")
    if not missing:
        return
        
    # Build exact matching map: query_string -> company_dict reference
    # using the template: '"{name}" sito ufficiale'
    query_to_company = {}
    for c in missing:
        name = c.get("name", "").strip()
        if name:
            q = f'"{name}" sito ufficiale'
            query_to_company[q] = c
            
    actor = "apify~google-search-scraper"
    resp = httpx.get(
        f"https://api.apify.com/v2/acts/{actor}/runs",
        params={"token": token, "limit": 400, "desc": True},
        timeout=30,
    )
    runs = resp.json().get("data", {}).get("items", [])
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_runs = [r for r in runs if today in r.get("startedAt", "") and r.get("status") == "SUCCEEDED"]
    
    logger.info(f"Found {len(today_runs)} successful Apify runs from today.")
    
    salvaged = 0
    with httpx.Client(timeout=20) as client:
        with tqdm(total=len(today_runs), desc="Scanning Apify datasets", ncols=100, file=sys.stdout, mininterval=2.0) as pbar:
            for r in today_runs:
                ds_id = r.get("defaultDatasetId", "")
                try:
                    items_resp = client.get(
                        f"https://api.apify.com/v2/datasets/{ds_id}/items",
                        params={"token": token, "limit": 5},
                    )
                    items = items_resp.json()
                    for item in items:
                        query = item.get("searchQuery", {}).get("term", "")
                        comp = query_to_company.get(query)
                        if not comp or comp.get("website"):
                            continue
                            
                        # Try to resolve website from organic results
                        for res in item.get("organicResults", []):
                            url = res.get("url", "")
                            title = res.get("title", "")
                            if not url or not url.startswith("http") or _is_skip_domain(url):
                                continue
                            if _is_plausible_match(comp["name"], title, url):
                                comp["website"] = _clean_url(url)
                                logger.info(f"Salvaged: {comp['name']} -> {comp['website']}")
                                salvaged += 1
                                break
                except Exception as e:
                    pass
                pbar.update(1)
                    
    logger.info(f"\nSuccessfully salvaged {salvaged} websites from Apify cache!")
    
    # Save back
    DISCOVERY_JSON.write_text(json.dumps(discovery, ensure_ascii=False, indent=2), encoding="utf-8")
    
    # Write snapshot
    fields = ["company_name","website","location","description","europages_profile","source_list"]
    with SNAPSHOT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for c in discovery:
            sources = c.get("discovery_sources", [c.get("source","")])
            w.writerow({
                "company_name":      c.get("name", ""),
                "website":           c.get("website", ""),
                "location":          c.get("location", ""),
                "description":       c.get("description", ""),
                "europages_profile": c.get("europages_profile", ""),
                "source_list":       "|".join(dict.fromkeys(s for s in sources if s)),
            })
            
    logger.info("Updated discovery_output.json and discovery_snapshot.csv")

if __name__ == "__main__":
    main()
