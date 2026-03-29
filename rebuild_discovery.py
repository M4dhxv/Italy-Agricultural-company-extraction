"""
rebuild_discovery.py — Rebuild discovery_output.json from cached sources.

Sources:
  1. Europages CSV  (local file, instant)
  2. Apify datasets (last 7 discovery query runs, already stored)
  3. Google Maps    (re-run city sweep — ~11 min)

Run:
    python rebuild_discovery.py
    python rebuild_discovery.py --skip-maps   # if Maps already done separately
"""
from __future__ import annotations

import argparse
import csv as csv_mod
import json
import logging
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("pipeline.log", encoding="utf-8")],
)
logger = logging.getLogger("rebuild_discovery")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
EUROPAGES_CSV        = Path.home() / "Downloads" / "europages.csv"
NACE_CSV             = Path.home() / "Downloads" / "italian_nace_2830_manufacturers.csv"
DISCOVERY_JSON       = Path("discovery_output.json")
SNAPSHOT_CSV         = Path("discovery_snapshot.csv")

APIFY_ACTOR          = "apify~google-search-scraper"
APIFY_BASE           = "https://api.apify.com/v2"

# Discovery query terms — must match what was run in google_search.py
DISCOVERY_QUERIES = [
    "ATECO 28.30 aziende Italia",
    "produttore macchine agricole Italia",
    "costruttore macchine agricole Italia",
    "agricultural machinery manufacturer Italy",
    "site:federunacoma.it associati",
    "site:eima.it espositori",
    "ATECO 28.30 Srl Italia",
]

GOOGLE_PLACES_URL    = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_DETAILS_URL   = "https://maps.googleapis.com/maps/api/place/details/json"
GOOGLE_PLACES_REGION = "it"
GOOGLE_PLACES_MAX_PAGES = 3
REQUEST_DELAY        = 1.5

TARGET_CITIES = [
    "Milano", "Brescia", "Bergamo", "Cremona", "Mantova",
    "Verona", "Padova", "Vicenza", "Treviso",
    "Bologna", "Modena", "Parma", "Reggio Emilia", "Ferrara", "Ravenna",
    "Torino", "Cuneo", "Alessandria",
    "Firenze", "Siena", "Arezzo",
    "Bari", "Lecce",
    "Palermo", "Catania",
    "Ancona", "Pesaro", "Macerata",
    "Udine", "Pordenone",
    "Trento", "Bolzano",
]
MAPS_QUERIES = [
    "produttore macchine agricole",
    "costruttore macchine agricole",
    "fabbricazione macchine agricole",
]

SKIP_DOMAINS = {
    "google.com", "google.it", "wikipedia", "facebook.com",
    "linkedin.com", "youtube.com", "instagram.com",
    "aziende.it", "registroaziende.it", "reportaziende.it",
    "companyreports.it", "europages", "fatturatoitalia.it",
    "ufficiocamerale.it",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_apify_token() -> str:
    t = os.getenv("APIFY_API_TOKEN", "")
    if not t:
        logger.error("APIFY_API_TOKEN not set"); sys.exit(1)
    return t


def _get_maps_key() -> str:
    k = os.getenv("GOOGLE_MAPS_API_KEY", "")
    if not k:
        logger.error("GOOGLE_MAPS_API_KEY not set"); sys.exit(1)
    return k


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "").strip()
    except Exception:
        return ""


def _is_skip(url: str) -> bool:
    d = _domain(url)
    return any(s in d for s in SKIP_DOMAINS)


# ---------------------------------------------------------------------------
# 1. Europages CSV
# ---------------------------------------------------------------------------

def load_europages() -> list[dict]:
    if not EUROPAGES_CSV.exists():
        logger.warning("Europages CSV not found at %s", EUROPAGES_CSV)
        return []
    companies = []
    skipped = 0
    with EUROPAGES_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv_mod.DictReader(f)
        for row in reader:
            name = (row.get("line-clamp-2") or "").strip()
            if not name:
                skipped += 1
                continue
            location = (row.get("truncate 2") or row.get("city") or "").strip().lstrip(", ")
            profile  = (row.get("flex href") or "").strip()
            companies.append({
                "name": name,
                "website": "",
                "location": location,
                "description": (row.get("font-copy-400") or "").strip(),
                "europages_profile": profile,
                "company_type_raw": (row.get("flex 4") or "").strip(),
                "source": "europages",
                "source_url": profile,
            })
    logger.info("Europages: loaded %d companies (%d skipped)", len(companies), skipped)
    return companies


# ---------------------------------------------------------------------------
# 2. Apify — pull last N runs for the discovery queries
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 1b. NACE 28.30 manufacturers CSV (FederUnacoma + EIMA sourced)
# ---------------------------------------------------------------------------

def load_nace_csv() -> list[dict]:
    if not NACE_CSV.exists():
        logger.warning("NACE CSV not found at %s", NACE_CSV)
        return []
    companies = []
    skipped = 0
    with NACE_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv_mod.DictReader(f)
        for row in reader:
            name = (row.get("Company Name") or "").strip()
            if not name:
                skipped += 1
                continue
            raw_web = (row.get("Website") or "").strip()
            # Normalise: add https:// if missing
            if raw_web and not raw_web.startswith("http"):
                raw_web = "https://" + raw_web
            location = (row.get("Location") or "").strip()
            companies.append({
                "name":       name,
                "website":    raw_web,
                "location":   location,
                "source":     "nace_csv",
                "source_url": raw_web,
            })
    logger.info("NACE CSV: loaded %d companies (%d skipped)", len(companies), skipped)
    return companies


def load_apify_search(n_runs: int = 30) -> list[dict]:
    token = _get_apify_token()
    resp = httpx.get(
        f"{APIFY_BASE}/acts/{APIFY_ACTOR}/runs",
        params={"token": token, "limit": n_runs, "desc": True},
        timeout=20,
    )
    runs = resp.json().get("data", {}).get("items", [])
    logger.info("Apify: found %d recent runs", len(runs))

    companies: list[dict] = []
    seen_urls: set[str] = set()
    matched_runs = 0

    for run in runs:
        if run.get("status") != "SUCCEEDED":
            continue
        ds_id = run.get("defaultDatasetId", "")
        items_resp = httpx.get(
            f"{APIFY_BASE}/datasets/{ds_id}/items",
            params={"token": token, "limit": 200},
            timeout=20,
        )
        items = items_resp.json()

        for item in items:
            q_term = item.get("searchQuery", {}).get("term", "")
            # Only process discovery queries (not website-resolution queries)
            if not any(dq.lower() in q_term.lower() or q_term.lower() in dq.lower()
                       for dq in DISCOVERY_QUERIES):
                continue
            matched_runs += 1
            for r in item.get("organicResults", []):
                url   = r.get("url", "")
                title = r.get("title", "")
                desc  = r.get("description", "")
                if not url or not title or _is_skip(url) or url in seen_urls:
                    continue
                seen_urls.add(url)
                name = _extract_company_name(title)
                if not name:
                    continue
                companies.append({
                    "name":        name,
                    "website":     url,
                    "description": desc[:400],
                    "source":      "google_search",
                    "source_url":  url,
                    "query":       q_term,
                })

    logger.info(
        "Apify Search: %d discovery runs matched → %d candidate companies",
        matched_runs, len(companies)
    )
    return companies


def _extract_company_name(title: str) -> str:
    for sep in [" - ", " | ", " – ", " — ", " :: ", " / "]:
        if sep in title:
            parts = [p.strip() for p in title.split(sep) if p.strip()]
            if parts:
                title = min(parts, key=len)
            break
    stopwords = ["srl","spa","snc","sas","ltd","gmbh","s.r.l.","s.p.a.",
                 "homepage","home","official","website","sito ufficiale"]
    cleaned = title.strip()
    for sw in stopwords:
        if cleaned.lower().endswith(sw):
            cleaned = cleaned[:-len(sw)].strip(" -,|")
    return cleaned.strip() if len(cleaned) > 2 else ""


# ---------------------------------------------------------------------------
# 3. Google Maps — city × query sweep
# ---------------------------------------------------------------------------

def load_google_maps(api_key: str) -> list[dict]:
    all_companies: list[dict] = []
    seen_place_ids: set[str] = set()
    n_cities  = len(TARGET_CITIES)
    n_queries = len(MAPS_QUERIES)
    total     = n_cities * n_queries

    logger.info("Google Maps: %d cities × %d queries = %d searches", n_cities, n_queries, total)

    completed = 0
    with httpx.Client(timeout=30) as client:
        with tqdm(total=total, desc="Google Maps", unit="search", ncols=100, file=sys.stdout, mininterval=2.0) as pbar:
            for city_idx, city in enumerate(TARGET_CITIES, 1):
                city_found = 0
                for q_idx, query in enumerate(MAPS_QUERIES, 1):
                    results = _run_maps_query(client, api_key,
                                              f"{query} {city} Italia",
                                              seen_place_ids, city)
                    all_companies.extend(results)
                    city_found += len(results)
                    completed  += 1
                    pbar.set_postfix_str(
                        f"{len(all_companies)} places | {city} ({city_idx}/{n_cities})"
                    )
                    pbar.update(1)
                    time.sleep(REQUEST_DELAY)

                remaining = total - completed
                logger.info(
                    "  City done: %s (+%d) | %d/%d done | %d left",
                    city, city_found, completed, total, remaining,
                )

    logger.info("Google Maps: %d unique places", len(all_companies))
    return all_companies


def _run_maps_query(client, api_key, query, seen_ids, city_hint=""):
    results = []
    next_token = None
    for _ in range(GOOGLE_PLACES_MAX_PAGES):
        params = {"query": query, "region": GOOGLE_PLACES_REGION,
                  "key": api_key, "type": "establishment"}
        if next_token:
            time.sleep(2.5)
            params = {"pagetoken": next_token, "key": api_key}
        try:
            resp = client.get(GOOGLE_PLACES_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Maps query failed: %s", exc)
            break

        status = data.get("status", "")
        if status not in ("OK", "ZERO_RESULTS"):
            break

        for place in data.get("results", []):
            pid = place.get("place_id", "")
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            address  = place.get("formatted_address", "")
            location = _parse_city(address) or city_hint
            website, phone = _get_place_details(client, api_key, pid)
            results.append({
                "name":       place.get("name", ""),
                "website":    website,
                "phone":      phone,
                "address":    address,
                "location":   location,
                "place_id":   pid,
                "source":     "google_maps",
                "source_url": f"https://maps.google.com/?q=place_id:{pid}",
            })

        next_token = data.get("next_page_token")
        if not next_token:
            break
    return results


def _get_place_details(client, api_key, place_id):
    try:
        time.sleep(0.3)
        resp = client.get(GOOGLE_DETAILS_URL,
                          params={"place_id": place_id,
                                  "fields": "website,formatted_phone_number",
                                  "key": api_key})
        resp.raise_for_status()
        r = resp.json().get("result", {})
        return r.get("website", ""), r.get("formatted_phone_number", "")
    except Exception:
        return "", ""


def _parse_city(address: str) -> str:
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 2:
        city_part = parts[-2]
        return " ".join(w for w in city_part.split()
                        if not w.isdigit() or len(w) != 5).strip()
    return address


# ---------------------------------------------------------------------------
# 4. Union-merge
# ---------------------------------------------------------------------------

def union_merge(all_raw: list[dict]) -> list[dict]:
    unique: list[dict] = []
    domain_index: dict[str, int] = {}
    name_index:   dict[str, int] = {}

    for c in all_raw:
        nname   = c.get("name", "").lower().strip()
        raw_web = c.get("website", "")
        dom     = _domain(raw_web) if raw_web else ""

        idx = -1
        if dom and dom in domain_index:
            idx = domain_index[dom]
        elif nname and nname in name_index:
            idx = name_index[nname]

        hit = {"source": c.get("source",""), "source_url": c.get("source_url","")}

        if idx >= 0:
            existing = unique[idx]
            for key, val in c.items():
                if key in ("discovery_hits", "discovery_sources"):
                    continue
                if val and not existing.get(key):
                    existing[key] = val
            existing.setdefault("discovery_hits", []).append(hit)
            if c.get("source") not in existing.get("discovery_sources", []):
                existing.setdefault("discovery_sources", []).append(c.get("source",""))
            if dom and dom not in domain_index:
                domain_index[dom] = idx
        else:
            new_idx = len(unique)
            c["discovery_hits"]    = [hit]
            c["discovery_sources"] = [c.get("source","")]
            unique.append(c)
            if dom:    domain_index[dom] = new_idx
            if nname:  name_index[nname] = new_idx

    return unique


# ---------------------------------------------------------------------------
# 5. Save outputs
# ---------------------------------------------------------------------------

def save_snapshot(companies: list[dict]) -> None:
    fields = ["company_name","website","location","description",
              "europages_profile","source_list"]
    with SNAPSHOT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv_mod.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for c in companies:
            sources = c.get("discovery_sources", [c.get("source","")])
            w.writerow({
                "company_name":      c.get("name",""),
                "website":           c.get("website",""),
                "location":          c.get("location",""),
                "description":       c.get("description",""),
                "europages_profile": c.get("europages_profile",""),
                "source_list":       "|".join(dict.fromkeys(s for s in sources if s)),
            })
    logger.info("Saved %s (%d rows)", SNAPSHOT_CSV, len(companies))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild discovery_output.json from cached sources")
    parser.add_argument("--skip-maps", action="store_true", help="Skip Google Maps (use if already done)")
    args = parser.parse_args()

    logger.info("=== REBUILD DISCOVERY ===")

    # 1. Europages
    ep   = load_europages()

    # 1b. NACE CSV
    nace = load_nace_csv()

    # 2. Apify Search (cached)
    srch = load_apify_search(n_runs=50)

    # 3. Google Maps
    if args.skip_maps:
        maps = []
        logger.info("Google Maps: skipped (--skip-maps)")
    else:
        maps_key = _get_maps_key()
        maps = load_google_maps(maps_key)

    # 4. Merge
    all_raw = ep + nace + maps + srch
    logger.info("Total raw: EP=%d  NACE=%d  Maps=%d  Search=%d", len(ep), len(nace), len(maps), len(srch))
    unique = union_merge(all_raw)
    logger.info("After dedup: %d unique companies", len(unique))

    # 5. Stats
    with_web    = sum(1 for c in unique if c.get("website"))
    without_web = len(unique) - with_web

    logger.info("=== DISCOVERY SUMMARY ===")
    logger.info("  Europages:             %d", len(ep))
    logger.info("  NACE CSV:              %d", len(nace))
    logger.info("  Google Maps:           %d", len(maps))
    logger.info("  Google Search:         %d", len(srch))
    logger.info("  Final unique:          %d", len(unique))
    logger.info("  With website:          %d", with_web)
    logger.info("  Without website:       %d", without_web)

    # 6. Save
    import os as _os
    tmp = DISCOVERY_JSON.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)
    _os.replace(tmp, DISCOVERY_JSON)
    logger.info("Saved %s", DISCOVERY_JSON)

    save_snapshot(unique)

    print(f"\n{'='*50}")
    print(f"  Europages loaded:      {len(ep)}")
    print(f"  NACE CSV:              {len(nace)}")
    print(f"  Google Maps:           {len(maps)}")
    print(f"  Google Search:         {len(srch)}")
    print(f"  Final unique:          {len(unique)}")
    print(f"  With website:          {with_web}")
    print(f"  Without website:       {without_web}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
