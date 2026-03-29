"""
discovery/google_maps.py — Google Places API for manufacturer discovery.

Uses city-by-city expansion: for each target Italian city, runs ALL
GOOGLE_MAPS_QUERIES to maximise recall across regional clusters.

Progress tracking: tqdm bar shows [city N/total | query N/total | places found so far]
"""
from __future__ import annotations

import logging
import os
import sys
import time

from tqdm import tqdm
from typing import Optional

import httpx
from dotenv import load_dotenv

from src.config import (
    GOOGLE_MAPS_QUERIES,
    GOOGLE_PLACES_URL,
    GOOGLE_PLACES_REGION,
    GOOGLE_PLACES_MAX_PAGES,
    REQUEST_DELAY,
)
from src.failure_logger import log_failure

load_dotenv()
logger = logging.getLogger(__name__)

_SOURCE_NAME = "google_maps"

# Italian cities to sweep — covers all major agri-machinery production clusters
TARGET_CITIES = [
    # Lombardia
    "Milano", "Brescia", "Bergamo", "Cremona", "Mantova",
    # Veneto
    "Verona", "Padova", "Vicenza", "Treviso",
    # Emilia-Romagna
    "Bologna", "Modena", "Parma", "Reggio Emilia", "Ferrara", "Ravenna",
    # Piemonte
    "Torino", "Cuneo", "Alessandria",
    # Toscana
    "Firenze", "Siena", "Arezzo",
    # Puglia
    "Bari", "Lecce",
    # Sicilia
    "Palermo", "Catania",
    # Marche
    "Ancona", "Pesaro", "Macerata",
    # Friuli-Venezia Giulia
    "Udine", "Pordenone",
    # Trentino-Alto Adige
    "Trento", "Bolzano",
]


def _get_api_key() -> str:
    key = os.getenv("GOOGLE_MAPS_API_KEY", "")
    if not key:
        raise EnvironmentError(
            "GOOGLE_MAPS_API_KEY is not set. Please add it to your .env file."
        )
    return key


def discover() -> list[dict]:
    """
    Query Google Places Text Search across all target cities × all queries.

    Returns list of:
        {name, website, phone, address, location, place_id, source, source_url}
    """
    try:
        api_key = _get_api_key()
    except EnvironmentError as exc:
        log_failure(
            company_name="N/A",
            stage="discovery",
            source=_SOURCE_NAME,
            error_type="ConfigError",
            error_message=str(exc),
        )
        logger.error("Google Maps: %s", exc)
        return []

    all_companies: list[dict] = []
    seen_place_ids: set[str] = set()

    n_cities  = len(TARGET_CITIES)
    n_queries = len(GOOGLE_MAPS_QUERIES)
    total     = n_cities * n_queries

    logger.info(
        "Google Maps: sweeping %d cities × %d queries = %d searches",
        n_cities, n_queries, total,
    )

    completed = 0
    with httpx.Client(timeout=30) as client:
        with tqdm(
            total=total,
            desc="Google Maps",
            unit="search",
            ncols=100,
            file=sys.stdout,
            mininterval=2.0,
            bar_format=(
                "{l_bar}{bar}| {n_fmt}/{total_fmt} searches "
                "[{elapsed}<{remaining}, {rate_fmt}] | found: {postfix}"
            ),
        ) as pbar:
            for city_idx, city in enumerate(TARGET_CITIES, 1):
                city_found = 0
                for q_idx, query in enumerate(GOOGLE_MAPS_QUERIES, 1):
                    city_query = f"{query} {city} Italia"
                    results = _run_query(client, api_key, city_query, seen_place_ids, city)
                    all_companies.extend(results)
                    city_found += len(results)
                    completed  += 1

                    pbar.set_postfix_str(
                        f"{len(all_companies)} places | city {city_idx}/{n_cities}: {city}"
                    )
                    pbar.update(1)

                    if results:
                        logger.info(
                            "  [%d/%d] %s | query %d/%d → %d new (total %d)",
                            city_idx, n_cities, city,
                            q_idx, n_queries,
                            len(results), len(all_companies),
                        )
                    time.sleep(REQUEST_DELAY)

                remaining = total - completed
                logger.info(
                    "  City done: %s (+%d places) | %d/%d searches complete | %d remaining",
                    city, city_found, completed, total, remaining,
                )

    logger.info("Google Maps: %d unique places found across all cities", len(all_companies))
    return all_companies


def _run_query(
    client: httpx.Client,
    api_key: str,
    query: str,
    seen_place_ids: set[str],
    city_hint: str = "",
) -> list[dict]:
    """Run one Places Text Search query, paginating up to MAX_PAGES."""
    results: list[dict] = []
    next_page_token: Optional[str] = None

    for page_num in range(GOOGLE_PLACES_MAX_PAGES):
        params: dict = {
            "query": query,
            "region": GOOGLE_PLACES_REGION,
            "key": api_key,
            "type": "establishment",
        }
        if next_page_token:
            time.sleep(2.5)  # Google requires delay before using next_page_token
            params = {"pagetoken": next_page_token, "key": api_key}

        try:
            resp = client.get(GOOGLE_PLACES_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log_failure(
                company_name="N/A",
                stage="discovery",
                source=_SOURCE_NAME,
                error_type=type(exc).__name__,
                error_message=f"query={query!r} page={page_num}: {exc}",
            )
            break

        status = data.get("status", "")
        if status not in ("OK", "ZERO_RESULTS"):
            log_failure(
                company_name="N/A",
                stage="discovery",
                source=_SOURCE_NAME,
                error_type="APIError",
                error_message=f"Places API status={status} for query={query!r}",
            )
            break

        for place in data.get("results", []):
            place_id = place.get("place_id", "")
            if place_id in seen_place_ids:
                continue
            seen_place_ids.add(place_id)

            name = place.get("name", "")
            address = place.get("formatted_address", "")
            location = _parse_city(address) or city_hint

            website, phone = _get_place_details(client, api_key, place_id)

            results.append({
                "name": name,
                "website": website,
                "phone": phone,
                "address": address,
                "location": location,
                "place_id": place_id,
                "source": _SOURCE_NAME,
                "source_url": f"https://maps.google.com/?q=place_id:{place_id}",
            })

        next_page_token = data.get("next_page_token")
        if not next_page_token:
            break

    return results


def _get_place_details(
    client: httpx.Client,
    api_key: str,
    place_id: str,
) -> tuple[str, str]:
    """Fetch website and phone number from Place Details API."""
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "website,formatted_phone_number",
        "key": api_key,
    }
    try:
        time.sleep(0.3)
        resp = client.get(url, params=params)
        resp.raise_for_status()
        result = resp.json().get("result", {})
        return result.get("website", ""), result.get("formatted_phone_number", "")
    except Exception as exc:
        logger.debug("Place details failed for %s: %s", place_id, exc)
        return "", ""


def _parse_city(address: str) -> str:
    """Extract city name from a Google formatted address string."""
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 2:
        city_part = parts[-2]
        city = " ".join(
            w for w in city_part.split() if not w.isdigit() or len(w) != 5
        ).strip()
        return city
    return address
