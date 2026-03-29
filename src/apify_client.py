"""
apify_client.py — Thin wrapper around the Apify REST API.

Uses the `apify~google-search-scraper` actor for ALL search operations.
Returns structured results including organic results and AI Overview text.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Optional

import httpx
from dotenv import load_dotenv
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from src.config import (
    APIFY_BASE_URL,
    APIFY_SEARCH_ACTOR,
    APIFY_MAX_RESULTS,
    MAX_RETRIES,
    BACKOFF_BASE,
)

load_dotenv()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Apify API token
# ---------------------------------------------------------------------------

def _get_token() -> str:
    token = os.getenv("APIFY_API_TOKEN", "")
    if not token:
        raise EnvironmentError(
            "APIFY_API_TOKEN is not set. Please add it to your .env file."
        )
    return token


# ---------------------------------------------------------------------------
# Core search function
# ---------------------------------------------------------------------------

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=BACKOFF_BASE, min=BACKOFF_BASE, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def run_apify_search(
    query: str,
    max_results: int = APIFY_MAX_RESULTS,
    country_code: str = "it",
    language_code: str = "it",
) -> list[dict[str, Any]]:
    """
    Run a Google Search via Apify's google-search-scraper actor.

    Returns a list of result dicts, each with:
        - title (str)
        - url   (str)
        - description (str)
        - position (int)
        - ai_overview (str | None)  — Google AI Overview text if present

    Raises on all errors (tenacity will retry).
    """
    token = _get_token()

    # Actor input schema for apify~google-search-scraper
    actor_input = {
        "queries": query,
        "maxPagesPerQuery": 1,
        "resultsPerPage": max_results,
        "countryCode": country_code,
        "languageCode": language_code,
        "includeAIOverview": True,
    }

    url = (
        f"{APIFY_BASE_URL}/acts/{APIFY_SEARCH_ACTOR}"
        f"/run-sync-get-dataset-items?token={token}&timeout=120"
    )

    with httpx.Client(timeout=150) as client:
        response = client.post(url, json=actor_input)

    if response.status_code not in (200, 201):
        raise httpx.HTTPStatusError(
            f"Apify returned {response.status_code}: {response.text[:200]}",
            request=response.request,
            response=response,
        )

    raw_items: list[dict] = response.json()

    results: list[dict] = []
    for item in raw_items:
        # Each item corresponds to one search result page (one query)
        organic = item.get("organicResults", [])
        ai_overview = item.get("aiOverview", {})
        ai_text = ""
        if isinstance(ai_overview, dict):
            ai_text = ai_overview.get("text", "")
        elif isinstance(ai_overview, str):
            ai_text = ai_overview

        for r in organic:
            results.append({
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "description": r.get("description", ""),
                "position": r.get("position", 0),
                "ai_overview": ai_text,
                "ai_overview_sources": _extract_ai_sources(ai_overview),
            })

    return results


def _extract_ai_sources(ai_overview: Any) -> list[str]:
    """Pull cited URLs out of the AI Overview block."""
    if not isinstance(ai_overview, dict):
        return []
    sources = []
    for ref in ai_overview.get("references", []):
        if isinstance(ref, dict):
            u = ref.get("url") or ref.get("link", "")
            if u:
                sources.append(u)
    return sources


# ---------------------------------------------------------------------------
# Convenience: search + return top URL
# ---------------------------------------------------------------------------

def search_top_url(query: str) -> Optional[str]:
    """Return the URL of the first organic result, or None on failure."""
    try:
        results = run_apify_search(query, max_results=5)
        for r in results:
            url = r.get("url", "")
            if url and url.startswith("http"):
                return url
    except Exception as exc:
        logger.warning("search_top_url failed for query=%r: %s", query, exc)
    return None


def search_organic_urls(query: str, n: int = 5) -> list[str]:
    """Return up to n organic result URLs for a query."""
    try:
        results = run_apify_search(query, max_results=n)
        return [r["url"] for r in results if r.get("url", "").startswith("http")]
    except Exception as exc:
        logger.warning("search_organic_urls failed for query=%r: %s", query, exc)
        return []

# ---------------------------------------------------------------------------
# Fallback: Website Content Crawler
# ---------------------------------------------------------------------------

@retry(
    stop=stop_after_attempt(1),
    wait=wait_exponential(multiplier=BACKOFF_BASE, min=BACKOFF_BASE, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def run_apify_website_crawler(website_url: str) -> dict[str, str]:
    """
    Fallback crawler using apify~website-content-crawler.
    Constrained to explicitly permitted paths and maximum 3 pages.
    """
    token = _get_token()
    base = website_url.rstrip("/")
    
    paths = ["/", "/about", "/azienda", "/chi-siamo", "/products", "/prodotti"]
    start_urls = [{"url": base + p} for p in paths]

    actor_input = {
        "startUrls": start_urls,
        "maxCrawlPages": 3,
        "maxCrawlDepth": 1,
        "crawlerType": "cheerio",  # Fast static text extraction
        "proxyConfiguration": {"useApifyProxy": True},
    }

    url = (
        f"{APIFY_BASE_URL}/acts/apify~website-content-crawler"
        f"/run-sync-get-dataset-items?token={token}&timeout=60"
    )

    with httpx.Client(timeout=65) as client:
        response = client.post(url, json=actor_input)

    if response.status_code not in (200, 201):
        raise httpx.HTTPStatusError(
            f"Apify Fallback Crawler returned {response.status_code}",
            request=response.request,
            response=response,
        )

    raw_items: list[dict] = response.json()
    scraped: dict[str, str] = {}
    
    for item in raw_items:
        u = item.get("url")
        text = item.get("text")
        if u and text:
            scraped[u] = text

    return scraped
