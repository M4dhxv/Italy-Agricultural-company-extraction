"""
discovery/google_search.py — Apify-powered Google Search discovery.

Runs multiple ATECO/manufacturer queries and collects company name + URL candidates.
"""
from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

from src.config import GOOGLE_SEARCH_DISCOVERY_QUERIES
from src.apify_client import run_apify_search
from src.failure_logger import log_failure

logger = logging.getLogger(__name__)

_SOURCE_NAME = "google_search"

# Domains to skip — they're directories, not company websites
_SKIP_DOMAINS = {
    "google.com", "google.it", "wikipedia", "facebook.com",
    "linkedin.com", "youtube.com", "instagram.com",
    "aziende.it", "registroaziende.it", "reportaziende.it",
    "companyreports.it", "europages", "fatturatoitalia.it",
    "ufficiocamerale.it",
}


def discover() -> list[dict]:
    """
    Run all discovery search queries via Apify and return candidate companies.

    Returns list of:
        {name, website, description, query, source, source_url}
    """
    all_companies: list[dict] = []
    seen_urls: set[str] = set()

    for query in GOOGLE_SEARCH_DISCOVERY_QUERIES:
        try:
            results = run_apify_search(query, max_results=10)
        except Exception as exc:
            log_failure(
                company_name="N/A",
                stage="discovery",
                source=_SOURCE_NAME,
                error_type=type(exc).__name__,
                error_message=f"query={query!r}: {exc}",
            )
            logger.warning("Google Search discovery failed for query=%r: %s", query, exc)
            continue

        for r in results:
            url = r.get("url", "")
            title = r.get("title", "")
            desc = r.get("description", "")

            if not url or not title:
                continue
            if _is_skip_domain(url):
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)

            name = _extract_company_name(title)
            if not name:
                continue

            all_companies.append({
                "name": name,
                "website": url,
                "description": desc[:400],
                "query": query,
                "source": _SOURCE_NAME,
                "source_url": url,
            })

    logger.info("Google Search discovery: %d candidate companies", len(all_companies))
    return all_companies


def _is_skip_domain(url: str) -> bool:
    try:
        domain = urlparse(url).netloc.lower()
        return any(skip in domain for skip in _SKIP_DOMAINS)
    except Exception:
        return True


def _extract_company_name(title: str) -> str:
    """
    Heuristically extract company name from a search result title.

    Typical titles:
      - "FELLA-Werke GmbH - Agricultural Machinery"
      - "Maschhio Gaspardo | Seeders, Tillers, Disc harrows"
      - "About us – SIPMA SA"
    """
    # Remove common suffixes after separator characters
    for sep in [" - ", " | ", " – ", " — ", " :: ", " / "]:
        if sep in title:
            parts = title.split(sep)
            # Take the shortest non-empty part as likely company name
            candidates = [p.strip() for p in parts if p.strip()]
            if candidates:
                title = min(candidates, key=len)
            break

    # Remove generic trailing words
    stopwords = [
        "srl", "spa", "snc", "sas", "ltd", "gmbh", "s.r.l.", "s.p.a.",
        "homepage", "home", "official", "website", "sito ufficiale",
    ]
    cleaned = title.strip()
    for sw in stopwords:
        if cleaned.lower().endswith(sw):
            cleaned = cleaned[: -len(sw)].strip(" -,|")

    return cleaned.strip() if len(cleaned) > 2 else ""
