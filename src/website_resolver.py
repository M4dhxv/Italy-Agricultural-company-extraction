"""
website_resolver.py — Resolves official website for companies discovered without one.

Uses Apify google-search-scraper to find the company's official website.
Companies with no resolvable website are discarded (as per global rules).
"""
from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

from src.config import WEBSITE_RESOLUTION_TEMPLATES
from src.apify_client import run_apify_search
from src.failure_logger import log_failure

logger = logging.getLogger(__name__)

_SKIP_DOMAINS = {
    "facebook.com", "linkedin.com", "instagram.com", "twitter.com",
    "youtube.com", "wikipedia.org", "google.com", "google.it",
    "bing.com", "paginegialle.it", "tuttitalia.it", "aziende.it",
    "registroaziende.it", "reportaziende.it", "companyreports.it",
    "europages.com", "europages.co.uk", "europages.it",
    "fatturatoitalia.it", "ufficiocamerale.it",
    "manta.com", "dnb.com",
}


def resolve_website(company_name: str) -> str | None:
    """
    Try to find the official website for a company name.

    Returns the website URL or None if not found.
    """
    for template in WEBSITE_RESOLUTION_TEMPLATES:
        query = template.format(name=company_name)
        try:
            results = run_apify_search(query, max_results=5)
        except Exception as exc:
            log_failure(
                company_name=company_name,
                stage="website_resolution",
                source="apify_search",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            logger.warning("Website resolution search failed for %r: %s", company_name, exc)
            continue

        for r in results:
            url = r.get("url", "")
            title = r.get("title", "")
            if not url or not url.startswith("http"):
                continue
            if _is_skip_domain(url):
                continue
            # Confirm the result is plausibly about this company
            if _is_plausible_match(company_name, title, url):
                logger.debug("Resolved %r → %s", company_name, url)
                return _clean_url(url)

    logger.info("No website found for %r — company will be discarded", company_name)
    return None


def _is_skip_domain(url: str) -> bool:
    try:
        domain = urlparse(url).netloc.lower()
        return any(skip in domain for skip in _SKIP_DOMAINS)
    except Exception:
        return True


def _is_plausible_match(company_name: str, title: str, url: str) -> bool:
    """
    Heuristic: does this search result plausibly correspond to the company?
    We check if any significant word from the company name appears in the
    title or the domain.
    """
    name_words = set(
        w.lower() for w in re.split(r"\W+", company_name) if len(w) > 3
    )
    title_lower = title.lower()
    domain = urlparse(url).netloc.lower()
    combined = title_lower + " " + domain

    matches = sum(1 for w in name_words if w in combined)
    # Require at least 1 substantial word match
    return matches >= 1 or len(name_words) == 0


def _clean_url(url: str) -> str:
    """Normalise URL: strip query strings and fragments, keep scheme+netloc+path."""
    try:
        parsed = urlparse(url)
        # Keep only scheme + netloc + path (strip ?query and #fragment)
        clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        # Remove trailing slashes except for root
        if clean.endswith("/") and len(clean) > len(f"{parsed.scheme}://{parsed.netloc}/"):
            clean = clean.rstrip("/")
        return clean
    except Exception:
        return url
