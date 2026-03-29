"""
scraper.py — Smart website scraper.

Strategy:
  1. Fetch homepage
  2. Discover ALL internal links that actually exist on the site
  3. Score each link — keep only RELEVANT pages (about, products, company info)
  4. Skip USELESS pages (contact, privacy, cart, login, blog, news, etc.)
  5. Fetch only the pages that passed the filter (max 12 pages total)
  6. If BS4 result is too thin → fall back to Apify crawler
"""
from __future__ import annotations

import logging
import re
import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src.config import (
    REQUEST_DELAY,
    KEYWORD_MAP,
    MANUFACTURER_ACCEPT_SIGNALS,
)
from src.http_client import safe_fetch
from src.failure_logger import log_failure
from src.apify_client import run_apify_website_crawler

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Page relevance scoring
# ---------------------------------------------------------------------------

KEYWORDS = {
    "about": [
        "about", "about-us", "aboutus",
        "azienda", "chi-siamo", "chi siamo",
        "company", "storia", "history",
        "impresa"
    ],
    "products": [
        "products", "product",
        "prodotti", "prodotto",
        "macchine", "macchina",
        "machines",
        "catalogo", "catalog",
        "linea", "linee",
        "gamma", "range",
        "attrezzature",
        "applicazioni", "applications",
        "settori", "industries"
    ],
    "contact": [
        "contact", "contacts",
        "contatti", "contattaci",
        "contattare", "get-in-touch"
    ],
    "brands": [
        "brands", "brand",
        "marchi", "divisioni"
    ],
    "exclude": [
        "privacy", "policy", "cookie", "cookies",
        "terms", "conditions",
        "login", "signin", "register", "account",
        "cart", "checkout", "wishlist",
        "pdf", "download", "faq",
        "blog", "news", "event", "eventi",
        "career", "careers", "lavora", "lavoro"
    ]
}

MAX_PAGES = 6          # cap total pages scraped per company
MIN_TEXT_LENGTH = 1500  # chars needed to pass validation


def scrape_company(website_url: str, company_name: str = "") -> tuple[dict[str, str], str]:
    """
    Smart scraper: discover real pages, keep relevant ones, skip useless ones.

    Returns:
        (pages_dict, scrape_source) — source is 'bs4', 'apify_fallback', or 'none'.
    """
    base = _normalise_base(website_url)
    if not base:
        return {}, "none"

    scraped: dict[str, str] = {}

    # ------------------------------------------------------------------ #
    # Step 1: Fetch homepage
    # ------------------------------------------------------------------ #
    homepage_text, homepage_html = _fetch_page(base, company_name)
    if homepage_text:
        scraped[base] = homepage_text

    if not homepage_html:
        logger.warning("Homepage fetch failed for %s, trying Apify", base)
        return _try_apify(base, company_name)

    # ------------------------------------------------------------------ #
    # Step 2: Discover all internal links from the homepage
    # ------------------------------------------------------------------ #
    all_links = _discover_internal_links(base, homepage_html)
    logger.debug("Discovered %d internal links on %s", len(all_links), base)

    # ------------------------------------------------------------------ #
    # Step 3: Score and filter — keep only relevant pages
    # ------------------------------------------------------------------ #
    prioritised = _rank_links(all_links)
    logger.info(
        "  Scraper: %d pages discovered → %d relevant pages to fetch",
        len(all_links), len(prioritised),
    )

    # ------------------------------------------------------------------ #
    # Step 4: Fetch relevant pages (up to MAX_PAGES total including homepage)
    # ------------------------------------------------------------------ #
    fetched = 1 if homepage_text else 0
    # Pre-slice: never iterate more than MAX_PAGES candidates to avoid 100+ URL loops
    seen_texts: set[int] = {hash(homepage_text)} if homepage_text else set()
    for url in prioritised[:MAX_PAGES * 3]:
        if fetched >= MAX_PAGES:
            break
        if url in scraped:
            continue
        time.sleep(REQUEST_DELAY * 0.4)
        text, _ = _fetch_page(url, company_name)
        if text:
            # Redirect dedup: skip if we already have this exact content (e.g. claas.it loop)
            text_hash = hash(text[:500])
            if text_hash in seen_texts:
                continue
            seen_texts.add(text_hash)
            scraped[url] = text
            fetched += 1

    # ------------------------------------------------------------------ #
    # Step 5: Validate — if thin, try Apify fallback
    # ------------------------------------------------------------------ #
    if _validate_scrape(scraped):
        logger.info("  BS4 scraped %d pages for %r (%d chars total)",
                    len(scraped), company_name or base,
                    sum(len(t) for t in scraped.values()))
        return scraped, "bs4"

    logger.warning(
        "  BS4 validation failed for %r (not enough content/keywords). Trying Apify.",
        company_name or base,
    )
    return _try_apify(base, company_name)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_page(url: str, company_name: str) -> tuple[str, str]:
    """
    Fetch URL and return (clean_text, raw_html).
    Returns ("", "") on failure.
    """
    html, _, status = safe_fetch(url)
    if not html:
        return "", ""
    try:
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        text = re.sub(r"\s+", " ", text).strip()
        return text, html
    except Exception as exc:
        log_failure(
            company_name=company_name, stage="scrape_parse",
            source=url, error_type=type(exc).__name__, error_message=str(exc),
        )
        return "", ""


def _discover_internal_links(base: str, html: str) -> list[str]:
    """
    Parse all internal links from the homepage HTML.
    Normalize, collapse depth, and deduplicate to prevent URL explosion.
    """
    base_netloc = urlparse(base).netloc.lower()
    found: set[str] = set()
    try:
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            raw = a["href"].strip()
            if not raw or raw.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            
            full = urljoin(base, raw)
            parsed = urlparse(full)
            if parsed.netloc.lower() != base_netloc:
                continue
                
            # 1. Normalize
            path = parsed.path.lower().rstrip("/")
            segments = [s for s in path.split("/") if s]
            
            # 2. Collapse depth to first 3 segments
            if len(segments) > 3:
                path = "/" + "/".join(segments[:3])
            elif segments:
                path = "/" + "/".join(segments)
            else:
                path = "/"
                
            # 3. Deduplicate via set
            clean = f"{parsed.scheme}://{parsed.netloc.lower()}{path}"
            found.add(clean)
    except Exception:
        pass
    return list(found)


def _rank_links(links: list[str]) -> list[str]:
    """
    Enforce strict priority selection and logical categorization.
    """
    contact_links = []
    about_links = []
    product_links = []
    brand_links = []
    others = []
    
    for url in links:
        path = urlparse(url).path.lower()

        if any(kw in path for kw in KEYWORDS["exclude"]):
            continue

        if any(kw in path for kw in KEYWORDS["contact"]):
            contact_links.append(url)
        elif any(kw in path for kw in KEYWORDS["about"]):
            about_links.append(url)
        elif any(kw in path for kw in KEYWORDS["products"]):
            product_links.append(url)
        elif any(kw in path for kw in KEYWORDS["brands"]):
            brand_links.append(url)
        else:
            others.append(url)

    # Sort shortest paths first (prioritizes root/hub pages over details)
    contact_links.sort(key=len)
    about_links.sort(key=len)
    product_links.sort(key=len)
    brand_links.sort(key=len)
    others.sort(key=len)
    
    # Enforce strict category caps
    contact_links = contact_links[:1]
    about_links = about_links[:2]
    product_links = product_links[:2]
    brand_links = brand_links[:1]
    others = others[:2]  # Cap others too — prevent massive sitemaps flooding remaining slots

    # Strict priority selection queue. The 6 MAX_PAGES cap enforces the cutoff.
    return contact_links + about_links + product_links + brand_links + others


def _validate_scrape(scraped: dict[str, str]) -> bool:
    """
    True if total text > MIN_TEXT_LENGTH.
    (Keyword checks removed to stop triggering Apify fallbacks on junk companies).
    """
    if not scraped:
        return False
    total_text = " ".join(scraped.values()).lower()
    return len(total_text) > MIN_TEXT_LENGTH


def _try_apify(base: str, company_name: str) -> tuple[dict[str, str], str]:
    """Fall back to Apify website-content-crawler."""
    try:
        fallback = run_apify_website_crawler(base)
        if fallback:
            return fallback, "apify_fallback"
    except Exception as exc:
        logger.error("Apify fallback failed for %s: %s", base, exc)
        log_failure(
            company_name=company_name, stage="apify_fallback",
            source=base, error_type=type(exc).__name__, error_message=str(exc),
        )
    return {}, "none"


def _normalise_base(url: str) -> str:
    """Ensure URL has a scheme and return scheme + netloc."""
    if not url:
        return ""
    if not url.startswith("http"):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return url
