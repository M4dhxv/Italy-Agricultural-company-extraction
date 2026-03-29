"""
discovery/europages.py — Scrapes Europages for Italian agricultural machinery companies.

Europages is a pan-European B2B directory. We target the category page for
"Agricultural machinery and equipment" (NACE 28.30) and filter to Italy.
"""
from __future__ import annotations

import logging
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.http_client import safe_fetch
from src.failure_logger import log_failure

logger = logging.getLogger(__name__)

_CANDIDATE_URLS = [
    "https://www.europages.co.uk/companies/Italy/pg-1/cs/agricultural-machinery-and-equipment.html",
    "https://www.europages.co.uk/companies/Italy/pg-1/cs/agricultural-machinery.html",
    "https://www.europages.com/en/companies/Italy/agricultural-machinery-and-equipment/",
    "https://www.europages.it/aziende/Italia/pg-1/cs/macchine-e-attrezzature-per-l-agricoltura.html",
    "https://www.europages.it/aziende/Italia/macchine-agricole.html",
]

_SOURCE_NAME = "europages"
_MAX_PAGES = 5   # scrape up to 5 pages (typically 10 results/page → 50 companies)


def discover() -> list[dict]:
    """
    Scrape Europages for Italian agri-machinery companies.

    Returns list of:
        {name, website, description, location, source, source_url}
    """
    all_companies: list[dict] = []
    seen: set[str] = set()

    for base_url in _CANDIDATE_URLS:
        page_companies = _scrape_paginated(base_url)
        for c in page_companies:
            key = c["name"].lower().strip()
            if key and key not in seen:
                seen.add(key)
                all_companies.append(c)
        if all_companies:
            break   # stop at first working URL pattern

    if not all_companies:
        log_failure(
            company_name="N/A",
            stage="discovery",
            source=_SOURCE_NAME,
            error_type="ScrapeFailure",
            error_message="All Europages URL patterns failed or returned empty",
        )
        logger.warning("Europages: no data found")

    logger.info("Europages: %d companies found", len(all_companies))
    return all_companies


def _scrape_paginated(base_url: str) -> list[dict]:
    """Scrape page 1..N until empty or max pages reached."""
    companies: list[dict] = []

    for page_num in range(1, _MAX_PAGES + 1):
        url = _build_page_url(base_url, page_num)
        html, final_url, status = safe_fetch(url)
        if not html:
            break
        soup = BeautifulSoup(html, "lxml")
        page_companies = _parse_page(soup, final_url or url)
        if not page_companies:
            break
        companies.extend(page_companies)
        logger.debug("  Europages page %d: %d results", page_num, len(page_companies))

    return companies


def _build_page_url(base_url: str, page: int) -> str:
    if page == 1:
        return base_url
    # Replace pagination segment
    if "/pg-1/" in base_url:
        return base_url.replace("/pg-1/", f"/pg-{page}/")
    if base_url.endswith("/"):
        return base_url + f"?page={page}"
    return base_url + f"?page={page}"


def _parse_page(soup: BeautifulSoup, source_url: str) -> list[dict]:
    companies: list[dict] = []

    # Pattern A: standard Europages company card
    card_selectors = [
        ".company-card", ".result", ".paginated-result", ".company",
        "article", ".listing-item", "li.company",
    ]
    for sel in card_selectors:
        for card in soup.select(sel):
            name = ""
            website = ""
            description = ""
            location = ""

            name_el = card.find(["h2", "h3", "h4", ".company-name", ".name"])
            if name_el:
                name = name_el.get_text(strip=True)

            for a in card.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("http") and "europages" not in href:
                    website = href
                    break

            desc_el = card.select_one(".description, .activity, p")
            if desc_el:
                description = desc_el.get_text(strip=True)[:500]

            loc_el = card.select_one(".address, .location, .city, .country")
            if loc_el:
                location = loc_el.get_text(strip=True)

            if name and len(name) > 2:
                companies.append(_build_record(name, website, description, location, source_url))

        if companies:
            return companies

    return companies


def _build_record(
    name: str, website: str, description: str, location: str, source_url: str
) -> dict:
    return {
        "name": name,
        "website": website,
        "description": description,
        "location": location,
        "source": _SOURCE_NAME,
        "source_url": source_url,
    }
