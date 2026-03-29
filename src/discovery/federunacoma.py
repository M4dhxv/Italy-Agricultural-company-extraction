"""
discovery/federunacoma.py — Scrapes FederUnacoma member list.

FederUnacoma is the Italian federation of agricultural machinery manufacturers.
Their members page lists accredited manufacturers with websites.

URL patterns tried (in order):
  - https://www.federunacoma.it/en/associates/
  - https://www.federunacoma.it/associati/
  - https://www.federunacoma.it/imprese-associate/
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
    "https://www.federunacoma.it/en/associates/",
    "https://www.federunacoma.it/associati/",
    "https://www.federunacoma.it/imprese-associate/",
    "https://www.federunacoma.it/en/members/",
]

_SOURCE_NAME = "federunacoma"


def discover() -> list[dict]:
    """
    Scrape FederUnacoma member list.

    Returns list of:
        {name, website, location, source, source_url}
    """
    for url in _CANDIDATE_URLS:
        html, final_url, status = safe_fetch(url)
        if not html:
            continue

        soup = BeautifulSoup(html, "lxml")
        companies = _parse_page(soup, final_url or url)
        if companies:
            logger.info("FederUnacoma: found %d companies at %s", len(companies), final_url)
            return companies

    log_failure(
        company_name="N/A",
        stage="discovery",
        source=_SOURCE_NAME,
        error_type="ScrapeFailure",
        error_message="All FederUnacoma URL patterns failed or returned empty",
    )
    logger.warning("FederUnacoma: all URLs failed")
    return []


def _parse_page(soup: BeautifulSoup, base_url: str) -> list[dict]:
    companies: list[dict] = []

    # Pattern A: table-based layout
    for row in soup.select("table tr"):
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        name_cell = cells[0].get_text(strip=True)
        if not name_cell or len(name_cell) < 3:
            continue
        website = ""
        location = ""
        for cell in cells:
            link = cell.find("a", href=True)
            if link:
                href = link["href"].strip()
                if href.startswith("http") and "federunacoma" not in href:
                    website = href
            for cls_hint in ["citta", "city", "location", "sede"]:
                if cls_hint in str(cell.get("class", "")):
                    location = cell.get_text(strip=True)
        if name_cell:
            companies.append(_build_record(name_cell, website, location, base_url))

    if companies:
        return companies

    # Pattern B: card/list layout
    for card in soup.select(".associate, .member, .azienda, .impresa, .company-card, article"):
        name = ""
        website = ""
        location = ""

        heading = card.find(["h2", "h3", "h4", "strong"])
        if heading:
            name = heading.get_text(strip=True)

        for a in card.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith("http") and "federunacoma" not in href:
                website = href
                break

        loc_el = card.select_one(".location, .city, .sede, .citta")
        if loc_el:
            location = loc_el.get_text(strip=True)

        if name:
            companies.append(_build_record(name, website, location, base_url))

    return companies


def _build_record(name: str, website: str, location: str, source_url: str) -> dict:
    return {
        "name": name,
        "website": website,
        "location": location,
        "source": _SOURCE_NAME,
        "source_url": source_url,
    }
