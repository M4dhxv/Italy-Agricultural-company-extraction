"""
discovery/eima.py — Scrapes EIMA International exhibitor list.

EIMA is the world's largest agricultural machinery trade fair, held in Bologna.
The 2024 edition exhibitor list is the primary target.
"""
from __future__ import annotations

import logging
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.http_client import safe_fetch
from src.failure_logger import log_failure

logger = logging.getLogger(__name__)

_CANDIDATE_URLS = [
    "https://www.eima.it/en/exhibitors/",
    "https://www.eima.it/espositori/",
    "https://espositori.eima.it/",
    "https://www.eima.it/en/exhibitors/list/",
    "https://www.eima.it/eima2024/en/exhibitors/",
    "https://www.eima.it/eima2024/espositori/",
]

_SOURCE_NAME = "eima"


def discover() -> list[dict]:
    """
    Scrape EIMA exhibitor list.

    Returns list of:
        {name, website, country, source, source_url}
    """
    for url in _CANDIDATE_URLS:
        html, final_url, status = safe_fetch(url)
        if not html:
            continue

        soup = BeautifulSoup(html, "lxml")
        companies = _parse_page(soup, final_url or url)
        if companies:
            # Filter to Italian companies only
            italian = [
                c for c in companies
                if not c.get("country") or c["country"].upper() in ("IT", "ITALY", "ITALIA", "")
            ]
            logger.info(
                "EIMA: found %d total / %d Italian exhibitors at %s",
                len(companies), len(italian), final_url,
            )
            return italian if italian else companies

    log_failure(
        company_name="N/A",
        stage="discovery",
        source=_SOURCE_NAME,
        error_type="ScrapeFailure",
        error_message="All EIMA URL patterns failed or returned empty",
    )
    logger.warning("EIMA: all URLs failed or returned no data")
    return []


def _parse_page(soup: BeautifulSoup, base_url: str) -> list[dict]:
    companies: list[dict] = []

    # Pattern A: table
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) < 1:
            continue
        name = cells[0].get_text(strip=True)
        website = ""
        country = ""
        if len(cells) > 1:
            country = cells[1].get_text(strip=True)
        link = row.find("a", href=True)
        if link:
            href = link["href"].strip()
            if href.startswith("http") and "eima.it" not in href:
                website = href
        if name and len(name) > 2:
            companies.append(_build_record(name, website, country, base_url))

    if companies:
        return companies

    # Pattern B: card / article
    selectors = [
        ".exhibitor", ".espositore", ".company", ".azienda",
        ".card", "article", ".list-item",
    ]
    for sel in selectors:
        for card in soup.select(sel):
            name_el = card.find(["h2", "h3", "h4", "strong", ".name", ".title"])
            name = name_el.get_text(strip=True) if name_el else ""
            website = ""
            country = ""
            for a in card.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("http") and "eima.it" not in href:
                    website = href
                    break
            country_el = card.select_one(".country, .nazione, .paese")
            if country_el:
                country = country_el.get_text(strip=True)
            if name and len(name) > 2:
                companies.append(_build_record(name, website, country, base_url))
        if companies:
            return companies

    # Pattern C: plain anchor list
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        name = a.get_text(strip=True)
        if (
            href.startswith("http")
            and "eima.it" not in href
            and name
            and len(name) > 3
        ):
            companies.append(_build_record(name, href, "", base_url))

    return companies


def _build_record(name: str, website: str, country: str, source_url: str) -> dict:
    return {
        "name": name,
        "website": website,
        "country": country,
        "source": _SOURCE_NAME,
        "source_url": source_url,
    }
