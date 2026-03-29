"""
discovery/directories.py — Scrapes Italian business directories filtered by ATECO 28.30.

Sources:
  - registroaziende.it
  - aziende.it
  - reportaziende.it
  - companyreports.it
  - topaziende.quotidiano.net
"""
from __future__ import annotations

import logging
import re
from urllib.parse import urljoin, urlencode

from bs4 import BeautifulSoup

from src.http_client import safe_fetch
from src.failure_logger import log_failure

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Directory configs
# ---------------------------------------------------------------------------

DIRECTORIES: list[dict] = [
    {
        "name": "registroaziende",
        "urls": [
            "https://www.registroaziende.it/qsearch?ateco=28.30",
            "https://www.registroaziende.it/ricerca?codice_ateco=28.30",
            "https://www.registroaziende.it/codici-ateco/28.30",
        ],
    },
    {
        "name": "aziende.it",
        "urls": [
            "https://www.aziende.it/ateco/28.30",
            "https://www.aziende.it/ricerca?ateco=28.30",
        ],
    },
    {
        "name": "reportaziende",
        "urls": [
            "https://www.reportaziende.it/ricerca?ateco=28.30",
            "https://www.reportaziende.it/aziende?codice_ateco=2830",
        ],
    },
    {
        "name": "companyreports",
        "urls": [
            "https://www.companyreports.it/ateco/28.30",
            "https://www.companyreports.it/search?ateco=28.30",
        ],
    },
    {
        "name": "topaziende",
        "urls": [
            "https://topaziende.quotidiano.net/ateco/28.30",
            "https://topaziende.quotidiano.net/aziende?ateco=28.30",
        ],
    },
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def discover() -> list[dict]:
    """
    Query all ATECO directories and return a de-duplicated list of companies.
    """
    all_companies: list[dict] = []
    seen_names: set[str] = set()

    for directory in DIRECTORIES:
        companies = _scrape_directory(directory)
        for c in companies:
            key = c["name"].lower().strip()
            if key and key not in seen_names:
                seen_names.add(key)
                all_companies.append(c)

    logger.info("Directories: %d unique companies found", len(all_companies))
    return all_companies


# ---------------------------------------------------------------------------
# Per-directory scraper
# ---------------------------------------------------------------------------

def _scrape_directory(directory: dict) -> list[dict]:
    src_name = directory["name"]
    for url in directory["urls"]:
        html, final_url, status = safe_fetch(url)
        if not html:
            continue
        soup = BeautifulSoup(html, "lxml")
        companies = _parse_generic(soup, final_url or url, src_name)
        if companies:
            logger.info("  %s: %d companies at %s", src_name, len(companies), final_url)
            return companies

    log_failure(
        company_name="N/A",
        stage="discovery",
        source=src_name,
        error_type="ScrapeFailure",
        error_message=f"All URLs for {src_name} failed or returned no companies",
    )
    logger.warning("  %s: no data found", src_name)
    return []


# ---------------------------------------------------------------------------
# Generic parser — handles table and card layouts
# ---------------------------------------------------------------------------

def _parse_generic(soup: BeautifulSoup, base_url: str, source: str) -> list[dict]:
    companies: list[dict] = []

    # --- Table layout ---
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if not cells:
            continue
        name = cells[0].get_text(strip=True)
        if not name or len(name) < 3:
            continue
        website, city, vat = "", "", ""
        # Look for links in all cells
        for cell in cells:
            for a in cell.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("http") and not _is_directory_link(href):
                    website = href
        # City / location heuristic
        if len(cells) >= 3:
            city = cells[2].get_text(strip=True)
        elif len(cells) >= 2:
            city = cells[1].get_text(strip=True)
        # VAT (Partita IVA) — look for IT + 11 digits
        row_text = row.get_text()
        vat_match = re.search(r"\bIT\s?(\d{11})\b|\b(\d{11})\b", row_text)
        if vat_match:
            vat = vat_match.group(0)
        if name:
            companies.append(_build_record(name, website, city, vat, source, base_url))

    if companies:
        return companies

    # --- Card / list layout ---
    card_selectors = [
        ".company", ".azienda", ".result", ".item", ".card",
        "article", "li.company", "div.result-item",
    ]
    for sel in card_selectors:
        for card in soup.select(sel):
            name_el = card.find(["h2", "h3", "h4", "strong", ".name", ".company-name"])
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or len(name) < 3:
                continue
            website = ""
            city = ""
            vat = ""
            for a in card.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("http") and not _is_directory_link(href):
                    website = href
                    break
            loc_el = card.select_one(".city, .citta, .location, .sede, .provincia")
            if loc_el:
                city = loc_el.get_text(strip=True)
            card_text = card.get_text()
            vat_match = re.search(r"\bIT\s?(\d{11})\b|\b(\d{11})\b", card_text)
            if vat_match:
                vat = vat_match.group(0)
            companies.append(_build_record(name, website, city, vat, source, base_url))
        if companies:
            return companies

    return companies


def _is_directory_link(href: str) -> bool:
    """True if the link points to the directory itself (not a company website)."""
    directory_domains = [
        "aziende.it", "registroaziende.it", "reportaziende.it",
        "companyreports.it", "quotidiano.net",
    ]
    return any(d in href for d in directory_domains)


def _build_record(
    name: str, website: str, city: str, vat: str, source: str, source_url: str
) -> dict:
    return {
        "name": name,
        "website": website,
        "location": city,
        "vat": vat,
        "source": source,
        "source_url": source_url,
    }
