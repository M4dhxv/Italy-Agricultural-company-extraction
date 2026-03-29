"""
directors_extractor.py — Extract CEO and directors via Apify search.
"""
from __future__ import annotations

import logging
import re
from typing import Any

from src.config import DIRECTORS_QUERY_TEMPLATES
from src.apify_client import run_apify_search
from src.failure_logger import log_failure

logger = logging.getLogger(__name__)

# Regex to match capitalized Italian name patterns (Firstname Lastname)
_NAME_PATTERN = re.compile(
    r"\b([A-ZÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ][a-zàáâãäåæçèéêëìíîïðñòóôõöøùúûüý]+"
    r"\s+(?:[A-ZÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝ][a-zàáâãäåæçèéêëìíîïðñòóôõöøùúûüý]+))"
    r"\b"
)

# Context words that indicate a person's role follows or precedes
_ROLE_CONTEXTS = [
    "amministratore delegato", "ad:", "a.d.", "ceo", "direttore generale",
    "presidente", "chairman", "managing director", "general manager",
    "fondatore", "founder", "proprietario", "owner",
]


def extract_directors(company_name: str) -> dict[str, Any]:
    """
    Search for directors/CEO of a company.

    Returns:
        {
            "directors": [str],            names found
            "directors_raw_text": [str],   snippets where names were found
            "directors_sources": [str],    URLs
        }
    """
    directors: set[str] = set()
    raw_snippets: list[str] = []
    sources: list[str] = []

    for template in DIRECTORS_QUERY_TEMPLATES:
        query = template.format(name=company_name)
        try:
            results = run_apify_search(query, max_results=5)
        except Exception as exc:
            log_failure(
                company_name=company_name,
                stage="directors_extraction",
                source=query,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            continue

        for r in results:
            snippet = f"{r.get('title', '')} {r.get('description', '')}".strip()
            url = r.get("url", "")
            if not snippet:
                continue

            names = _extract_names_from_snippet(snippet)
            if names:
                raw_snippets.append(snippet[:400])
                sources.append(url)
                directors.update(names)

        # Also mine AI overview
        ai_text = results[0].get("ai_overview", "") if results else ""
        if ai_text:
            names = _extract_names_from_snippet(ai_text)
            directors.update(names)

    return {
        "directors": sorted(directors),
        "directors_raw_text": raw_snippets[:5],
        "directors_sources": list(dict.fromkeys(sources))[:5],
    }


def _extract_names_from_snippet(text: str) -> list[str]:
    """Extract person names near role-context words."""
    names: list[str] = []
    text_lower = text.lower()

    for context in _ROLE_CONTEXTS:
        idx = text_lower.find(context)
        if idx == -1:
            continue
        # Look in a window of 100 characters around the context
        window_start = max(0, idx - 50)
        window_end = min(len(text), idx + len(context) + 80)
        window = text[window_start:window_end]
        found = _NAME_PATTERN.findall(window)
        names.extend(found)

    # Also try scanning the full text for names in name-shaped patterns
    all_names = _NAME_PATTERN.findall(text)
    # Only include names that appear near a role context
    for n in all_names:
        n_idx = text.find(n)
        surrounding = text[max(0, n_idx - 60):n_idx + len(n) + 60].lower()
        if any(ctx in surrounding for ctx in _ROLE_CONTEXTS):
            names.append(n)

    # Deduplicate
    return list(dict.fromkeys(names))
