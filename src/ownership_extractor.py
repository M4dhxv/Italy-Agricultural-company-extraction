"""
ownership_extractor.py — Classify company as independent or subsidiary via Apify search.
"""
from __future__ import annotations

import logging
import re
from typing import Any

from src.config import OWNERSHIP_QUERY_TEMPLATES
from src.apify_client import run_apify_search
from src.failure_logger import log_failure

logger = logging.getLogger(__name__)

# Signals that indicate subsidiary/group membership
_SUBSIDIARY_SIGNALS = [
    "gruppo", "group", "subsidiary", "subsidiaria",
    "acquisita da", "acquired by", "holding", "parent company",
    "controllata da", "parte del gruppo", "part of the group",
    "joint venture", "division of", "divisione di",
    "owned by", "owned by", "di proprietà di",
]

# Signals that indicate independence
_INDEPENDENT_SIGNALS = [
    "azienda familiare", "family business", "impresa familiare",
    "indipendente", "independent", "privately held",
    "a conduzione familiare", "gestione familiare",
]


def extract_ownership(company_name: str) -> dict[str, Any]:
    """
    Classify company as 'independent' or 'subsidiary' based on search evidence.

    Returns:
        {
            "ownership_classification": "independent" | "subsidiary" | "unknown",
            "subsidiary_evidence":      str,
            "parent_company":           str,
            "ownership_sources":        [str],
        }
    """
    subsidiary_evidence_parts: list[str] = []
    independent_evidence_parts: list[str] = []
    sources: list[str] = []
    parent_mentions: list[str] = []

    for template in OWNERSHIP_QUERY_TEMPLATES:
        query = template.format(name=company_name)
        try:
            results = run_apify_search(query, max_results=5)
        except Exception as exc:
            log_failure(
                company_name=company_name,
                stage="ownership_extraction",
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
            snippet_lower = snippet.lower()

            sub_hits = [sig for sig in _SUBSIDIARY_SIGNALS if sig in snippet_lower]
            ind_hits = [sig for sig in _INDEPENDENT_SIGNALS if sig in snippet_lower]

            if sub_hits:
                subsidiary_evidence_parts.append(snippet[:300])
                sources.append(url)
                parent = _extract_parent_name(snippet, company_name)
                if parent:
                    parent_mentions.append(parent)

            if ind_hits:
                independent_evidence_parts.append(snippet[:200])

        # Check AI overview too
        if results:
            ai = results[0].get("ai_overview", "")
            if ai:
                ai_lower = ai.lower()
                if any(sig in ai_lower for sig in _SUBSIDIARY_SIGNALS):
                    subsidiary_evidence_parts.append(f"[AI Overview] {ai[:300]}")
                    parent = _extract_parent_name(ai, company_name)
                    if parent:
                        parent_mentions.append(parent)

    # Classification logic
    if subsidiary_evidence_parts and not independent_evidence_parts:
        classification = "subsidiary"
    elif independent_evidence_parts and not subsidiary_evidence_parts:
        classification = "independent"
    elif subsidiary_evidence_parts and independent_evidence_parts:
        # Evidence both ways — lean subsidiary (conservative)
        classification = "subsidiary"
    else:
        classification = "unknown"

    return {
        "ownership_classification": classification,
        "subsidiary_evidence": " | ".join(subsidiary_evidence_parts[:3]),
        "parent_company": parent_mentions[0] if parent_mentions else "",
        "ownership_sources": list(dict.fromkeys(sources))[:5],
    }


def _extract_parent_name(text: str, company_name: str) -> str:
    """Try to extract the parent group/company name from surrounding text."""
    patterns = [
        r"(?:gruppo|group|acquired by|acquisita da|parte del gruppo)\s+([A-Z][A-Za-z\s&\-]{2,30})",
        r"(?:subsidiary of|subsidiaria di|controllata da)\s+([A-Z][A-Za-z\s&\-]{2,30})",
        r"([A-Z][A-Za-z\s&]{3,25})\s+(?:group|gruppo|holding)",
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            candidate = m.group(1).strip()
            if candidate.lower() != company_name.lower():
                return candidate
    return ""
