"""
financial_extractor.py — Multi-source financial data extraction via Apify search.

Performs yearly clustering of extracted values using precision normalization,
tracking all historical data, computing centroid buckets, and scoring confidence.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from src.config import FINANCIAL_QUERY_TEMPLATES
from src.apify_client import run_apify_search
from src.failure_logger import log_failure
from src.http_client import safe_fetch

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regex patterns for financial value extraction
# ---------------------------------------------------------------------------

_REVENUE_PATTERNS = [
    r"fatturato\s*(?:di\s*)?(?:circa\s*)?(?:€\s*)?(\d[\d.,]+\s*(?:milioni?|mln|mld|miliard[oi]))",
    r"fatturato\s*(?:di\s*)?(?:circa\s*)?(?:€\s*)?(\d[\d.,]+)",
    r"ricavi\s*(?:pari\s*a\s*)?(?:circa\s*)?(?:€\s*)?(\d[\d.,]+\s*(?:milioni?|mln|mld|miliard[oi]))",
    r"ricavi\s*(?:pari\s*a\s*)?(?:circa\s*)?(?:€\s*)?(\d[\d.,]+)",
    r"revenue\s*(?:of\s*)?(?:approx\.\s*)?(?:€\s*|EUR\s*)?(\d[\d.,]+\s*(?:million|mln|bn))",
    r"revenue\s*(?:of\s*)?(?:approx\.\s*)?(?:€\s*|EUR\s*)?(\d[\d.,]+)",
    r"(?:€|EUR)\s*(\d[\d.,]+\s*(?:milioni?|mln|million|bn|mld))",
]

_NET_INCOME_PATTERNS = [
    r"utile\s*(?:netto\s*)?(?:di\s*)?(?:€\s*)?(\d[\d.,]+\s*(?:milioni?|mln|mld))",
    r"utile\s*(?:netto\s*)?(?:di\s*)?(?:€\s*)?(\d[\d.,]+)",
    r"risultato\s*(?:d[ie])\s*esercizio\s*(?:€\s*)?(\d[\d.,]+)",
    r"net\s*income\s*(?:of\s*)?(?:€\s*|EUR\s*)?(\d[\d.,]+)",
]

_YEAR_PATTERNS = [
    r"\b(20[123]\d)\b",  # 2010–2039
]

_EMPLOYEE_PATTERNS = [
    r"(\d[\d.,]*)\s*(?:dipendenti|employees|persone|collaboratori|addetti)",
    r"(?:dipendenti|employees|staff|personale)\s*(?:di\s*)?(\d[\d.,]*)",
    r"\b(\d{1,4})\s*(?:persone|persone\s+impiegate)",
]

TRUSTED_SOURCES = {"fatturatoitalia.it", "ufficiocamerale.it", "reportaziende.it"}


def _normalize_italian_number(text: str) -> float | None:
    text = text.lower().replace('€', '').replace('eur', '').strip()
    multiplier = 1.0
    if any(kw in text for kw in ['milion', 'mln', 'million']):
        multiplier = 1e6
    elif any(kw in text for kw in ['miliard', 'billion', 'mld', 'bn']):
        multiplier = 1e9

    m = re.search(r'[\d.,]+', text)
    if not m:
        return None
    num_str = m.group(0).rstrip('.,')

    # Handle Italian vs English decimal structures
    if ',' in num_str and '.' in num_str:
        if num_str.rfind(',') > num_str.rfind('.'):
            num_str = num_str.replace('.', '').replace(',', '.')
        else:
            num_str = num_str.replace(',', '')
    else:
        if '.' in num_str:
            if num_str.count('.') > 1 or len(num_str) - num_str.find('.') == 4:
                num_str = num_str.replace('.', '')
        elif ',' in num_str:
            if num_str.count(',') > 1 or len(num_str) - num_str.find(',') == 4:
                num_str = num_str.replace(',', '')
            else:
                num_str = num_str.replace(',', '.')

    try:
        return float(num_str) * multiplier
    except ValueError:
        return None


def _cluster_records(records: list[dict], metric_key: str) -> list[dict]:
    # Filter only records that have this metric
    valid_recs = [r for r in records if r.get(metric_key) is not None]
    if not valid_recs:
        return []

    # Sort ascending for clustering stability
    valid_recs.sort(key=lambda x: x[f"{metric_key}_normalized"])

    clusters = []
    for rec in valid_recs:
        val = rec[f"{metric_key}_normalized"]
        assigned = False
        for cluster in clusters:
            centroid = cluster['centroid']
            if centroid == 0:
                if val == 0:
                    assigned = True
            elif abs(val - centroid) / centroid <= 0.10:
                assigned = True
                
            if assigned:
                cluster['records'].append(rec)
                cluster['centroid'] = sum(r[f"{metric_key}_normalized"] for r in cluster['records']) / len(cluster['records'])
                break
                
        if not assigned:
            clusters.append({
                'centroid': val,
                'records': [rec]
            })
            
    # Sort clusters descending by size
    clusters.sort(key=lambda c: len(c['records']), reverse=True)
    return clusters


def _select_best_from_clusters(clusters: list[dict], metric_key: str) -> dict | None:
    if not clusters:
        return None
        
    largest = clusters[0]
    
    # Priority: 1. Trusted Source, 2. Most recent year, 3. First occurrence
    # Since we don't have global occurrence index easily preserved in clustering sort,
    # we'll use original sort if possible, but we'll sort explicitly:
    def rank(r):
        p1 = 0 if any(t in r['source'] for t in TRUSTED_SOURCES) else 1
        p2 = -int(r['year']) if str(r.get('year', '')).isdigit() else 0
        return (p1, p2)
        
    best_rec = min(largest['records'], key=rank)
    
    # Evaluate confidence
    size = len(largest['records'])
    has_trusted = any(any(t in r['source'] for t in TRUSTED_SOURCES) for r in largest['records'])
    
    if size >= 2 and has_trusted:
        confidence = "HIGH"
    elif size >= 2:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"
        
    # Standardize result structure
    return {
        "value_raw": best_rec[metric_key],
        "value_normalized": best_rec[f"{metric_key}_normalized"],
        "source": best_rec['source'],
        "confidence": confidence,
        "cluster_size": size
    }


def extract_financials(company_name: str) -> dict[str, Any]:
    raw_records: list[dict] = []
    employee_count: int | None = None
    all_results: list[dict] = []

    for template in FINANCIAL_QUERY_TEMPLATES:
        query = template.format(name=company_name)
        try:
            results = run_apify_search(query, max_results=5)
            for r in results:
                r['_query'] = query
                all_results.append(r)
        except Exception as exc:
            log_failure(
                company_name=company_name,
                stage="financial_extraction",
                source=query,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            continue

    # --- Fetch Upgrade: top 2 unique domains ---
    # 1. Deduplicate by domain, sort by trust
    domain_to_result = {}
    for r in all_results:
        url = r.get("url", "")
        if url:
            dom = _extract_domain(url)
            if dom not in domain_to_result:
                domain_to_result[dom] = r

    # Sort domains: Trusted first
    def _rank_dom(d: str) -> int:
        return 0 if any(t in d for t in TRUSTED_SOURCES) else 1

    sorted_domains = sorted(domain_to_result.keys(), key=_rank_dom)
    top_2_urls = [domain_to_result[d]["url"] for d in sorted_domains[:2]]

    fetched_content: dict[str, str] = {}
    from bs4 import BeautifulSoup
    for url in top_2_urls:
        try:
            # lightweight fetch
            html, url_eff, status = safe_fetch(url, timeout=6)
            if html and status == 200:
                soup = BeautifulSoup(html, "lxml")
                for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
                    tag.decompose()
                fetched_text = soup.get_text(separator=" ", strip=True)
                fetched_content[url] = re.sub(r"\s+", " ", fetched_text).strip()
        except Exception as exc:
            logger.debug("Failed lightweight fetch for %s: %s", url, exc)

    # --- Processing ---
    for r in all_results:
        url = r.get("url", "")
        desc = r.get("description", "")
        title = r.get("title", "")
        query = r.get("_query", "")
        
        # Override with fetched full-text if available
        if url in fetched_content and len(fetched_content[url]) > 50:
            raw_text = fetched_content[url]
        else:
            raw_text = f"{title} {desc}".strip()
            
        if not raw_text:
            continue

        source_domain = _extract_domain(url)
        year = _extract_year(raw_text)

        revenue = _extract_value(raw_text, _REVENUE_PATTERNS)
        rev_norm = _normalize_italian_number(revenue) if revenue else None

        net_income = _extract_value(raw_text, _NET_INCOME_PATTERNS)
        net_norm = _normalize_italian_number(net_income) if net_income else None

        if revenue or net_income:
            raw_records.append({
                "source": source_domain,
                "year": year,
                "revenue": revenue,
                "revenue_normalized": rev_norm,
                "net_income": net_income,
                "net_income_normalized": net_norm,
                "raw_text": raw_text[:800],
                "url": url,
                "query": query,
            })

        if employee_count is None:
            emp = _extract_value(raw_text, _EMPLOYEE_PATTERNS)
            norm_emp = _normalize_italian_number(emp) if emp else None
            if norm_emp is not None:
                employee_count = int(norm_emp)

    yearly_groups = {}
    for r in raw_records:
        y = r.get("year")
        if not y:
            y = "unknown"
        yearly_groups.setdefault(y, []).append(r)
        
    yearly_output = {}
    
    for year, recs in yearly_groups.items():
        rev_clusters = _cluster_records(recs, "revenue")
        net_clusters = _cluster_records(recs, "net_income")
        
        rev_best = _select_best_from_clusters(rev_clusters, "revenue")
        net_best = _select_best_from_clusters(net_clusters, "net_income")
        
        yearly_output[year] = {
            "revenue_clusters": [
                {
                    "centroid": c['centroid'],
                    "size": len(c['records']),
                    "sources": list(set(r['source'] for r in c['records']))
                } for c in rev_clusters
            ],
            "net_income_clusters": [
                {
                    "centroid": c['centroid'],
                    "size": len(c['records']),
                    "sources": list(set(r['source'] for r in c['records']))
                } for c in net_clusters
            ],
            "selected": {
                "revenue": rev_best["value_raw"] if rev_best else "",
                "revenue_normalized": rev_best["value_normalized"] if rev_best else None,
                "revenue_source": rev_best["source"] if rev_best else "",
                "revenue_confidence": rev_best["confidence"] if rev_best else "NONE",
                "net_income": net_best["value_raw"] if net_best else "",
                "net_income_normalized": net_best["value_normalized"] if net_best else None,
                "net_income_source": net_best["source"] if net_best else "",
                "net_income_confidence": net_best["confidence"] if net_best else "NONE",
            }
        }

    return {
        "raw_records": raw_records,
        "yearly": yearly_output,
        "employee_count": employee_count,
    }


def _extract_value(text: str, patterns: list[str]) -> str:
    text_lower = text.lower()
    for pattern in patterns:
        try:
            m = re.search(pattern, text_lower)
            if m:
                return m.group(1).strip()
        except re.error:
            continue
    return ""


def _extract_year(text: str) -> str:
    for pattern in _YEAR_PATTERNS:
        years = re.findall(pattern, text)
        if years:
            return max(years)
    return ""


def _extract_domain(url: str) -> str:
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc
    except Exception:
        return url
