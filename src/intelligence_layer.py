"""
intelligence_layer.py — Gemini 2.5 Flash classification layer
"""
import json
import logging
import os
import re
from typing import Any
from urllib.parse import urlparse

from tenacity import retry, wait_exponential, stop_after_attempt
from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

def text_heuristics_language(text: str) -> str:
    """Light heuristic to detect Italian vs English."""
    italian_words = {"di", "e", "il", "la", "che", "per", "macchine", "agricole", "azienda"}
    english_words = {"the", "and", "of", "to", "a", "in", "machinery", "agricultural", "company"}
    
    words = set(re.findall(r'\b\w+\b', text.lower()))
    it_count = len(words.intersection(italian_words))
    en_count = len(words.intersection(english_words))
    
    if it_count > en_count and it_count > 2:
        return "Italian"
    elif en_count > it_count and en_count > 2:
        return "English"
    return "unknown"

def build_ai_input_text(scraped_pages: dict[str, str]) -> str:
    """
    Build text for Gemini — manufacturer-relevant pages only.

    Order: about/company > products/catalog > homepage > other relevant
    Excludes: contact, privacy, legal, news, career, cookie pages
    Truncates at 15000 chars.
    """
    # Pages to skip for Gemini — contact has phone/email but doesn't help
    # classify manufacturer status or products
    GEMINI_SKIP = [
        "contact", "contacts", "contatti", "contattaci", "contattare", "get-in-touch",
        "privacy", "policy", "cookie", "cookies", "terms", "conditions",
        "login", "signin", "register", "account", "cart", "checkout", "wishlist",
        "pdf", "download", "faq", "blog", "news", "event", "eventi",
        "career", "careers", "lavora", "lavoro"
    ]

    def _is_gemini_relevant(url: str) -> bool:
        low = url.lower()
        return not any(sig in low for sig in GEMINI_SKIP)

    # Filter to manufacturer-relevant URLs only
    relevant = {url: text for url, text in scraped_pages.items()
                if _is_gemini_relevant(url)}

    ordered_texts = []
    used_urls = set()

    # Priority 0: Homepage ALWAYS first
    for url, text in relevant.items():
        path = urlparse(url).path
        if path in ("", "/", "/index.html", "/index.php"):
            if url not in used_urls:
                ordered_texts.append(text)
                used_urls.add(url)

    # Priority 1: About / company identity
    for url, text in relevant.items():
        if url not in used_urls and any(x in url.lower() for x in ["/about", "/azienda", "/chi-siamo", "/company",
                                            "/impresa", "/storia", "/history"]):
            ordered_texts.append(text)
            used_urls.add(url)

    # Priority 2: Products / catalog
    for url, text in relevant.items():
        if url not in used_urls and any(x in url.lower() for x in ["/product", "/prodott", "/catalog",
                                            "/gamma", "/serie", "/linea", "/portfolio"]):
            ordered_texts.append(text)
            used_urls.add(url)

    # Priority 3: Homepage + anything else not already added
    for url, text in relevant.items():
        if url not in used_urls:
            ordered_texts.append(text)
            used_urls.add(url)
            
    # Fix 6: fallback if filtered list was empty
    if not ordered_texts and scraped_pages:
        for url, text in scraped_pages.items():
            if url not in used_urls:
                ordered_texts.append(text)
                used_urls.add(url)

    concatenated = "\n\n".join(ordered_texts)
    if len(concatenated) > 15000:
        return concatenated[:15000]
    return concatenated

def _parse_gemini_response(raw: str) -> dict:
    """Robustly parse Gemini JSON — handles markdown fences and partial output."""
    text = raw.strip()
    # Strip markdown code fences if present
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()
    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Try extracting first {...} block
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass
    raise ValueError(f"Cannot parse Gemini response: {raw[:200]}")


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=10, max=65),
    reraise=True
)
def _call_gemini(api_key: str, prompt: str) -> dict[str, Any]:
    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.2,
            max_output_tokens=4096,
            response_mime_type="application/json",
            response_schema={
                "type": "OBJECT",
                "properties": {
                    "is_manufacturer": {"type": "BOOLEAN"},
                    "is_agri_manufacturer": {"type": "BOOLEAN"},
                    "products_detected": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                        "maxItems": 10
                    },
                    "confidence": {"type": "NUMBER"},
                    "evidence": {"type": "STRING"}
                },
                "required": ["is_manufacturer", "is_agri_manufacturer", "products_detected", "confidence", "evidence"]
            }
        )
    )
    return _parse_gemini_response(response.text)

def analyze_company(company_name: str, website_url: str, scraped_pages: dict[str, str]) -> dict[str, Any]:
    # 1. Domain Blocklist
    domain_low = website_url.lower()
    directory_domains = ["europages", "alibaba", "indiamart", "kompass", "manta", "yellowpages", "paginegialle", "directindustry"]
    if any(d in domain_low for d in directory_domains):
        logger.info(f"Skipping directory domain: {website_url}")
        return {
            "is_manufacturer": False,
            "is_agri_manufacturer": False,
            "products_detected_raw": [],
            "confidence": 0.0,
            "ai_input_text": "",
            "evidence": "Skipped directory domain",
            "ai_used": False,
            "scrape_status": "skipped_directory"
        }

    ai_input_text = build_ai_input_text(scraped_pages)
    
    # FIX 3: AI input text fallback
    if len(ai_input_text) < 50 and scraped_pages:
        ai_input_text = "\n\n".join(scraped_pages.values())[:15000]
        
    language = text_heuristics_language(ai_input_text)
    
    result = {
        "is_manufacturer": False,
        "is_agri_manufacturer": False,
        "products_detected_raw": [],
        "confidence": 0.0,
        "ai_input_text": ai_input_text,
        "evidence": "",
        "ai_used": False,
    }
    
    # FIX 1 & 2: Remove hard length filter. Fail ONLY if scraped_pages empty or completely unusable.
    if not scraped_pages:
        logger.warning(f"No scraped pages for {company_name}, skipping Gemini analysis.")
        result["scrape_status"] = "failed"
        return result
        
    if not ai_input_text.strip():
        logger.warning(f"All pages completely empty/unusable for {company_name}.")
        result["scrape_status"] = "failed"
        return result

    # FIX 4: Over-aggressive 404/Error detection
    text_low = ai_input_text.lower()
    error_signals = [
        "404", "not found", "page not found",
        "access denied", "forbidden",
        "captcha", "cloudflare",
        "enable javascript",
        "error occurred"
    ]
    error_count = sum(1 for signal in error_signals if signal in text_low)
    if error_count >= 2:
        logger.warning(f"Multiple error signals ({error_count}) found in text for {company_name}.")
        result["scrape_status"] = "failed"
        return result

    result["scrape_status"] = "success"
    
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        logger.warning("No GEMINI_API_KEY found. Skipping AI intelligence layer.")
        return result
        
    prompt = f"""
You are an industrial classification system for agricultural machinery companies.
Your task is to analyze company website text and extract structured data.

STRICT RULES:
- A company is a MANUFACTURER only if it DESIGNS, PRODUCES, makes, or BUILDS machinery.
- In addition to determining if the company is a manufacturer, also determine if they manufacture agricultural machinery or equipment specifically.
- If the company manufactures machinery unrelated to agriculture (e.g. automotive, construction, general industrial), set is_agri_manufacturer = false.
- If the company clearly produces agricultural machinery, implements, or equipment, set is_agri_manufacturer = true.
- If unclear, reduce confidence.
- DO NOT enforce strict verb matching. Let semantics dictate.
- HARD REJECT (is_manufacturer = false) if they are explicitly a "dealer", "distributor", "reseller", "rivenditore", or "distributore".
- Positive signals (soft): produce, manufacture, build, design, make.
- Use ONLY the provided text. Do not hallucinate.
- Return AT MOST 10 representative product names (broad categories preferred over specific model names).
- Keep the evidence field to 1-2 sentences max.

USER INPUT:
Company: {company_name}
Website: {website_url}

Note: The website content is primarily in {language}. You MUST translate all extracted product names to ENGLISH. Do not return Italian product names.

Content:
{ai_input_text}
"""
    
    try:
        parsed = _call_gemini(api_key, prompt)
        
        # Only retry if confirmed agri-manufacturer but products list is empty
        # (avoids double calls on rejections and non-agri manufacturers)
        if (parsed.get("is_agri_manufacturer") and 
                not parsed.get("products_detected")):
            logger.warning("Agri manufacturer with no products detected — retrying once.")
            parsed = _call_gemini(api_key, prompt)
            
        final_confidence = parsed.get("confidence", 0.0)
        final_confidence = min(max(final_confidence, 0.0), 1.0)
            
        result.update({
            "is_manufacturer": parsed.get("is_manufacturer", False),
            "is_agri_manufacturer": parsed.get("is_agri_manufacturer", False),
            "products_detected_raw": parsed.get("products_detected", []),
            "confidence": final_confidence,
            "evidence": parsed.get("evidence", ""),
            "ai_used": True,
        })
    except Exception as e:
        logger.error("Gemini AI classification totally failed: %s", e)
    
    return result
