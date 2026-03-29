"""
product_extractor.py — Validates AI product detections against canonical taxonomy.
"""
from typing import Any

import collections
from src.config import KEYWORD_MAP, CATEGORY_MAP

def extract_products(ai_raw_products: list[str]) -> dict[str, Any]:
    """
    Applies simple partial matching using KEYWORD_MAP.
    Matches if keyword is contained in product string (case-insensitive).
    If no match is found, keeps the original product.
    Does not drop any tools or enforce strict matching.
    Includes lightweight aggregation by category to determine primary and secondary categories.
    """
    mapped_products = set()
    
    for raw in ai_raw_products:
        raw_low = raw.lower()
        matched_any = False
        
        for kw, canonical in KEYWORD_MAP.items():
            if kw.lower() in raw_low:
                mapped_products.add(canonical)
                matched_any = True
                
        # If no keywords matched, just keep the raw product
        if not matched_any:
            mapped_products.add(raw)
            
    mapped_products_list = sorted(list(mapped_products))
    unmapped_products_count = len(ai_raw_products) - len(mapped_products_list)
    
    if len(mapped_products_list) == 0:
        return {
            "products_detected_raw": ai_raw_products,
            "products_mapped": [],
            "category_counts": {},
            "primary_category": "",
            "secondary_categories": [],
            "weak_product_signal": True,
            "unmapped_products_count": unmapped_products_count
        }
    
    # ------------------------------------------------------------------ #
    # CATEGORY AGGREGATION
    # ------------------------------------------------------------------ #
    category_counts = collections.defaultdict(int)
    for prod in mapped_products_list:
        cat = CATEGORY_MAP.get(prod)
        if cat:
            category_counts[cat] += 1
            
    category_counts_dict = dict(category_counts)
    
    primary_category = ""
    if category_counts_dict:
        primary_category = max(category_counts_dict.items(), key=lambda x: x[1])[0]
        
    secondary_categories = sorted([
        cat for cat, count in category_counts_dict.items()
        if count >= 2 and cat != primary_category
    ])
    
    weak_product_signal = len(mapped_products_list) < 2
                    
    return {
        "products_detected_raw": ai_raw_products,
        "products_mapped": mapped_products_list,
        "category_counts": category_counts_dict,
        "primary_category": primary_category,
        "secondary_categories": secondary_categories,
        "weak_product_signal": weak_product_signal,
        "unmapped_products_count": unmapped_products_count,
    }
