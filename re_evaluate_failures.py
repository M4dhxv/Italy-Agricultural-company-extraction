import json
import os
import sys
from dotenv import load_dotenv

# Ensure local imports work
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.intelligence_layer import analyze_company
from src.product_extractor import extract_products

def run():
    load_dotenv()
    print("Loading raw_master.json...")
    with open('raw_master.json', 'r') as f:
        data = json.load(f)
    
    recent = data[-150:]
    # Targets: Any company in the last 150 that was either marked scrape failed OR rejected OR missing products, BUT has scraped_pages
    targets = []
    for x in recent:
        has_pages = 'scraped_pages' in x and isinstance(x['scraped_pages'], dict) and len(x['scraped_pages']) > 0
        if not has_pages:
            continue
            
        is_failed = x.get('scrape_status') == 'failed'
        is_rejected = bool(x.get('rejected_reason'))
        not_agri = not x.get('is_agri_manufacturer')
        
        if is_failed or is_rejected or not_agri:
            targets.append(x)
    
    print(f"Found {len(targets)} targets marked as scrape failed or mislabelled out of the last 150.")
    print("Re-evaluating directly through Gemini (Bypassing Scraper)...")
    
    results = []
    
    for idx, rec in enumerate(targets):
        name = rec.get('name', rec.get('company_name', 'Unknown'))
        website = rec.get('website', '')
        pages = rec.get('scraped_pages', {})
        print(f"[{idx+1}/{len(targets)}] Re-evaluating: {name}")
        
        # Run new AI logic directly on the preserved scraped content
        ai_res = analyze_company(name, website, pages)
        
        # Update the record with the new Intelligence Layer results!
        new_rec = dict(rec)
        new_rec['scrape_status'] = ai_res.get('scrape_status', 'failed')
        new_rec['is_manufacturer'] = ai_res.get('is_manufacturer', False)
        new_rec['is_agri_manufacturer'] = ai_res.get('is_agri_manufacturer', False)
        new_rec['products_detected_raw'] = ai_res.get('products_detected_raw', [])
        new_rec['confidence'] = ai_res.get('confidence', 0.0)
        new_rec['evidence'] = ai_res.get('evidence', '')
        new_rec['ai_input_text'] = ai_res.get('ai_input_text', '')
        
        # Optional: drop the old rejected reason
        if 'rejected_reason' in new_rec:
            del new_rec['rejected_reason']
            
        # Re-apply product extractor if it passes!
        base_valid = new_rec['scrape_status'] != 'failed' and new_rec['is_manufacturer'] and new_rec['is_agri_manufacturer']
        
        if base_valid:
            norm = extract_products(new_rec.get('products_detected_raw', []))
            new_rec['products_mapped'] = norm.get('products_mapped', [])
            new_rec['category_counts'] = norm.get('category_counts', {})
            new_rec['primary_category'] = norm.get('primary_category', '')
            new_rec['secondary_categories'] = norm.get('secondary_categories', [])
            new_rec['weak_product_signal'] = norm.get('weak_product_signal', False)
            new_rec['unmapped_products_count'] = norm.get('unmapped_products_count', 0)
        else:
            if new_rec['scrape_status'] == 'failed':
                new_rec['rejected_reason'] = "scrape failed"
            else:
                new_rec['rejected_reason'] = "not agri manufacturer"
                
        results.append(new_rec)
        
    out_path = '/Users/madhavsharma/Desktop/fixed_scrape_failures.json'
    with open(out_path, 'w') as f:
        json.dump(results, f, indent=4)
        
    print("-" * 50)
    print(f"DONE! Saved {len(results)} newly evaluated records to:")
    print(f"-> {out_path}")
    
    # Just a small summary of how many actually passed now:
    passed = [x for x in results if 'rejected_reason' not in x]
    print(f"Total magically recovered: {len(passed)} / {len(results)}")

if __name__ == '__main__':
    run()
