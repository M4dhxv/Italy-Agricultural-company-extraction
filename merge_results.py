"""
merge_results.py — Merges all worker shards back into raw_master.json and rebuilds CSV.

Usage:
    python merge_results.py
"""
import json
import os
import sys

def build_csv():
    # Helper to call the existing CSV builder
    from src.output_builder import build_csv as original_build_csv
    original_build_csv()


def main():
    try:
        with open("shards/manifest.json") as f:
            manifest = json.load(f)
    except FileNotFoundError:
        print("Error: shards/manifest.json not found.")
        sys.exit(1)

    master_file = "raw_master.json"
    master = []
    if os.path.exists(master_file):
        with open(master_file) as f:
            try:
                master = json.load(f)
            except Exception:
                master = []
                
    existing_names = set(r.get("name", "").strip() for r in master)
    original_len = len(master)
    print(f"Loaded {original_len} existing records from {master_file}")

    added = 0
    for shard_file in manifest["outputs"]:
        if not os.path.exists(shard_file):
            print(f"Warning: {shard_file} not found.")
            continue
            
        with open(shard_file) as f:
            try:
                records = json.load(f)
            except Exception as e:
                print(f"Error reading {shard_file}: {e}")
                continue
                
        for r in records:
            name = r.get("name", "").strip()
            if name and name not in existing_names:
                master.append(r)
                existing_names.add(name)
                added += 1

    print(f"Merged {added} new records from shards.")
    print(f"Total records now: {len(master)}")
    
    # Save back
    tmp = master_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(master, f, ensure_ascii=False, indent=2)
    os.replace(tmp, master_file)
    print("Saved to raw_master.json.")
    
    print("\nRebuilding CSV...")
    build_csv()
    
if __name__ == "__main__":
    main()
