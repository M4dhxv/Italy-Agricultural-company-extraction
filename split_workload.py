"""
split_workload.py — Splits discovery_output.json into N shards for parallel processing.

Excludes companies already in raw_master.json.
Each shard gets its own discovery file, state file, and output file.

Usage:
    python split_workload.py --workers 5
"""
import argparse
import json
import os

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--discovery", default="discovery_output.json")
    parser.add_argument("--master", default="raw_master.json")
    parser.add_argument("--skip-no-website", action="store_true", default=True)
    args = parser.parse_args()

    # Load already-processed names from raw_master (source of truth)
    processed_names: set[str] = set()
    if os.path.exists(args.master):
        with open(args.master) as f:
            master = json.load(f)
        processed_names = {r.get("name", "").strip() for r in master}
    print(f"Already in raw_master: {len(processed_names)} companies")

    # Load ALL companies from discovery
    with open(args.discovery) as f:
        all_companies = json.load(f)

    # Filter: not yet in raw_master, has website (if flag set)
    remaining = []
    for c in all_companies:
        name = c.get("name", "").strip()
        if not name:
            continue
        if name in processed_names:
            continue
        if args.skip_no_website and not c.get("website", "").strip():
            continue
        remaining.append(c)

    print(f"Remaining to process: {len(remaining)} companies")
    print(f"Splitting into {args.workers} shards...")

    # Split into N even shards
    chunk_size = len(remaining) // args.workers
    os.makedirs("shards", exist_ok=True)

    for i in range(args.workers):
        start = i * chunk_size
        end = start + chunk_size if i < args.workers - 1 else len(remaining)
        shard = remaining[start:end]

        shard_file = f"shards/discovery_shard_{i}.json"
        with open(shard_file, "w") as f:
            json.dump(shard, f, ensure_ascii=False, indent=2)

        print(f"  Shard {i}: {len(shard)} companies → {shard_file}")

    # Write a manifest
    manifest = {
        "workers": args.workers,
        "total_remaining": len(remaining),
        "already_processed": len(processed_names),
        "shards": [f"shards/discovery_shard_{i}.json" for i in range(args.workers)],
        "outputs": [f"shards/raw_shard_{i}.json" for i in range(args.workers)],
        "states": [f"shards/state_shard_{i}.json" for i in range(args.workers)],
    }
    with open("shards/manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\nManifest saved to shards/manifest.json")
    print("Run: python run_parallel.py")

if __name__ == "__main__":
    main()
