"""
run_parallel.py — Orchestrates the execution of shard workers.

Runs N independent python processes for run_worker.py and streams their output.

Usage:
    python run_parallel.py
"""
import json
import subprocess
import sys
import time

def main():
    try:
        with open("shards/manifest.json") as f:
            manifest = json.load(f)
    except FileNotFoundError:
        print("Error: shards/manifest.json not found.")
        print("Run 'python split_workload.py' first.")
        sys.exit(1)

    workers = manifest["workers"]
    print(f"Starting {workers} parallel workers...")
    
    processes = []
    for i in range(workers):
        cmd = [sys.executable, "run_worker.py", "--shard-id", str(i)]
        # We start them in the background. Their stdout is piped to their log files
        # natively by the worker script itself, but we can let them share this terminal's stdout too
        # using subprocess.Popen. To avoid total chaos, let's keep it simple.
        p = subprocess.Popen(cmd)
        processes.append(p)
        time.sleep(1) # Stagger start slightly
        
    print("All workers started! Waiting for them to finish...")
    try:
        for p in processes:
            p.wait()
    except KeyboardInterrupt:
        print("\nSending kill signal to all workers...")
        for p in processes:
            p.terminate()
    print("\nAll workers finished successfully!")
    print("Auto-merging shards back into master and rebuilding CSV...")
    try:
        subprocess.run([sys.executable, "merge_results.py"], check=True)
    except subprocess.CalledProcessError:
        print("Error during merge. You can try merging manually with 'python merge_results.py'")
        sys.exit(1)

if __name__ == "__main__":
    main()
