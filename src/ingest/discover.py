"""
discover.py — Find CIC-IDS2017 (flow + packet) and UNSW-NB15 repos on Hugging Face.
Writes artifacts/dataset_manifest.json with repo ids, file names, and sizes.
Run inside bda-spark container:
  docker run --rm -v "${PWD}/artifacts:/opt/spark/work-dir/artifacts" bda-spark:latest python3 /opt/spark/work-dir/src/ingest/discover.py
"""

import json
import os
import sys
from pathlib import Path

from huggingface_hub import HfApi

ARTIFACTS = Path("/opt/spark/work-dir/artifacts")
ARTIFACTS.mkdir(parents=True, exist_ok=True)

api = HfApi()

# Candidates for discovery
SEARCH_CONFIG = {
    "cicids2017_flow": {
        "repo_id": "rdpahalavan/CIC-IDS2017",
        "primary_file": "Network-Flows/CICIDS_Flow.parquet",
        "description": "CIC-IDS2017 full flow-level dataset (all 5 days, all attack classes)"
    },
    "cicids2017_packet": {
        "repo_id": "rdpahalavan/CIC-IDS2017",
        "primary_file": "Payload-Bytes/Payload_Bytes_File_18.parquet",
        "description": "CIC-IDS2017 packet/payload-bytes level capture shard with payload hex"
    },
    "unswnb15": {
        "repo_id": "wwydmanski/UNSW-NB15",
        "primary_file": "test.parquet",
        "description": "UNSW-NB15 validation flow dataset slice"
    }
}

manifest = {}

print("=" * 70)
print("BDA Phase 1 — Dataset Discovery on Hugging Face")
print("=" * 70)

for cat, cfg in SEARCH_CONFIG.items():
    repo_id = cfg["repo_id"]
    print(f"\n--- Checking {cat} in repo '{repo_id}' ---")
    try:
        files = []
        tree = api.list_repo_tree(repo_id=repo_id, repo_type="dataset", recursive=True)
        for item in tree:
            if hasattr(item, "size") and item.size is not None:
                files.append({
                    "name": item.path,
                    "size_bytes": item.size,
                    "size_mb": round(item.size / (1024 * 1024), 2)
                })
        
        # Locate chosen file
        chosen = next((f for f in files if f["name"] == cfg["primary_file"]), None)
        if chosen:
            print(f"  Target file FOUND: {chosen['name']} ({chosen['size_mb']} MB)")
        else:
            print(f"  Target file not found, available files: {[f['name'] for f in files[:5]]}")

        manifest[cat] = {
            "repo_id": repo_id,
            "repo_url": f"https://huggingface.co/datasets/{repo_id}",
            "description": cfg["description"],
            "selected_file": cfg["primary_file"],
            "selected_size_mb": chosen["size_mb"] if chosen else None,
            "total_files": len(files),
            "files": files[:25]
        }
    except Exception as e:
        print(f"  Error discovering {repo_id}: {e}")
        manifest[cat] = {"repo_id": repo_id, "error": str(e)}

out_path = ARTIFACTS / "dataset_manifest.json"
out_path.write_text(json.dumps(manifest, indent=2))
print(f"\nManifest successfully written to {out_path}")

print("\nSummary of Selected Working Set:")
for cat, data in manifest.items():
    print(f"  * {cat:18s} -> {data.get('repo_id')} : {data.get('selected_file')} ({data.get('selected_size_mb')} MB)")
print("=" * 70)

