"""
download_and_stage.py — Download capped subsets of CIC-IDS2017 and UNSW-NB15 from Hugging Face.
Budget: flow ~350 MB, packet ~180-200 MB, unsw <= 50 MB, total <= 600 MB.
Stages files to /data/staging/ and writes artifacts/staging_manifest.json.
"""

import os
import sys
import json
import shutil
from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd
from huggingface_hub import hf_hub_download

STAGING_ROOT = Path("/data/staging")
ARTIFACTS_ROOT = Path("/opt/spark/work-dir/artifacts")
HF_CACHE = STAGING_ROOT / "hf_cache"

STAGING_ROOT.mkdir(parents=True, exist_ok=True)
HF_CACHE.mkdir(parents=True, exist_ok=True)
os.environ["HF_HOME"] = str(HF_CACHE)

MAX_TOTAL_BYTES = 600 * 1024 * 1024  # 600 MB hard cap

def mb(b: int) -> float:
    return round(b / (1024 * 1024), 2)

print("=" * 70)
print("BDA Phase 1 — Download and Staging (Max 600 MB Working Set)")
print("=" * 70)

manifest_file = ARTIFACTS_ROOT / "dataset_manifest.json"
if not manifest_file.exists():
    sys.exit("Error: dataset_manifest.json not found. Run discover.py first.")

with open(manifest_file, "r") as f:
    manifest = json.load(f)

staged_records = {}
total_downloaded = 0

# ── 1. CIC-IDS2017 Flow-Level (~350 MB) ──────────────────────────────────────
print("\n[1/3] Downloading CIC-IDS2017 Flow-level data...")
flow_repo = manifest["cicids2017_flow"]["repo_id"]
flow_file = manifest["cicids2017_flow"]["selected_file"]
flow_stage_dir = STAGING_ROOT / "cicids2017_flow"
flow_stage_dir.mkdir(parents=True, exist_ok=True)

flow_local = hf_hub_download(
    repo_id=flow_repo,
    filename=flow_file,
    repo_type="dataset",
    cache_dir=str(HF_CACHE),
    local_dir=str(flow_stage_dir)
)
flow_size = Path(flow_local).stat().st_size
total_downloaded += flow_size
print(f"  Downloaded: {Path(flow_local).name} ({mb(flow_size)} MB) [Total: {mb(total_downloaded)} MB]")
staged_records["cicids2017_flow"] = [{
    "filename": Path(flow_local).name,
    "path": str(flow_local),
    "size_bytes": flow_size,
    "size_mb": mb(flow_size)
}]

# ── 2. CIC-IDS2017 Packet-Level (Payload Bytes Shard trimmed to ~180 MB) ────
print("\n[2/3] Downloading & slicing CIC-IDS2017 Packet-level data...")
packet_repo = manifest["cicids2017_packet"]["repo_id"]
packet_file = manifest["cicids2017_packet"]["selected_file"]
packet_stage_dir = STAGING_ROOT / "cicids2017_packet"
packet_stage_dir.mkdir(parents=True, exist_ok=True)

packet_local = hf_hub_download(
    repo_id=packet_repo,
    filename=packet_file,
    repo_type="dataset",
    cache_dir=str(HF_CACHE),
    local_dir=str(packet_stage_dir)
)
raw_packet_size = Path(packet_local).stat().st_size
print(f"  Raw packet shard downloaded: {mb(raw_packet_size)} MB")

# Trim to ~180 MB to ensure we strictly honour the <= 200 MB packet budget and <= 600 MB total
trimmed_packet_path = packet_stage_dir / "cicids2017_packet_slice.parquet"
print(f"  Trimming to ~180 MB working slice...")
table = pq.read_table(packet_local)
# Keep ~65% of rows (approx 175 MB)
target_rows = int(table.num_rows * 0.65)
sliced_table = table.slice(0, target_rows)
pq.write_table(sliced_table, trimmed_packet_path, compression="snappy")

# Remove untrimmed raw download to save space
if Path(packet_local) != trimmed_packet_path and Path(packet_local).exists():
    Path(packet_local).unlink()

packet_size = trimmed_packet_path.stat().st_size
total_downloaded += packet_size
print(f"  Final packet slice: {trimmed_packet_path.name} ({mb(packet_size)} MB) [Total: {mb(total_downloaded)} MB]")
staged_records["cicids2017_packet"] = [{
    "filename": trimmed_packet_path.name,
    "path": str(trimmed_packet_path),
    "size_bytes": packet_size,
    "size_mb": mb(packet_size)
}]

# ── 3. UNSW-NB15 Validation Slice (<= 50 MB) ────────────────────────────────
print("\n[3/3] Downloading UNSW-NB15 validation slice...")
unsw_repo = manifest["unswnb15"]["repo_id"]
unsw_stage_dir = STAGING_ROOT / "unswnb15"
unsw_stage_dir.mkdir(parents=True, exist_ok=True)

unsw_files = ["test.parquet", "train.parquet"]
staged_records["unswnb15"] = []
for u_file in unsw_files:
    u_local = hf_hub_download(
        repo_id=unsw_repo,
        filename=u_file,
        repo_type="dataset",
        cache_dir=str(HF_CACHE),
        local_dir=str(unsw_stage_dir)
    )
    u_size = Path(u_local).stat().st_size
    total_downloaded += u_size
    print(f"  Downloaded: {Path(u_local).name} ({mb(u_size)} MB) [Total: {mb(total_downloaded)} MB]")
    staged_records["unswnb15"].append({
        "filename": Path(u_local).name,
        "path": str(u_local),
        "size_bytes": u_size,
        "size_mb": mb(u_size)
    })

print("\n" + "=" * 70)
print(f"Total Staged Working Set Size: {mb(total_downloaded)} MB (Budget: 600 MB)")
if total_downloaded > MAX_TOTAL_BYTES:
    sys.exit(f"ABORT: Downloaded {mb(total_downloaded)} MB exceeds hard cap of 600 MB!")

print("SUCCESS: Dataset size is strictly within limits.")

staging_manifest = {
    "total_bytes": total_downloaded,
    "total_mb": mb(total_downloaded),
    "datasets": staged_records
}

out_staging = ARTIFACTS_ROOT / "staging_manifest.json"
out_staging.write_text(json.dumps(staging_manifest, indent=2))
print(f"Staging manifest written to {out_staging}")
print("=" * 70)
