"""Fail-closed disk guard for planned host-side ingestion and storage work."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

MIN_FREE_BYTES = 12 * 1024**3
ROOT = Path(os.environ.get("BDA_ROOT", "D:/bda"))


def run(args: list[str]) -> str | None:
    try:
        result = subprocess.run(args, check=True, capture_output=True, text=True, timeout=60)
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def main() -> int:
    usage = shutil.disk_usage("D:/" if os.name == "nt" else "/")
    free_gb = usage.free / 1024**3
    print(f"D: free: {free_gb:.2f} GiB (required floor: 12.00 GiB)")

    hdfs = run(["docker", "exec", "namenode", "hdfs", "dfs", "-du", "-s", "/bda"])
    if hdfs:
        fields = hdfs.split()
        print(f"HDFS /bda: logical={fields[0]} bytes, physical={fields[1]} bytes")
    else:
        print("HDFS /bda: unavailable (not measured)")

    mongo = run([
        "docker", "exec", "mongodb", "mongosh", "--quiet", "--eval",
        "JSON.stringify(db.adminCommand({listDatabases:1}).databases.map(x=>({name:x.name,sizeOnDisk:x.sizeOnDisk})))",
    ])
    if mongo:
        try:
            print("MongoDB databases (sizeOnDisk bytes): " + json.dumps(json.loads(mongo)))
        except json.JSONDecodeError:
            print("MongoDB databases (unparsed): " + mongo)
    else:
        print("MongoDB: unavailable (not measured)")

    volume_report = run(["docker", "system", "df", "-v"])
    mongo_line = next((line.strip() for line in (volume_report or "").splitlines()
                       if line.strip().startswith("bda_mongo_data ")), None)
    print("MongoDB volume (Docker-reported physical): " + (mongo_line or "unavailable (not measured)"))

    stage = ROOT / "staging"
    stage_bytes = sum(p.stat().st_size for p in stage.rglob("*") if p.is_file()) if stage.exists() else 0
    print(f"D: staging: {stage_bytes} bytes")
    if free_gb < 12:
        print("ABORT: D: is below the 12 GiB free-space floor.", file=sys.stderr)
        return 1
    if not hdfs or not mongo:
        print("ABORT: Storage state could not be measured; refusing a heavy operation.", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
