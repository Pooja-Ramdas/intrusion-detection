"""Create a metadata-only, capped CIC-IDS2017/UNSW-NB15 ingest proposal."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version

from huggingface_hub import HfApi
from nids_datasets import DatasetInfo

CAPS = {
    "cicids2017_packet_bytes": 40 * 1_000_000_000,
    "cicids2017_flow_bytes": 3 * 1_000_000_000,
    "unswnb15_bytes": 6 * 1_000_000_000,
    "staging_bytes": 4 * 1_000_000_000,
}
REPOS = {
    "cicids2017": "rdpahalavan/CIC-IDS2017",
    "unswnb15": "wwydmanski/UNSW-NB15",
}
PACKET_IDS = [1, 2, 3, 6, 9, 10, 11, 12, 13, 14, 15, 16, 17]
PACKET_SUBSET = "Packet-Bytes"
TARGET_SOURCE_BYTES = 30 * 1_000_000_000
DAY_BY_FILE = {
    1: "Monday (2017-07-03)", 2: "Monday (2017-07-03)",
    3: "Tuesday (2017-07-04)", 4: "Tuesday (2017-07-04)",
    5: "Tuesday (2017-07-04)", 6: "Tuesday (2017-07-04)",
    7: "Tuesday (2017-07-04)", 8: "Tuesday (2017-07-04)",
    9: "Wednesday (2017-07-05)", 10: "Wednesday (2017-07-05)",
    11: "Wednesday (2017-07-05)",
    12: "Thursday (2017-07-06)", 13: "Thursday (2017-07-06)",
    14: "Thursday (2017-07-06)",
    15: "Friday (2017-07-07)", 16: "Friday (2017-07-07)",
    17: "Friday (2017-07-07)", 18: "Friday (2017-07-07)",
}
RARE = ["Infiltration", "Heartbleed", "Web Attack – SQL Injection", "Web Attack – XSS", "Web Attack – Brute Force", "SSH-Patator"]


def item_sha(item) -> str | None:
    lfs = getattr(item, "lfs", None)
    return getattr(lfs, "sha256", None) if lfs else None


def main() -> None:
    api = HfApi()
    infos = {key: api.dataset_info(repo_id=repo) for key, repo in REPOS.items()}
    trees = {
        key: [x for x in api.list_repo_tree(repo_id=repo, repo_type="dataset", recursive=True)
              if getattr(x, "size", None) is not None]
        for key, repo in REPOS.items()
    }
    parquet_inventories = {
        key: [{"name": x.path, "size_bytes": x.size, "sha256": item_sha(x)}
              for x in tree if x.path.lower().endswith(".parquet")]
        for key, tree in trees.items()
    }

    cic_repo = REPOS["cicids2017"]
    flow_path = "Network-Flows/CICIDS_Flow.parquet"
    packet_paths = [f"{PACKET_SUBSET}/Packet_Bytes_File_{i}.parquet" for i in PACKET_IDS]
    unsw_repo = REPOS["unswnb15"]
    unsw_paths = ["train.parquet", "test.parquet"]
    selected_info = {
        (cic_repo, x.path): x for x in api.get_paths_info(
            repo_id=cic_repo, paths=[flow_path, *packet_paths], repo_type="dataset"
        )
    }
    selected_info.update({
        (unsw_repo, x.path): x for x in api.get_paths_info(
            repo_id=unsw_repo, paths=unsw_paths, repo_type="dataset"
        )
    })
    cic_counts = DatasetInfo("CIC-IDS2017")

    packet_files = []
    for file_id, path in zip(PACKET_IDS, packet_paths):
        info = selected_info[(cic_repo, path)]
        row = cic_counts.loc[str(file_id)]
        labels = {str(label): int(count) for label, count in row.items()
                  if label != "total" and int(count) > 0}
        packet_files.append({
            "repo_id": cic_repo,
            "path": path,
            "size_bytes": int(info.size),
            "sha256": item_sha(info),
            "file_id": file_id,
            "day_stratum": DAY_BY_FILE[file_id],
            "published_label_counts": labels,
            "published_row_count": int(row["total"]),
            "selection": "whole_file",
            "destination": f"/bda/raw/cicids2017_packet/{path.rsplit('/', 1)[-1]}",
            "action": "download_after_approval",
        })

    flow_info = selected_info[(cic_repo, flow_path)]
    flow_file = {
        "repo_id": cic_repo, "path": flow_path,
        "size_bytes": int(flow_info.size), "sha256": item_sha(flow_info),
        "selection": "entire_flow_file", "destination": "/bda/raw/cicids2017_flow/CICIDS_Flow.parquet",
        "action": "verify_existing_then_skip_or_download_after_approval",
    }
    unsw_files = []
    for path in unsw_paths:
        info = selected_info[(unsw_repo, path)]
        unsw_files.append({
            "repo_id": unsw_repo, "path": path, "size_bytes": int(info.size),
            "sha256": item_sha(info), "selection": "entire_file",
            "destination": f"/bda/raw/unswnb15/{path}",
            "action": "verify_existing_then_skip_or_download_after_approval",
        })

    selected_packet_bytes = sum(x["size_bytes"] for x in packet_files)
    flow_bytes = flow_file["size_bytes"]
    unsw_bytes = sum(x["size_bytes"] for x in unsw_files)
    rare_coverage = {
        label: [x["path"] for x in packet_files if x["published_label_counts"].get(label, 0) > 0]
        for label in RARE
    }
    selected_labels = sorted({
        label for item in packet_files for label, count in item["published_label_counts"].items()
        if count > 0 and label != "BENIGN"
    })
    all_source_labels = sorted({
        str(label) for label in cic_counts.columns if label != "total" and int(cic_counts[label].sum()) > 0 and label != "BENIGN"
    })
    plan = {
        "status": "proposed_pending_explicit_approval",
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "metadata_only": True,
        "source": "HfApi.list_repo_tree/get_paths_info; nids_datasets.DatasetInfo published per-file counts",
        "repos": {
            key: {"repo_id": repo, "revision": infos[key].sha,
                  "parquet_inventory": parquet_inventories[key]}
            for key, repo in REPOS.items()
        },
        "caps_bytes": CAPS,
        "plan": {
            "cicids2017_packet": {
                "subset": "Packet-Bytes (original CIC-IDS2017 packet-level source files)",
                "cap_bytes": CAPS["cicids2017_packet_bytes"],
                "selected_bytes": selected_packet_bytes,
                "existing_preserved_file": {
                    "hdfs_path": "/bda/raw/cicids2017_packet/cicids2017_packet_slice.parquet",
                    "size_bytes": 169017675,
                    "source_provenance": "Legacy working slice was created from Packet_Bytes_File_18.parquet; preserve and exclude from unique-source-byte target.",
                },
                "files": packet_files,
                "day_coverage": sorted({x["day_stratum"] for x in packet_files}),
                "attack_class_coverage": selected_labels,
                "all_published_attack_classes_covered": selected_labels == all_source_labels,
                "attack_class_coverage_gaps": sorted(set(all_source_labels) - set(selected_labels)),
                "rare_class_file_coverage": rare_coverage,
                "rare_class_policy": "Keep complete whole source files for required rare classes; no row slicing or synthesis.",
                "excluded_file18": {
                    "path": "Packet-Bytes/Packet_Bytes_File_18.parquet",
                    "size_bytes": int(next(x.size for x in trees["cicids2017"] if x.path == "Packet-Bytes/Packet_Bytes_File_18.parquet")),
                    "reason": "Existing working slice was derived from this source; avoid known overlap until checksum/revision-level deduplication is approved.",
                },
            },
            "cicids2017_flow": {
                "cap_bytes": CAPS["cicids2017_flow_bytes"],
                "selected_bytes": flow_bytes,
                "files": [flow_file],
                "selection": "all available flow-level records; one source parquet",
            },
            "unswnb15": {
                "cap_bytes": CAPS["unswnb15_bytes"],
                "selected_bytes": unsw_bytes,
                "files": unsw_files,
                "selection": "all available validation train/test files",
            },
            "staging": {
                "max_bytes": CAPS["staging_bytes"],
                "max_selected_file_bytes": max(x["size_bytes"] for x in packet_files),
                "chunked_staging_bytes": 512 * 1_000_000,
                "transfer_order": "one file at a time; file 1 uses resumable byte-range chunks staged on D: (512 MB each, total staging <=4 GB); verify full-file SHA-256 and HDFS byte count",
            },
            "totals": {
                "selected_source_bytes": selected_packet_bytes + flow_bytes + unsw_bytes,
                "selected_source_decimal_gb": (selected_packet_bytes + flow_bytes + unsw_bytes) / 1_000_000_000,
                "new_packet_download_bytes": selected_packet_bytes,
                "existing_flow_and_unsw_bytes": flow_bytes + unsw_bytes,
                "existing_packet_slice_bytes_excluded_from_target": 169017675,
                "target_bytes": TARGET_SOURCE_BYTES,
                "shortfall_bytes": max(0, TARGET_SOURCE_BYTES - selected_packet_bytes - flow_bytes - unsw_bytes),
                "target_status": "met" if selected_packet_bytes + flow_bytes + unsw_bytes >= TARGET_SOURCE_BYTES else "blocked until a verified non-overlapping source file covers any shortfall",
                "existing_hdfs_data_policy": "Never overwrite or delete existing files; verify checksum/size then skip or halt on conflict.",
            },
        },
    }
    if selected_packet_bytes > CAPS["cicids2017_packet_bytes"]:
        raise SystemExit("Proposed packet files plus preserved working set exceed the packet cap")
    if any(not files for files in rare_coverage.values()):
        raise SystemExit("A required rare class is absent from the selected packet shards")
    if flow_bytes > CAPS["cicids2017_flow_bytes"] or unsw_bytes > CAPS["unswnb15_bytes"]:
        raise SystemExit("A dataset selection exceeds its configured cap")
    if any(x["sha256"] is None for x in packet_files + [flow_file, *unsw_files]):
        raise SystemExit("Hugging Face did not provide an LFS SHA-256 for every planned file")
    try:
        plan["nids_datasets_version"] = version("nids-datasets")
    except PackageNotFoundError:
        plan["nids_datasets_version"] = "unknown"
    print(json.dumps(plan, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
