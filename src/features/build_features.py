"""
build_features.py - Main feature engineering pipeline.
Extracts flow, packet, and payload features, joins them, imputes missing values,
generates catalog and summary stats artifacts, and saves the full feature table to HDFS.
"""

import csv
import json
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from flow_features import (
    extract_flow_features,
    get_flow_feature_names,
    ZERO_VARIANCE_COLS,
    HIGH_CORRELATION_DROPS,
    METADATA_COLS,
)
from packet_aggregates import (
    extract_packet_aggregates,
    get_packet_feature_names,
)
from payload_features import (
    extract_payload_features,
    get_payload_feature_names,
)

ARTIFACTS_DIR = Path("/opt/spark/work-dir/artifacts")
FEATURES_DIR = ARTIFACTS_DIR / "features"
TIMINGS_DIR = ARTIFACTS_DIR / "timings"

def main():
    t_start = time.time()
    timings = {}

    spark = (
        SparkSession.builder
        .appName("BDA-Phase3-FeatureEngineering")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1100m")
        .config("spark.cores.max", "4")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("======================================================================")
    print("PHASE 3: Distributed Feature Engineering on Spark Cluster")
    print("======================================================================")

    # 1. Read Cleaned Data
    t0 = time.time()
    print("\n[Step 1] Reading cleaned flows and packets from HDFS...")
    df_flows = spark.read.parquet("hdfs://namenode:9000/bda/clean/flows/")
    df_packets = spark.read.parquet("hdfs://namenode:9000/bda/clean/packets/")
    timings["read_seconds"] = round(time.time() - t0, 2)
    print(f"  Read completed in {timings['read_seconds']}s")

    # 2. Extract Flow Features
    t0 = time.time()
    print("\n[Step 2] Extracting and filtering flow-level features...")
    df_flow_feat = extract_flow_features(df_flows)
    flow_features_list = get_flow_feature_names()
    print(f"  Selected flow features ({len(flow_features_list)}): {flow_features_list[:5]}...")
    timings["flow_features_seconds"] = round(time.time() - t0, 2)

    # 3. Extract Packet Aggregates
    t0 = time.time()
    print("\n[Step 3] Computing per-flow packet aggregates...")
    df_packet_feat = extract_packet_aggregates(df_packets, df_flows)
    packet_features_list = get_packet_feature_names()
    print(f"  Computed packet aggregates ({len(packet_features_list)}): {packet_features_list}")
    timings["packet_aggregates_seconds"] = round(time.time() - t0, 2)

    # 4. Extract Payload Features
    t0 = time.time()
    print("\n[Step 4] Computing payload-byte statistics, Shannon entropy & 16-bin histogram...")
    df_payload_feat = extract_payload_features(spark, df_packets, max_byte_cols=64)
    payload_features_list = get_payload_feature_names()
    print(f"  Computed payload features ({len(payload_features_list)}): {payload_features_list[:4]} + 16 bins")
    timings["payload_features_seconds"] = round(time.time() - t0, 2)

    # 5. Join Features
    t0 = time.time()
    print("\n[Step 5] Joining features into single per-flow feature table...")
    # Left join flow features with packet aggregates
    df_joined = df_flow_feat.join(df_packet_feat, on="flow_id", how="left")
    # Left join with payload features
    df_joined = df_joined.join(df_payload_feat, on="flow_id", how="left")

    # Add has_packet_features flag
    df_joined = df_joined.withColumn(
        "has_packet_features",
        F.when(F.col("pkt_count").isNotNull() & (F.col("pkt_count") > 0), 1).otherwise(0)
    )

    # Impute missing packet & payload features with 0.0 for flows without packets
    fill_dict = {col: 0.0 for col in packet_features_list + payload_features_list}
    df_full = df_joined.fillna(fill_dict)

    timings["join_seconds"] = round(time.time() - t0, 2)
    print(f"  Join and imputation finished in {timings['join_seconds']}s")

    # 6. Write Full Features to HDFS
    t0 = time.time()
    print("\n[Step 6] Writing /bda/features/flow_features_full/ to HDFS (8 coalesced partitions)...")
    output_path = "hdfs://namenode:9000/bda/features/flow_features_full/"
    df_full.coalesce(8).write.mode("overwrite").parquet(output_path)
    timings["write_seconds"] = round(time.time() - t0, 2)
    print(f"  Write completed in {timings['write_seconds']}s")

    # 7. Collect Feature Summary Stats and Verification
    t0 = time.time()
    print("\n[Step 7] Generating feature catalog and summary statistics...")
    total_flows = df_full.count()
    linked_flows_count = df_full.filter(F.col("has_packet_features") == 1).count()
    unlinked_flows_count = total_flows - linked_flows_count

    print(f"  Total Rows: {total_flows:,}")
  
    print(f"  Linked Flows (has_packet_features=1): {linked_flows_count:,} ({linked_flows_count / total_flows * 100:.2f}%)")
    print(f"  Unlinked Flows (has_packet_features=0): {unlinked_flows_count:,} ({unlinked_flows_count / total_flows * 100:.2f}%)")

    # Build Feature Catalog
    catalog_rows = []
    # Flow features
    for f in flow_features_list:
        desc = f"Flow-level metric: {f}"
        if f == "destination_port":
            desc = "Target transport layer service port (structural endpoint feature)"
        elif f == "protocol_num":
            desc = "Transport protocol encoded (6=TCP, 17=UDP, 0=Other)"
        catalog_rows.append({"feature_name": f, "group": "flow", "description": desc})

    # Packet features
    packet_descriptions = {
        "pkt_count": "Total packet count per flow from packet capture",
        "pkt_len_total": "Total payload bytes summed across linked packets",
        "pkt_len_mean": "Average payload length across linked packets",
        "pkt_len_std": "Standard deviation of payload length across linked packets",
        "pkt_len_min": "Minimum payload length in linked packets",
        "pkt_len_max": "Maximum payload length in linked packets",
        "pkt_payload_fraction": "Fraction of packets possessing non-zero payload (>0 bytes)",
        "pkt_fwd_ratio": "Ratio of forward directional packets to total packets",
        "pkt_iat_mean": "Mean packet inter-arrival time (0.0: timestamps absent in packet release)",
        "pkt_iat_std": "Stddev packet inter-arrival time (0.0: timestamps absent in packet release)",
        "pkt_iat_max": "Max packet inter-arrival time (0.0: timestamps absent in packet release)",
    }
    for f in packet_features_list:
        catalog_rows.append({"feature_name": f, "group": "packet", "description": packet_descriptions.get(f, f"Packet metric: {f}")})

    # Payload features
    payload_descriptions = {
        "payload_bytes_total": "Total payload bytes across all packets in the flow",
        "payload_bytes_mean": "Mean payload bytes per packet in the flow",
        "payload_bytes_max": "Maximum payload bytes in any packet of the flow",
        "payload_entropy": "Shannon entropy (bits, base-2) of payload bytes across packets",
    }
    for i in range(16):
        payload_descriptions[f"payload_hist_bin_{i}"] = f"Normalized byte histogram frequency bucket {i} (bytes [{i*16}..{i*16+15}])"

    for f in payload_features_list:
        catalog_rows.append({"feature_name": f, "group": "payload", "description": payload_descriptions.get(f, f"Payload metric: {f}")})

    # Flag
    catalog_rows.append({
        "feature_name": "has_packet_features",
        "group": "metadata_flag",
        "description": "Binary indicator: 1 if flow has linked packet capture records, 0 otherwise"
    })

    # Write Feature Catalog CSV
    FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    catalog_file = FEATURES_DIR / "feature_catalog.csv"
    with open(catalog_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["feature_name", "group", "description"])
        writer.writeheader()
        writer.writerows(catalog_rows)
    print(f"  Feature catalog saved to {catalog_file} ({len(catalog_rows)} features)")

    # Sample stats on key features
    sample_stats_df = df_full.select(
        "flow_duration", "flow_bytes_s", "destination_port",
        "pkt_count", "pkt_len_mean", "payload_entropy", "payload_hist_bin_2"
    ).summary("count", "mean", "stddev", "min", "max")
    stats_dict = {row["summary"]: {c: row[c] for c in row.asDict() if c != "summary"} for row in sample_stats_df.collect()}

    feature_stats = {
        "total_rows": total_flows,
        "total_columns": len(df_full.columns),
        "feature_counts_by_group": {
            "flow": len(flow_features_list),
            "packet": len(packet_features_list),
            "payload": len(payload_features_list),
            "metadata_flag": 1,
            "total_model_features": len(catalog_rows),
        },
        "flow_linkage": {
            "total_flows": total_flows,
            "linked_flows": linked_flows_count,
            "unlinked_flows": unlinked_flows_count,
            "link_rate_percent": round((linked_flows_count / total_flows) * 100, 4),
            "imputation_rule": "Zero-fill (0.0) for numeric packet and payload attributes on unlinked flows"
        },
        "zero_variance_drops": ZERO_VARIANCE_COLS,
        "high_correlation_drops": HIGH_CORRELATION_DROPS,
        "sample_feature_summary": stats_dict,
    }

    stats_file = FEATURES_DIR / "feature_stats.json"
    with open(stats_file, "w", encoding="utf-8") as f:
        json.dump(feature_stats, f, indent=2)
    print(f"  Feature stats saved to {stats_file}")

    timings["total_phase3_seconds"] = round(time.time() - t_start, 2)
    timings_file = TIMINGS_DIR / "phase3.json"
    TIMINGS_DIR.mkdir(parents=True, exist_ok=True)
    with open(timings_file, "w", encoding="utf-8") as f:
        json.dump(timings, f, indent=2)
    print(f"  Phase 3 timings saved to {timings_file}")

    print("\nSample Output Records (Cleaned Features + Metadata):")
    df_full.select("flow_id", "destination_port", "has_packet_features", "pkt_count", "payload_entropy", "label_multi", "split_stratified").show(5)

    print("======================================================================")
    print(f"Phase 3 Complete. Total Wallclock Time: {timings['total_phase3_seconds']}s")
    print("======================================================================")

    spark.stop()

if __name__ == "__main__":
    main()
