"""
profile_raw.py — Distributed profiling of raw datasets in HDFS using PySpark.
Saves compact JSON profiling reports in artifacts/profile/.
"""

import sys
import time
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, DoubleType, FloatType

START_TIME = time.time()
ARTIFACTS_DIR = Path("/opt/spark/work-dir/artifacts/profile")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

spark = SparkSession.builder \
    .appName("BDA-Phase2-ProfileRaw") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DAY_MAP = {
    "Monday": "Monday",
    "03/07/2017": "Monday",
    "3/7/2017": "Monday",
    "Tuesday": "Tuesday",
    "04/07/2017": "Tuesday",
    "4/7/2017": "Tuesday",
    "Wednesday": "Wednesday",
    "05/07/2017": "Wednesday",
    "5/7/2017": "Wednesday",
    "Thursday": "Thursday",
    "06/07/2017": "Thursday",
    "6/7/2017": "Thursday",
    "Friday": "Friday",
    "07/07/2017": "Friday",
    "7/7/2017": "Friday"
}

print("=" * 70)
print("PHASE 2: Profiling Raw Datasets on HDFS")
print("=" * 70)

# ── 1. Profile CIC-IDS2017 Flow Data ──────────────────────────────────────────
print("\n[1/3] Profiling CIC-IDS2017 Flow-Level Dataset...")
t0 = time.time()
df_flow = spark.read.parquet("hdfs://namenode:9000/bda/raw/cicids2017_flow/")
flow_rows = df_flow.count()
flow_cols = len(df_flow.columns)
print(f"  Rows: {flow_rows:,}, Columns: {flow_cols}")

# Approximate duplicates count (exact on 5-tuple + Timestamp + Label)
tuple_cols = ["source_ip", "source_port", "destination_ip", "destination_port", "protocol", "Timestamp", "attack_label"]
flow_distinct_tuples = df_flow.select(*tuple_cols).distinct().count()
flow_duplicate_rows = flow_rows - flow_distinct_tuples
print(f"  Duplicate flow records: {flow_duplicate_rows:,}")

# Label distribution
print("  Computing label distribution...")
label_counts = df_flow.groupBy("attack_label").count().collect()
label_dist = {
    row["attack_label"]: {
        "count": row["count"],
        "percent": round((row["count"] / flow_rows) * 100, 4)
    }
    for row in sorted(label_counts, key=lambda r: r["count"], reverse=True)
}

# Per-day label distribution
print("  Computing per-day label distribution...")
df_flow_day = df_flow.withColumn("date_str", F.split(F.col("Timestamp"), " ")[0])
day_counts = df_flow_day.groupBy("date_str", "attack_label").count().collect()
per_day_dist = {}
for r in day_counts:
    d_label = DAY_MAP.get(r["date_str"], r["date_str"])
    if d_label not in per_day_dist:
        per_day_dist[d_label] = {}
    per_day_dist[d_label][r["attack_label"]] = r["count"]

# Null, NaN, and Infinity counts on key columns
print("  Checking Null, NaN, and Infinity occurrences...")
inf_nan_cols = ["Flow Bytes/s", "Flow Packets/s", "Flow Duration", "Total Fwd Packets", "Total Backward Packets"]
inf_nan_stats = {}

for c in inf_nan_cols:
    if c in df_flow.columns:
        # Check nulls, nan, and string/double infinities
        null_cnt = df_flow.filter(F.col(c).isNull()).count()
        nan_cnt = df_flow.filter(F.isnan(F.col(c).cast("double"))).count()
        # In Spark SQL, check for infinite values
        inf_cnt = df_flow.filter(
            (F.col(c).cast("string") == "Infinity") | 
            (F.col(c).cast("string") == "+Infinity") | 
            (F.col(c).cast("string") == "-Infinity") |
            (F.col(c) == float("inf")) | 
            (F.col(c) == float("-inf"))
        ).count()
        inf_nan_stats[c] = {
            "null_count": null_cnt,
            "nan_count": nan_cnt,
            "infinity_count": inf_cnt
        }

# Descriptive statistics for key numeric columns
print("  Computing summary statistics on key columns...")
clean_exprs = []
for c in ["Flow Duration", "Total Fwd Packets", "Total Backward Packets", "Total Length of Fwd Packets", "Total Length of Bwd Packets"]:
    if c in df_flow.columns:
        clean_exprs.extend([
            F.mean(F.col(c).cast("double")).alias(f"{c}__mean"),
            F.stddev(F.col(c).cast("double")).alias(f"{c}__stddev"),
            F.min(F.col(c).cast("double")).alias(f"{c}__min"),
            F.max(F.col(c).cast("double")).alias(f"{c}__max")
        ])

summary_row = df_flow.select(*clean_exprs).collect()[0].asDict()
numeric_stats = {}
for c in ["Flow Duration", "Total Fwd Packets", "Total Backward Packets", "Total Length of Fwd Packets", "Total Length of Bwd Packets"]:
    if f"{c}__mean" in summary_row:
        numeric_stats[c] = {
            "mean": round(summary_row[f"{c}__mean"] or 0, 2),
            "stddev": round(summary_row[f"{c}__stddev"] or 0, 2),
            "min": round(summary_row[f"{c}__min"] or 0, 2),
            "max": round(summary_row[f"{c}__max"] or 0, 2)
        }

# Top Protocols and Destination Ports
top_ports = [
    {"port": r["destination_port"], "count": r["count"]}
    for r in df_flow.groupBy("destination_port").count().orderBy(F.desc("count")).limit(10).collect()
]
top_protocols = [
    {"protocol": r["protocol"], "count": r["count"]}
    for r in df_flow.groupBy("protocol").count().orderBy(F.desc("count")).collect()
]

flow_profile = {
    "dataset": "cicids2017_flow",
    "row_count": flow_rows,
    "col_count": flow_cols,
    "duplicate_records_by_5tuple_timestamp": flow_duplicate_rows,
    "label_distribution": label_dist,
    "per_day_distribution": per_day_dist,
    "inf_nan_stats": inf_nan_stats,
    "numeric_stats": numeric_stats,
    "top_destination_ports": top_ports,
    "top_protocols": top_protocols,
    "elapsed_seconds": round(time.time() - t0, 2)
}

(ARTIFACTS_DIR / "flow_profile.json").write_text(json.dumps(flow_profile, indent=2))
print(f"  Flow profile saved to {ARTIFACTS_DIR / 'flow_profile.json'}")

# ── 2. Profile CIC-IDS2017 Packet Data ────────────────────────────────────────
print("\n[2/3] Profiling CIC-IDS2017 Packet-Level Dataset...")
t1 = time.time()
df_packet = spark.read.parquet("hdfs://namenode:9000/bda/raw/cicids2017_packet/")
packet_rows = df_packet.count()
packet_cols = len(df_packet.columns)
print(f"  Rows: {packet_rows:,}, Columns: {packet_cols}")

packet_meta_cols = [c for c in df_packet.columns if not c.startswith("payload_byte_")]
packet_nulls = {
    c: df_packet.filter(F.col(c).isNull()).count()
    for c in packet_meta_cols
}
packet_label_counts = df_packet.groupBy("attack_label").count().collect()
packet_labels = {
    r["attack_label"]: {
        "count": r["count"],
        "percent": round((r["count"] / packet_rows) * 100, 4)
    }
    for r in sorted(packet_label_counts, key=lambda x: x["count"], reverse=True)
}

packet_profile = {
    "dataset": "cicids2017_packet",
    "row_count": packet_rows,
    "col_count": packet_cols,
    "metadata_columns": packet_meta_cols,
    "null_counts": packet_nulls,
    "label_distribution": packet_labels,
    "payload_bytes_columns": packet_cols - len(packet_meta_cols),
    "elapsed_seconds": round(time.time() - t1, 2)
}
(ARTIFACTS_DIR / "packet_profile.json").write_text(json.dumps(packet_profile, indent=2))
print(f"  Packet profile saved to {ARTIFACTS_DIR / 'packet_profile.json'}")

# ── 3. Profile UNSW-NB15 Validation Data ──────────────────────────────────────
print("\n[3/3] Profiling UNSW-NB15 Validation Dataset...")
t2 = time.time()
df_unsw = spark.read.parquet("hdfs://namenode:9000/bda/raw/unswnb15/")
unsw_rows = df_unsw.count()
unsw_cols = len(df_unsw.columns)
print(f"  Rows: {unsw_rows:,}, Columns: {unsw_cols}")

unsw_labels = {
    str(r["label"]): r["count"]
    for r in df_unsw.groupBy("label").count().collect()
}
unsw_attacks = {
    str(r["attack_cat"]): r["count"]
    for r in df_unsw.groupBy("attack_cat").count().orderBy(F.desc("count")).collect()
}

unsw_profile = {
    "dataset": "unswnb15",
    "row_count": unsw_rows,
    "col_count": unsw_cols,
    "label_distribution": unsw_labels,
    "attack_cat_distribution": unsw_attacks,
    "elapsed_seconds": round(time.time() - t2, 2)
}
(ARTIFACTS_DIR / "unsw_profile.json").write_text(json.dumps(unsw_profile, indent=2))
print(f"  UNSW profile saved to {ARTIFACTS_DIR / 'unsw_profile.json'}")

# Overall Summary
summary = {
    "cicids2017_flow_rows": flow_rows,
    "cicids2017_packet_rows": packet_rows,
    "unswnb15_rows": unsw_rows,
    "total_raw_rows": flow_rows + packet_rows + unsw_rows,
    "flow_duplicates": flow_duplicate_rows,
    "flow_infinity_bytes_sec": inf_nan_stats.get("Flow Bytes/s", {}).get("infinity_count", 0),
    "flow_infinity_packets_sec": inf_nan_stats.get("Flow Packets/s", {}).get("infinity_count", 0),
    "total_profiling_wallclock_seconds": round(time.time() - START_TIME, 2)
}
(ARTIFACTS_DIR / "raw_profile_summary.json").write_text(json.dumps(summary, indent=2))

print("\n" + "=" * 70)
print(f"Raw Profiling Complete in {summary['total_profiling_wallclock_seconds']}s")
print("=" * 70)
spark.stop()
