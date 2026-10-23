"""
payload_features.py - Payload-byte statistics and Shannon entropy per flow.
"""

import math
from typing import List, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType

PAYLOAD_HIST_BINS = [f"payload_hist_bin_{i}" for i in range(16)]
PAYLOAD_FEATURE_NAMES = [
    "payload_bytes_total",
    "payload_bytes_mean",
    "payload_bytes_max",
    "payload_entropy",
] + PAYLOAD_HIST_BINS

def compute_entropy_from_counts(counts: List[int], total: int) -> float:
    """Computes standard Shannon entropy in bits (base-2) given byte frequencies."""
    if total <= 0:
        return 0.0
    entropy = 0.0
    for c in counts:
        if c > 0:
            p = c / total
            entropy -= p * math.log2(p)
    return float(entropy)

def compute_histogram_bins(counts: List[int], total: int, num_bins: int = 16) -> List[float]:
    """Buckets 256 byte counts into num_bins normalized frequency bins."""
    if total <= 0:
        return [0.0] * num_bins
    bin_size = 256 // num_bins
    bins = [0.0] * num_bins
    for i in range(num_bins):
        sub_sum = sum(counts[i * bin_size : (i + 1) * bin_size])
        bins[i] = float(sub_sum / total)
    return bins

def extract_payload_features(spark: SparkSession, df_packets: DataFrame, max_byte_cols: int = 64) -> DataFrame:
    """
    Extracts payload statistics per flow:
    - payload_bytes_total, payload_bytes_mean, payload_bytes_max (from payload_length)
    - Shannon entropy across payload bytes
    - 16-bucket normalized byte histogram
    """
    # 1. First aggregate payload length metrics per flow
    len_agg = df_packets.groupBy("flow_id").agg(
        F.sum("payload_length").cast("double").alias("payload_bytes_total"),
        F.avg("payload_length").cast("double").alias("payload_bytes_mean"),
        F.max("payload_length").cast("double").alias("payload_bytes_max"),
    )

    # 2. Extract byte counts across representative payload window (first 64 bytes)
    byte_cols = [f"payload_byte_{i}" for i in range(1, max_byte_cols + 1)]
    proj_df = df_packets.select("flow_id", *byte_cols)

    def row_to_counts(row):
        flow_id = row[0]
        counts = [0] * 256
        for b in row[1:]:
            if b is not None and 0 <= b <= 255:
                counts[b] += 1
        return (flow_id, counts)

    def merge_counts(a, b):
        return [x + y for x, y in zip(a, b)]

    def counts_to_features(item):
        flow_id, counts = item
        total = sum(counts)
        entropy = compute_entropy_from_counts(counts, total)
        bins = compute_histogram_bins(counts, total, 16)
        return (flow_id, float(entropy), *[float(x) for x in bins])

    rdd_counts = proj_df.rdd.map(row_to_counts).reduceByKey(merge_counts)
    stats_rdd = rdd_counts.map(counts_to_features)

    entropy_schema = StructType(
        [StructField("flow_id", LongType(), False), StructField("payload_entropy", DoubleType(), False)]
        + [StructField(f"payload_hist_bin_{i}", DoubleType(), False) for i in range(16)]
    )

    df_entropy_hist = spark.createDataFrame(stats_rdd, entropy_schema)

    # Join payload length metrics with entropy & histogram
    payload_df = len_agg.join(df_entropy_hist, on="flow_id", how="inner")
    return payload_df.select("flow_id", *PAYLOAD_FEATURE_NAMES)

def get_payload_feature_names() -> List[str]:
    """Returns the list of payload feature names."""
    return PAYLOAD_FEATURE_NAMES
