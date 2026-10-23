"""
packet_aggregates.py - Per-flow packet aggregates from packet-level captures.
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import List

PACKET_FEATURE_NAMES = [
    "pkt_count",
    "pkt_len_total",
    "pkt_len_mean",
    "pkt_len_std",
    "pkt_len_min",
    "pkt_len_max",
    "pkt_payload_fraction",
    "pkt_fwd_ratio",
    "pkt_iat_mean",
    "pkt_iat_std",
    "pkt_iat_max",
]

def extract_packet_aggregates(df_packets: DataFrame, df_flows: DataFrame) -> DataFrame:
    """
    Computes per-flow packet aggregates:
    - Packet count
    - Payload length statistics (sum, mean, std, min, max)
    - Fraction of packets with non-zero payload
    - Forward/backward packet direction ratio
    - Inter-arrival times (set to 0.0 with documentation since timestamps are absent in packet dataset)
    """
    # Join with flow source_ip to classify directional forward vs backward packets
    flow_endpoints = df_flows.select(
        F.col("flow_id"),
        F.col("source_ip").alias("flow_client_ip")
    ).distinct()

    pkts_annotated = df_packets.join(flow_endpoints, on="flow_id", how="left")

    # Packet direction: 1 if source matches flow client IP, 0 otherwise
    is_fwd_expr = F.when(
        F.col("source_ip") == F.col("flow_client_ip"), 1.0
    ).otherwise(0.0)
    pkts_annotated = pkts_annotated.withColumn("is_fwd_pkt", is_fwd_expr)

    # Group by flow_id and aggregate
    agg_df = pkts_annotated.groupBy("flow_id").agg(
        F.count("packet_id").alias("pkt_count"),
        F.sum("payload_length").alias("pkt_len_total"),
        F.avg("payload_length").alias("pkt_len_mean"),
        F.coalesce(F.stddev("payload_length"), F.lit(0.0)).alias("pkt_len_std"),
        F.min("payload_length").alias("pkt_len_min"),
        F.max("payload_length").alias("pkt_len_max"),
        F.avg(F.when(F.col("payload_length") > 0, 1.0).otherwise(0.0)).alias("pkt_payload_fraction"),
        F.avg("is_fwd_pkt").alias("pkt_fwd_ratio"),
    )

    # Add explicit 0.0 placeholders for fields absent in packet dataset (documented in schema notes)
    agg_df = (
        agg_df
        .withColumn("pkt_iat_mean", F.lit(0.0))
        .withColumn("pkt_iat_std", F.lit(0.0))
        .withColumn("pkt_iat_max", F.lit(0.0))
    )

    # Cast to standard double types
    for c in PACKET_FEATURE_NAMES:
        agg_df = agg_df.withColumn(c, F.col(c).cast("double"))

    return agg_df.select("flow_id", *PACKET_FEATURE_NAMES)

def get_packet_feature_names() -> List[str]:
    """Returns the list of packet-level feature names."""
    return PACKET_FEATURE_NAMES
