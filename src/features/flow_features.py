"""
flow_features.py - Flow-level feature extraction, selection, and leakage removal.
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import List, Tuple

# Leakage and identifier columns to exclude from modeling features
IDENTIFIER_LEAKAGE_COLS = [
    "source_ip",
    "source_port",
    "destination_ip",
    "timestamp",
]

# Metadata and target columns to preserve for downstream training & evaluation
METADATA_COLS = [
    "flow_id",
    "label_raw",
    "label_multi",
    "is_attack",
    "label_binary",
    "day_of_week",
    "split_temporal",
    "split_stratified",
]

# Zero-variance (constant) columns identified empirically
ZERO_VARIANCE_COLS = [
    "bwd_psh_flags",
    "bwd_urg_flags",
    "fwd_avg_bytes_bulk",
    "fwd_avg_packets_bulk",
    "fwd_avg_bulk_rate",
    "bwd_avg_bytes_bulk",
    "bwd_avg_packets_bulk",
    "bwd_avg_bulk_rate",
]

# Highly correlated columns (|corr| > 0.95) dropped to prevent multicollinearity
HIGH_CORRELATION_DROPS = [
    "subflow_bwd_packets",
    "subflow_fwd_bytes",
    "syn_flag_count",
    "avg_fwd_segment_size",
    "fwd_iat_total",
    "cwe_flag_count",
    "average_packet_size",
    "fwd_packets_s",
    "subflow_fwd_packets",
    "bwd_packet_length_mean",
    "bwd_header_length",
    "avg_bwd_segment_size",
    "subflow_bwd_bytes",
    "total_backward_packets",
    "fwd_packet_length_std",
    "idle_max",
    "fwd_iat_max",
    "total_length_of_bwd_packets",
    "packet_length_std",
    "idle_min",
    "bwd_packet_length_std",
    "ece_flag_count",
    "idle_mean",
    "fwd_header_length",
]

# Core selected flow features (45 numeric + destination_port + protocol_num = 47)
SELECTED_FLOW_NUMERIC_FEATURES = [
    "flow_duration",
    "total_fwd_packets",
    "total_length_of_fwd_packets",
    "fwd_packet_length_max",
    "fwd_packet_length_min",
    "fwd_packet_length_mean",
    "bwd_packet_length_max",
    "bwd_packet_length_min",
    "flow_bytes_s",
    "flow_packets_s",
    "flow_iat_mean",
    "flow_iat_std",
    "flow_iat_max",
    "flow_iat_min",
    "fwd_iat_mean",
    "fwd_iat_std",
    "fwd_iat_min",
    "bwd_iat_total",
    "bwd_iat_mean",
    "bwd_iat_std",
    "bwd_iat_max",
    "bwd_iat_min",
    "fwd_psh_flags",
    "fwd_urg_flags",
    "bwd_packets_s",
    "min_packet_length",
    "max_packet_length",
    "packet_length_mean",
    "packet_length_variance",
    "fin_flag_count",
    "rst_flag_count",
    "psh_flag_count",
    "ack_flag_count",
    "urg_flag_count",
    "down_up_ratio",
    "init_win_bytes_forward",
    "init_win_bytes_backward",
    "act_data_pkt_fwd",
    "min_seg_size_forward",
    "active_mean",
    "active_std",
    "active_max",
    "active_min",
    "idle_std",
]

def extract_flow_features(df_flows: DataFrame) -> DataFrame:
    """
    Extracts, encodes, and filters flow features from the cleaned flow dataset.
    Retains destination_port as a structural target feature and protocol encoded as numeric.
    Removes source_ip, destination_ip, source_port, and raw timestamps to prevent leakage.
    Drops 8 zero-variance features and 24 highly correlated (|corr| > 0.95) features.
    """
    # 1. Encode protocol to numeric (tcp=6, udp=17, other=0)
    protocol_expr = (
        F.when(F.lower(F.col("protocol")) == "tcp", 6)
        .when(F.lower(F.col("protocol")) == "udp", 17)
        .otherwise(0)
    )
    df = df_flows.withColumn("protocol_num", protocol_expr.cast("int"))

    # 2. Cast destination_port to double/long as structural service feature
    df = df.withColumn("destination_port", F.col("destination_port").cast("double"))

    # 3. Assemble target feature selection
    keep_cols = (
        METADATA_COLS
        + ["destination_port", "protocol_num"]
        + SELECTED_FLOW_NUMERIC_FEATURES
    )

    return df.select(*keep_cols)

def get_flow_feature_names() -> List[str]:
    """Returns the list of active flow-level model feature names."""
    return ["destination_port", "protocol_num"] + SELECTED_FLOW_NUMERIC_FEATURES
