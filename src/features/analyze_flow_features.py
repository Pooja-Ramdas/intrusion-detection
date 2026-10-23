import json
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("FlowFeatureSelection").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/bda/clean/flows/")

# Identify columns to exclude from modeling features
EXCLUDE_COLS = {
    "flow_id", "source_ip", "source_port", "destination_ip", "timestamp",
    "label_raw", "label_multi", "is_attack", "label_binary", "day_of_week",
    "split_temporal", "split_stratified", "protocol"
}

# Candidate flow numeric features
candidate_features = [c for c, t in df.dtypes if c not in EXCLUDE_COLS and t in ("bigint", "int", "double", "float")]
print(f"Total candidate flow features: {len(candidate_features)}")

# Sample 100k rows for variance and correlation analysis
sample_df = df.select(candidate_features).sample(fraction=0.05, seed=42).limit(100000).toPandas()
print(f"Sampled {len(sample_df)} rows for feature analysis.")

# 1. Zero variance check
variances = sample_df.var()
zero_variance_cols = variances[variances == 0.0].index.tolist()
print(f"Zero variance columns ({len(zero_variance_cols)}): {zero_variance_cols}")

# Filter out zero variance columns
active_df = sample_df.drop(columns=zero_variance_cols)
print(f"Remaining features after zero variance filtering: {len(active_df.columns)}")

# 2. Correlation check (|corr| > 0.95)
corr_matrix = active_df.corr().abs()

# Find pairs with correlation > 0.95
upper_tri = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
high_corr_drops = []
corr_pairs = []

for col in upper_tri.columns:
    high_corr = upper_tri[col][upper_tri[col] > 0.95]
    for orig_col, corr_val in high_corr.items():
        corr_pairs.append({
            "feature_kept": orig_col,
            "feature_dropped": col,
            "correlation": round(float(corr_val), 4)
        })
        if col not in high_corr_drops and col not in [p["feature_dropped"] for p in corr_pairs[:-1]]:
            high_corr_drops.append(col)

# Deduplicate drops
high_corr_drops = list(set(high_corr_drops))
print(f"High correlation drops ({len(high_corr_drops)}): {high_corr_drops}")

selected_features = [c for c in active_df.columns if c not in high_corr_drops]
print(f"Final selected flow features: {len(selected_features)}")

result = {
    "candidate_features_count": len(candidate_features),
    "zero_variance_cols": zero_variance_cols,
    "high_corr_drops": high_corr_drops,
    "high_corr_pairs": corr_pairs,
    "selected_features": selected_features,
    "selected_features_count": len(selected_features)
}

with open("/tmp/flow_feature_selection.json", "w") as f:
    json.dump(result, f, indent=2)

spark.stop()
