"""
test_features.py - Unit tests for feature engineering pipeline.
Runnable inside the bda-spark container via:
  python3 tests/test_features.py
"""

import math
import sys
from pathlib import Path

# Add src/features to Python module path
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "features"))

from payload_features import compute_entropy_from_counts, compute_histogram_bins

def test_entropy_known_strings():
    print("[Test 1] Testing Shannon entropy on known byte distributions...")
    
    # 1. Zero/empty bytes
    counts_empty = [0] * 256
    assert compute_entropy_from_counts(counts_empty, 0) == 0.0, "Empty bytes should have 0.0 entropy"
    
    # 2. Homogeneous byte string (e.g. b"AAAA" -> all same byte)
    counts_mono = [0] * 256
    counts_mono[65] = 100
    entropy_mono = compute_entropy_from_counts(counts_mono, 100)
    assert abs(entropy_mono - 0.0) < 1e-6, f"Expected 0.0, got {entropy_mono}"
    print("  ✓ Monotonic byte string entropy == 0.0 bits")

    # 3. Two equally distributed byte values (e.g. b"ABAB" -> 50% byte 65, 50% byte 66)
    counts_binary = [0] * 256
    counts_binary[65] = 50
    counts_binary[66] = 50
    entropy_binary = compute_entropy_from_counts(counts_binary, 100)
    assert abs(entropy_binary - 1.0) < 1e-6, f"Expected 1.0, got {entropy_binary}"
    print("  ✓ Two equal byte values entropy == 1.0 bit")

    # 4. Four equally distributed byte values (e.g. b"ABCD" -> 25% each of 4 bytes)
    counts_quad = [0] * 256
    counts_quad[65] = 25
    counts_quad[66] = 25
    counts_quad[67] = 25
    counts_quad[68] = 25
    entropy_quad = compute_entropy_from_counts(counts_quad, 100)
    assert abs(entropy_quad - 2.0) < 1e-6, f"Expected 2.0, got {entropy_quad}"
    print("  ✓ Four equal byte values entropy == 2.0 bits")

def test_histogram_bins():
    print("\n[Test 2] Testing 16-bucket byte histogram normalization...")
    counts = [0] * 256
    # Put 50 bytes in bin 0 (bytes 0-15) and 50 bytes in bin 15 (bytes 240-255)
    counts[5] = 50
    counts[250] = 50
    bins = compute_histogram_bins(counts, 100, 16)
    assert len(bins) == 16, f"Expected 16 bins, got {len(bins)}"
    assert abs(bins[0] - 0.5) < 1e-6, f"Expected bin 0 == 0.5, got {bins[0]}"
    assert abs(bins[15] - 0.5) < 1e-6, f"Expected bin 15 == 0.5, got {bins[15]}"
    assert abs(sum(bins) - 1.0) < 1e-6, f"Histogram bins must sum to 1.0, got {sum(bins)}"
    print("  ✓ 16-bucket histogram normalization and bounds verified")

def test_spark_null_safety_and_row_preservation():
    print("\n[Test 3] Testing Null-Safety and Join Row-Count Preservation...")
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

    spark = SparkSession.builder.master("local[2]").appName("TestFeaturesSpark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Synthetic flows: 100 records
    flow_data = [(i, float(i * 10), "BENIGN") for i in range(100)]
    flow_schema = StructType([
        StructField("flow_id", LongType(), False),
        StructField("flow_duration", DoubleType(), True),
        StructField("label_multi", StringType(), False),
    ])
    df_f = spark.createDataFrame(flow_data, flow_schema)

    # Synthetic packets: only for 10 flows (flow_id 0 to 9)
    packet_data = [(i, float(i + 1), 3.5) for i in range(10)]
    packet_schema = StructType([
        StructField("flow_id", LongType(), False),
        StructField("pkt_count", DoubleType(), True),
        StructField("payload_entropy", DoubleType(), True),
    ])
    df_p = spark.createDataFrame(packet_data, packet_schema)

    # Left join
    df_joined = df_f.join(df_p, on="flow_id", how="left")

    # Add flag
    df_joined = df_joined.withColumn(
        "has_packet_features",
        F.when(F.col("pkt_count").isNotNull() & (F.col("pkt_count") > 0), 1).otherwise(0)
    )

    # Impute missing with 0.0
    df_imputed = df_joined.fillna({"pkt_count": 0.0, "payload_entropy": 0.0})

    # Assertions
    # 1. Row count preservation
    assert df_imputed.count() == 100, f"Expected exactly 100 rows, got {df_imputed.count()}"
    print("  ✓ Exact row count (100) preserved after left join")

    # 2. Linked vs unlinked count
    linked_cnt = df_imputed.filter(F.col("has_packet_features") == 1).count()
    unlinked_cnt = df_imputed.filter(F.col("has_packet_features") == 0).count()
    assert linked_cnt == 10, f"Expected 10 linked flows, got {linked_cnt}"
    assert unlinked_cnt == 90, f"Expected 90 unlinked flows, got {unlinked_cnt}"
    print("  ✓ has_packet_features correctly flagged (10 linked, 90 unlinked)")

    # 3. Null safety check
    for col_name in ["pkt_count", "payload_entropy", "flow_duration"]:
        null_cnt = df_imputed.filter(F.col(col_name).isNull()).count()
        assert null_cnt == 0, f"Found {null_cnt} nulls in column {col_name}"
    print("  ✓ Zero nulls present in all numeric feature columns post-imputation")

    spark.stop()

def main():
    print("=" * 60)
    print("RUNNING UNIT TESTS FOR FEATURE ENGINEERING PIPELINE")
    print("=" * 60)
    test_entropy_known_strings()
    test_histogram_bins()
    test_spark_null_safety_and_row_preservation()
    print("\n" + "=" * 60)
    print("ALL UNIT TESTS PASSED SUCCESSFULLY!")
    print("=" * 60)

if __name__ == "__main__":
    main()
