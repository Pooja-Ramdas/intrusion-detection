import time
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from common import HDFS, RAW, fs_path, label_and_day, normalize_columns, path_exists, spark_session, write_small_table

SAMPLE_LIMIT = 50_000

def main():
    spark = spark_session("BDA-Phase2-ValidateLabels")
    spark.sparkContext.setLogLevel("WARN")
    flow = label_and_day(normalize_columns(spark.read.parquet(f"{RAW}/cicids2017_flow/CICIDS_Flow.parquet").limit(SAMPLE_LIMIT)), "cicids2017_flow")
    packet = label_and_day(normalize_columns(spark.read.parquet(f"{RAW}/cicids2017_packet/cicids2017_packet_slice.parquet").limit(SAMPLE_LIMIT)), "cicids2017_packet")
    started = time.time()
    # Packet schema has flow_id but no timestamp or 5-tuple capture timestamp. Use exact flow_id only.
    flow_labels = flow.select(F.col("flow_id").cast("string").alias("_key"), F.col("label_multi").alias("flow_label"), F.col("day").alias("flow_day")).dropDuplicates(["_key"])
    packet_labels = packet.select(F.col("flow_id").cast("string").alias("_key"), F.col("label_multi").alias("packet_label"), F.col("day").alias("packet_day"))
    flow_rows = flow_labels.count()
    fs, flow_path = fs_path(spark, f"{RAW}/cicids2017_flow/CICIDS_Flow.parquet")
    flow_file_bytes = int(fs.getFileStatus(flow_path).getLen())
    joined = packet_labels.join(broadcast(flow_labels), "_key", "left")
    metrics = joined.agg(
        F.count(F.lit(1)).alias("packet_rows"),
        F.sum(F.when(F.col("flow_label").isNotNull(), 1).otherwise(0)).alias("matched"),
        F.sum(F.when(F.col("flow_label").isNotNull() & (F.col("flow_label") != F.col("packet_label")), 1).otherwise(0)).alias("disagreements"),
    ).first()
    packet_rows = int(metrics["packet_rows"] or 0)
    matched = int(metrics["matched"] or 0)
    disagreements = joined.filter(F.col("flow_label").isNotNull() & (F.col("flow_label") != F.col("packet_label")))
    disagreement_count = int(metrics["disagreements"] or 0)
    unmatched = packet_rows - matched
    result = spark.createDataFrame([
        ("join_key", "flow_id_exact", "packet_timestamp_available", False),
        ("flow_rows_distinct", str(flow_rows), "", None),
        ("packet_rows", str(packet_rows), "", None),
        ("matched_packet_rows", str(matched), "", None),
        ("unmatched_packet_rows", str(unmatched), "", None),
        ("label_disagreement_rows", str(disagreement_count), "", None),
        ("match_rate", str(matched / packet_rows if packet_rows else 0.0), "", None),
        ("shuffle_policy", "broadcast projected distinct flow labels; packet side streamed", "", None),
        ("flow_projected_source_bytes", str(flow_file_bytes), "measured raw Parquet source bytes", None),
    ], "metric string, value string, note string, flag boolean")
    write_small_table(spark, result, "label_validation")
    sample_path = f"{HDFS}/bda/_meta/profile/phase2_v1/label_disagreements"
    if path_exists(spark, sample_path):
        raise RuntimeError(f"Refusing to overwrite existing disagreement sample: {sample_path}")
    disagreements.limit(1000).write.mode("errorifexists").option("compression", "snappy").parquet(sample_path)
    print(f"LABEL_VALIDATION packet_rows={packet_rows} matched={matched} disagreements={disagreement_count} seconds={time.time()-started:.2f} key=flow_id_exact", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
