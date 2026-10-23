"""
verify_hdfs.py — PySpark job: read each HDFS dataset, print row count, col count, schema.
spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/src/ingest/verify_hdfs.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BDA-Phase1-Verify") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DATASETS = {
    "cicids2017_flow":   "hdfs://namenode:9000/bda/raw/cicids2017_flow/",
    "cicids2017_packet": "hdfs://namenode:9000/bda/raw/cicids2017_packet/",
    "unswnb15":          "hdfs://namenode:9000/bda/raw/unswnb15/",
}

print("\n" + "=" * 70)
print("BDA Phase 1 — HDFS Dataset Verification")
print("=" * 70)

for name, path in DATASETS.items():
    print(f"\n--- {name} ---")
    print(f"Path: {path}")
    try:
        try:
            df = spark.read.parquet(path)
        except Exception:
            df = spark.read.option("header", "true") \
                           .option("inferSchema", "true") \
                           .csv(path)
        rows = df.count()
        cols = len(df.columns)
        print(f"Rows   : {rows:,}")
        print(f"Columns: {cols}")
        print("Schema :")
        df.printSchema()

        # Label distribution (if label column exists)
        label_col = next((c for c in df.columns
                          if c.strip().lower() in ("label", "attack", "class", "attack_label", " label")), None)
        if label_col:
            print(f"Label distribution (column='{label_col}'):")
            df.groupBy(label_col).count().orderBy("count", ascending=False).show(20, truncate=False)
    except Exception as e:
        print(f"ERROR reading {path}: {e}")

print("=" * 70)
print("Verification complete.")
spark.stop()
