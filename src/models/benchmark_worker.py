"""
benchmark_worker.py - Single scalability run measuring RF fit time given cores and data fraction.
"""

import argparse
import csv
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cores", type=int, required=True, help="Total spark cores")
    parser.add_argument("--fraction", type=float, required=True, help="Fraction of training data (0.5 or 1.0)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"Scalability-Cores{args.cores}-Frac{args.fraction}")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1100m")
        .config("spark.cores.max", str(args.cores))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Load features
    catalog_path = Path("/opt/spark/work-dir/artifacts/features/feature_catalog.csv")
    with open(catalog_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        feature_cols = [r["feature_name"] for r in reader if r["group"] in ("flow", "packet", "payload", "metadata_flag")]

    df = spark.read.parquet("hdfs://namenode:9000/bda/features/flow_features_full/")
    train_df = df.filter(F.col("split_stratified") == "train")

    if args.fraction < 1.0:
        train_df = train_df.sample(fraction=args.fraction, seed=42)

    train_count = train_df.count()

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="is_attack",
        numTrees=30,
        maxDepth=8,
        maxBins=32,
        seed=42,
    )
    pipeline = Pipeline(stages=[assembler, rf])

    # Measure pure training wallclock time
    t0 = time.time()
    model = pipeline.fit(train_df)
    fit_duration = round(time.time() - t0, 2)

    spark.stop()

    print(f"RESULT: cores={args.cores}, fraction={args.fraction}, rows={train_count}, fit_time={fit_duration}s")
    # Write to temporary file for runner aggregation
    out_line = f"{args.cores},{args.fraction},{train_count},{fit_duration}\n"
    with open("/tmp/scalability_runs.csv", "a") as f:
        f.write(out_line)

if __name__ == "__main__":
    main()
