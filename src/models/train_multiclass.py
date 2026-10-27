"""
train_multiclass.py - Distributed multi-class classification with Spark MLlib.
Trains RandomForestClassifier for 15 canonical network traffic classes
(BENIGN + 14 attack types) with and without inverse-frequency class weights.
Evaluates on both Stratified and Friday Hold-out test sets.
Explicitly analyzes rare attack classes and records confusion matrices.
"""

import csv
import json
import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier

from eval_utils import evaluate_predictions

ARTIFACTS_DIR = Path("/opt/spark/work-dir/artifacts")
MODELS_DIR = ARTIFACTS_DIR / "models"

def main():
    t_start = time.time()
    spark = (
        SparkSession.builder
        .appName("BDA-Phase4-TrainMultiClass")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1100m")
        .config("spark.cores.max", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("======================================================================")
    print("PHASE 4: Distributed Multi-Class Model Training (15 Classes)")
    print("======================================================================")
    print("Note: Spark MLlib GBTClassifier is natively binary-only.")
    print("RandomForestClassifier is used for distributed multi-class classification.")

    # 1. Load Feature Catalog
    catalog_path = ARTIFACTS_DIR / "features" / "feature_catalog.csv"
    with open(catalog_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        full_features = [r["feature_name"] for r in reader if r["group"] in ("flow", "packet", "payload", "metadata_flag")]

    # 2. Load Features Table
    print("\n[Step 1] Loading full feature table from HDFS...")
    t0 = time.time()
    df_all = spark.read.parquet("hdfs://namenode:9000/bda/features/flow_features_full/")
    total_records = df_all.count()
    print(f"  Loaded {total_records:,} rows in {time.time() - t0:.2f}s")

    # Fit StringIndexer on label_multi across full dataset to preserve label mapping
    indexer = StringIndexer(inputCol="label_multi", outputCol="label_index", handleInvalid="keep")
    indexer_model = indexer.fit(df_all)
    label_names = list(indexer_model.labels)
    num_classes = len(label_names)
    print(f"  Indexed {num_classes} classes: {label_names}")

    df_indexed = indexer_model.transform(df_all)

    # Splits
    train_strat = df_indexed.filter(F.col("split_stratified") == "train")
    test_strat = df_indexed.filter(F.col("split_stratified") == "test")
    train_temp = df_indexed.filter(F.col("split_temporal") == "train")
    test_temp = df_indexed.filter(F.col("split_temporal") == "test")

    # Compute Multi-class Inverse Frequency Weights
    n_train = train_strat.count()
    class_counts = train_strat.groupBy("label_index").count().collect()
    count_map = {int(r["label_index"]): int(r["count"]) for r in class_counts}

    # Weight per class: w_c = min(n_train / (K * n_c), 500.0)
    weight_map = {}
    print("\nMulti-Class Training Distribution & Assigned Class Weights:")
    for idx, cname in enumerate(label_names):
        c_count = count_map.get(idx, 0)
        if c_count > 0:
            raw_w = n_train / (num_classes * c_count)
            capped_w = min(raw_w, 500.0)  # Bound to prevent tree split destabilization
        else:
            capped_w = 1.0
        weight_map[idx] = round(capped_w, 4)
        print(f"  Class {idx:2d} ({cname:<24}): count={c_count:9,d} | weight={capped_w:8.2f}")

    # Add weight column to training set
    weight_cond = F.when(F.col("label_index") == 0, weight_map.get(0, 1.0))
    for c_idx in range(1, num_classes):
        weight_cond = weight_cond.when(F.col("label_index") == c_idx, weight_map.get(c_idx, 1.0))
    weight_cond = weight_cond.otherwise(1.0)

    train_strat_weighted = train_strat.withColumn("class_weight", weight_cond)

    assembler = VectorAssembler(inputCols=full_features, outputCol="features")

    multiclass_metrics = {}
    confusion_matrices = {}

    # ──────────────────────────────────────────────────────────────────────────
    # Model 1: Multi-Class Random Forest (Weighted)
    # ──────────────────────────────────────────────────────────────────────────
    print("\n[Step 2] Training Multi-Class Random Forest (Weighted, numTrees=40, maxDepth=8)...")
    t0 = time.time()
    rf_multi_w = RandomForestClassifier(
        featuresCol="features",
        labelCol="label_index",
        weightCol="class_weight",
        numTrees=40,
        maxDepth=8,
        maxBins=32,
        seed=42,
    )
    pipe_multi_w = Pipeline(stages=[assembler, rf_multi_w])
    model_multi_w = pipe_multi_w.fit(train_strat_weighted)
    fit_time_w = round(time.time() - t0, 2)
    print(f"  Weighted Multi-Class RF trained in {fit_time_w}s")

    # Evaluate Stratified Test
    preds_strat_w = model_multi_w.transform(test_strat)
    metrics_strat_w = evaluate_predictions(
        preds_strat_w, label_col="label_index", is_binary=False, label_names=label_names
    )

    # Evaluate Friday Hold-out Test
    preds_temp_w = model_multi_w.transform(test_temp)
    metrics_temp_w = evaluate_predictions(
        preds_temp_w, label_col="label_index", is_binary=False, label_names=label_names
    )

    multiclass_metrics["RandomForest_Multiclass_Weighted"] = {
        "hyperparameters": {"numTrees": 50, "maxDepth": 10, "maxBins": 32, "weightCol": "inverse_frequency_capped"},
        "fit_time_seconds": fit_time_w,
        "stratified_test": metrics_strat_w,
        "friday_holdout_test": metrics_temp_w,
    }
    confusion_matrices["RF_Multiclass_Weighted_Stratified"] = metrics_strat_w["confusion_matrix"]
    confusion_matrices["RF_Multiclass_Weighted_Friday"] = metrics_temp_w["confusion_matrix"]

    print(f"  [Stratified Test] Acc: {metrics_strat_w['accuracy']}, Weighted F1: {metrics_strat_w['weighted_f1']}, Macro F1: {metrics_strat_w['macro_f1']}")
    print(f"  [Friday Holdout ] Acc: {metrics_temp_w['accuracy']}, Weighted F1: {metrics_temp_w['weighted_f1']}, Macro F1: {metrics_temp_w['macro_f1']}")

    # Save to HDFS
    print("  Saving Weighted Multi-Class RF to HDFS /bda/models/rf_multiclass_weighted ...")
    model_multi_w.write().overwrite().save("hdfs://namenode:9000/bda/models/rf_multiclass_weighted")

    # ──────────────────────────────────────────────────────────────────────────
    # Model 2: Multi-Class Random Forest (Unweighted Baseline)
    # ──────────────────────────────────────────────────────────────────────────
    print("\n[Step 3] Training Multi-Class Random Forest (Unweighted Baseline)...")
    t0 = time.time()
    rf_multi_unw = RandomForestClassifier(
        featuresCol="features",
        labelCol="label_index",
        numTrees=40,
        maxDepth=8,
        maxBins=32,
        seed=42,
    )
    pipe_multi_unw = Pipeline(stages=[assembler, rf_multi_unw])
    model_multi_unw = pipe_multi_unw.fit(train_strat)
    fit_time_unw = round(time.time() - t0, 2)
    print(f"  Unweighted Multi-Class RF trained in {fit_time_unw}s")

    preds_strat_unw = model_multi_unw.transform(test_strat)
    metrics_strat_unw = evaluate_predictions(
        preds_strat_unw, label_col="label_index", is_binary=False, label_names=label_names
    )

    preds_temp_unw = model_multi_unw.transform(test_temp)
    metrics_temp_unw = evaluate_predictions(
        preds_temp_unw, label_col="label_index", is_binary=False, label_names=label_names
    )

    multiclass_metrics["RandomForest_Multiclass_Unweighted"] = {
        "hyperparameters": {"numTrees": 50, "maxDepth": 10, "maxBins": 32, "weightCol": None},
        "fit_time_seconds": fit_time_unw,
        "stratified_test": metrics_strat_unw,
        "friday_holdout_test": metrics_temp_unw,
    }
    confusion_matrices["RF_Multiclass_Unweighted_Stratified"] = metrics_strat_unw["confusion_matrix"]
    confusion_matrices["RF_Multiclass_Unweighted_Friday"] = metrics_temp_unw["confusion_matrix"]

    print(f"  [Stratified Test] Acc: {metrics_strat_unw['accuracy']}, Weighted F1: {metrics_strat_unw['weighted_f1']}, Macro F1: {metrics_strat_unw['macro_f1']}")
    print(f"  [Friday Holdout ] Acc: {metrics_temp_unw['accuracy']}, Weighted F1: {metrics_temp_unw['weighted_f1']}, Macro F1: {metrics_temp_unw['macro_f1']}")

    # ──────────────────────────────────────────────────────────────────────────
    # Rare Class Explicit Comparison
    # ──────────────────────────────────────────────────────────────────────────
    print("\n[Step 4] Explicit Performance Breakdown on Rare Classes (Stratified Test):")
    rare_classes = ["Infiltration", "Heartbleed", "Web_Attack_Sql_Injection", "Web_Attack_XSS", "Web_Attack_Brute_Force"]
    print(f"{'Class':<26} | {'Unweighted F1':<14} | {'Weighted F1':<14} | {'Support':<8}")
    print("-" * 70)
    for rc in rare_classes:
        unw_f1 = metrics_strat_unw["per_class"].get(rc, {}).get("f1_score", 0.0)
        w_f1 = metrics_strat_w["per_class"].get(rc, {}).get("f1_score", 0.0)
        sup = metrics_strat_w["per_class"].get(rc, {}).get("support", 0)
        print(f"{rc:<26} | {unw_f1:<14.4f} | {w_f1:<14.4f} | {sup:<8d}")

    # Save artifacts
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    metrics_path = MODELS_DIR / "metrics_multiclass.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(multiclass_metrics, f, indent=2)
    print(f"\n  Multi-class metrics saved to {metrics_path}")

    cm_path = MODELS_DIR / "confusion_matrices.json"
    existing_cm = {}
    if cm_path.exists():
        try:
            with open(cm_path, "r", encoding="utf-8") as f:
                existing_cm = json.load(f)
        except Exception:
            existing_cm = {}
    existing_cm.update(confusion_matrices)
    with open(cm_path, "w", encoding="utf-8") as f:
        json.dump(existing_cm, f, indent=2)
    print(f"  Confusion matrices updated in {cm_path}")

    print(f"\nMulti-Class Training Finished in {time.time() - t_start:.2f}s")
    spark.stop()

if __name__ == "__main__":
    main()
