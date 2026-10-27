"""
train_binary.py - Distributed binary classification with Spark MLlib.
Trains and compares RandomForest and GBT classifiers with inverse-frequency class weights,
an unweighted baseline, and majority-class undersampling.
Evaluates on both Stratified and Friday Hold-out test sets.
Executes feature ablation and extracts top 20 feature importances.
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
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier

from eval_utils import evaluate_predictions

ARTIFACTS_DIR = Path("/opt/spark/work-dir/artifacts")
MODELS_DIR = ARTIFACTS_DIR / "models"
TIMINGS_DIR = ARTIFACTS_DIR / "timings"

def main():
    t_start = time.time()
    spark = (
        SparkSession.builder
        .appName("BDA-Phase4-TrainBinary")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1100m")
        .config("spark.cores.max", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("======================================================================")
    print("PHASE 4: Distributed Binary Model Training (BENIGN vs ATTACK)")
    print("======================================================================")

    # 1. Load Feature Catalog
    catalog_path = ARTIFACTS_DIR / "features" / "feature_catalog.csv"
    with open(catalog_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        features_meta = list(reader)

    full_features = [r["feature_name"] for r in features_meta if r["group"] in ("flow", "packet", "payload", "metadata_flag")]
    flow_features = [r["feature_name"] for r in features_meta if r["group"] == "flow"]
    packet_features = [r["feature_name"] for r in features_meta if r["group"] == "packet"]
    payload_features = [r["feature_name"] for r in features_meta if r["group"] == "payload"]
    flow_packet_features = flow_features + packet_features

    print(f"Total Model Features: {len(full_features)} (Flow: {len(flow_features)}, Packet: {len(packet_features)}, Payload: {len(payload_features)})")

    # 2. Load Features Table
    print("\n[Step 1] Loading full feature table from HDFS...")
    t0 = time.time()
    df_all = spark.read.parquet("hdfs://namenode:9000/bda/features/flow_features_full/")
    total_records = df_all.count()
    print(f"  Loaded {total_records:,} rows in {time.time() - t0:.2f}s")

    # Splits
    train_strat = df_all.filter(F.col("split_stratified") == "train")
    test_strat = df_all.filter(F.col("split_stratified") == "test")
    train_temp = df_all.filter(F.col("split_temporal") == "train")
    test_temp = df_all.filter(F.col("split_temporal") == "test")

    print(f"  Stratified Split -> Train: {train_strat.count():,}, Test: {test_strat.count():,}")
    print(f"  Temporal Split   -> Train: {train_temp.count():,}, Test (Friday): {test_temp.count():,}")

    # Compute Class Weights on Stratified Train
    n_train = train_strat.count()
    n_benign = train_strat.filter(F.col("is_attack") == 0).count()
    n_attack = train_strat.filter(F.col("is_attack") == 1).count()
    w_benign = n_train / (2.0 * n_benign)
    w_attack = n_train / (2.0 * n_attack)
    print(f"\n  Binary Class Balance: BENIGN={n_benign:,} (w={w_benign:.4f}), ATTACK={n_attack:,} (w={w_attack:.4f})")

    weight_expr = F.when(F.col("is_attack") == 0, w_benign).otherwise(w_attack)
    train_strat_weighted = train_strat.withColumn("weight", weight_expr)

    # 3. Model Training & Evaluation Dictionary
    binary_metrics = {}
    confusion_matrices = {}

    assembler = VectorAssembler(inputCols=full_features, outputCol="features")

    # ──────────────────────────────────────────────────────────────────────────
    # Model 1: Random Forest (Weighted)
    # ──────────────────────────────────────────────────────────────────────────
    print("\n[Step 2] Training Model 1: RandomForest (Weighted, numTrees=50, maxDepth=10)...")
    t0 = time.time()
    rf_weighted = RandomForestClassifier(
        featuresCol="features",
        labelCol="is_attack",
        weightCol="weight",
        numTrees=50,
        maxDepth=10,
        maxBins=32,
        seed=42,
    )
    pipeline_rf_w = Pipeline(stages=[assembler, rf_weighted])
    model_rf_w = pipeline_rf_w.fit(train_strat_weighted)
    rf_w_fit_time = round(time.time() - t0, 2)
    print(f"  RandomForest (Weighted) trained in {rf_w_fit_time}s")

    # Evaluate on Stratified Test
    preds_strat = model_rf_w.transform(test_strat)
    metrics_rf_strat = evaluate_predictions(preds_strat, label_col="is_attack", is_binary=True, label_names=["BENIGN", "ATTACK"])

    # Evaluate on Friday Hold-out Test
    preds_temp = model_rf_w.transform(test_temp)
    metrics_rf_temp = evaluate_predictions(preds_temp, label_col="is_attack", is_binary=True, label_names=["BENIGN", "ATTACK"])

    binary_metrics["RandomForest_Weighted"] = {
        "hyperparameters": {"numTrees": 50, "maxDepth": 10, "maxBins": 32, "weightCol": "inverse_frequency"},
        "fit_time_seconds": rf_w_fit_time,
        "stratified_test": metrics_rf_strat,
        "friday_holdout_test": metrics_rf_temp,
    }
    confusion_matrices["RF_Binary_Weighted_Stratified"] = metrics_rf_strat["confusion_matrix"]
    confusion_matrices["RF_Binary_Weighted_Friday"] = metrics_rf_temp["confusion_matrix"]

    print(f"  [Stratified Test] Acc: {metrics_rf_strat['accuracy']}, F1: {metrics_rf_strat['weighted_f1']}, Macro F1: {metrics_rf_strat['macro_f1']}, ROC-AUC: {metrics_rf_strat['roc_auc']}")
    print(f"  [Friday Holdout ] Acc: {metrics_rf_temp['accuracy']}, F1: {metrics_rf_temp['weighted_f1']}, Macro F1: {metrics_rf_temp['macro_f1']}, ROC-AUC: {metrics_rf_temp['roc_auc']}")

    # Save RF Model to HDFS
    print("  Saving RandomForest model to HDFS /bda/models/rf_binary_weighted ...")
    model_rf_w.write().overwrite().save("hdfs://namenode:9000/bda/models/rf_binary_weighted")

    # ──────────────────────────────────────────────────────────────────────────
    # Model 2: GBTClassifier (Weighted)
    # ──────────────────────────────────────────────────────────────────────────
    print("\n[Step 3] Training Model 2: GBTClassifier (Weighted, maxIter=25, maxDepth=6)...")
    t0 = time.time()
    gbt_weighted = GBTClassifier(
        featuresCol="features",
        labelCol="is_attack",
        weightCol="weight",
        maxIter=25,
        maxDepth=6,
        maxBins=32,
        seed=42,
    )
    pipeline_gbt_w = Pipeline(stages=[assembler, gbt_weighted])
    model_gbt_w = pipeline_gbt_w.fit(train_strat_weighted)
    gbt_w_fit_time = round(time.time() - t0, 2)
    print(f"  GBTClassifier (Weighted) trained in {gbt_w_fit_time}s")

    preds_gbt_strat = model_gbt_w.transform(test_strat)
    metrics_gbt_strat = evaluate_predictions(preds_gbt_strat, label_col="is_attack", is_binary=True, label_names=["BENIGN", "ATTACK"])

    preds_gbt_temp = model_gbt_w.transform(test_temp)
    metrics_gbt_temp = evaluate_predictions(preds_gbt_temp, label_col="is_attack", is_binary=True, label_names=["BENIGN", "ATTACK"])

    binary_metrics["GBT_Weighted"] = {
        "hyperparameters": {"maxIter": 25, "maxDepth": 6, "maxBins": 32, "weightCol": "inverse_frequency"},
        "fit_time_seconds": gbt_w_fit_time,
        "stratified_test": metrics_gbt_strat,
        "friday_holdout_test": metrics_gbt_temp,
    }
    confusion_matrices["GBT_Binary_Weighted_Stratified"] = metrics_gbt_strat["confusion_matrix"]
    confusion_matrices["GBT_Binary_Weighted_Friday"] = metrics_gbt_temp["confusion_matrix"]

    print(f"  [Stratified Test] Acc: {metrics_gbt_strat['accuracy']}, F1: {metrics_gbt_strat['weighted_f1']}, Macro F1: {metrics_gbt_strat['macro_f1']}, ROC-AUC: {metrics_gbt_strat['roc_auc']}")
    print(f"  [Friday Holdout ] Acc: {metrics_gbt_temp['accuracy']}, F1: {metrics_gbt_temp['weighted_f1']}, Macro F1: {metrics_gbt_temp['macro_f1']}, ROC-AUC: {metrics_gbt_temp['roc_auc']}")

    print("  Saving GBT model to HDFS /bda/models/gbt_binary_weighted ...")
    model_gbt_w.write().overwrite().save("hdfs://namenode:9000/bda/models/gbt_binary_weighted")

    # ──────────────────────────────────────────────────────────────────────────
    # Model 3: Random Forest (Undersampled Majority Class)
    # ──────────────────────────────────────────────────────────────────────────
    print("\n[Step 4] Training Model 3: RandomForest (Undersampled Majority Class)...")
    t0 = time.time()
    undersample_ratio = n_attack / max(n_benign, 1)
    df_benign_under = train_strat.filter(F.col("is_attack") == 0).sample(fraction=undersample_ratio, seed=42)
    df_attack_under = train_strat.filter(F.col("is_attack") == 1)
    df_train_undersampled = df_benign_under.unionByName(df_attack_under)
    print(f"  Undersampled Dataset: {df_train_undersampled.count():,} rows (~1:1 ratio)")

    rf_under = RandomForestClassifier(
        featuresCol="features",
        labelCol="is_attack",
        numTrees=50,
        maxDepth=10,
        maxBins=32,
        seed=42,
    )
    pipeline_rf_under = Pipeline(stages=[assembler, rf_under])
    model_rf_under = pipeline_rf_under.fit(df_train_undersampled)
    rf_under_fit_time = round(time.time() - t0, 2)

    preds_under_strat = model_rf_under.transform(test_strat)
    metrics_under_strat = evaluate_predictions(preds_under_strat, label_col="is_attack", is_binary=True, label_names=["BENIGN", "ATTACK"])

    preds_under_temp = model_rf_under.transform(test_temp)
    metrics_under_temp = evaluate_predictions(preds_under_temp, label_col="is_attack", is_binary=True, label_names=["BENIGN", "ATTACK"])

    binary_metrics["RandomForest_Undersampled"] = {
        "hyperparameters": {"numTrees": 50, "maxDepth": 10, "maxBins": 32, "sampling": "majority_undersampling_1:1"},
        "fit_time_seconds": rf_under_fit_time,
        "stratified_test": metrics_under_strat,
        "friday_holdout_test": metrics_under_temp,
    }
    confusion_matrices["RF_Binary_Undersampled_Stratified"] = metrics_under_strat["confusion_matrix"]
    confusion_matrices["RF_Binary_Undersampled_Friday"] = metrics_under_temp["confusion_matrix"]

    # ──────────────────────────────────────────────────────────────────────────
    # Model 4: Random Forest (Unweighted Baseline)
    # ──────────────────────────────────────────────────────────────────────────
    print("\n[Step 5] Training Model 4: RandomForest (Unweighted Baseline)...")
    t0 = time.time()
    rf_unweighted = RandomForestClassifier(
        featuresCol="features",
        labelCol="is_attack",
        numTrees=50,
        maxDepth=10,
        maxBins=32,
        seed=42,
    )
    pipeline_rf_unw = Pipeline(stages=[assembler, rf_unweighted])
    model_rf_unw = pipeline_rf_unw.fit(train_strat)
    rf_unw_fit_time = round(time.time() - t0, 2)

    preds_unw_strat = model_rf_unw.transform(test_strat)
    metrics_unw_strat = evaluate_predictions(preds_unw_strat, label_col="is_attack", is_binary=True, label_names=["BENIGN", "ATTACK"])

    preds_unw_temp = model_rf_unw.transform(test_temp)
    metrics_unw_temp = evaluate_predictions(preds_unw_temp, label_col="is_attack", is_binary=True, label_names=["BENIGN", "ATTACK"])

    binary_metrics["RandomForest_Unweighted"] = {
        "hyperparameters": {"numTrees": 50, "maxDepth": 10, "maxBins": 32, "weightCol": None},
        "fit_time_seconds": rf_unw_fit_time,
        "stratified_test": metrics_unw_strat,
        "friday_holdout_test": metrics_unw_temp,
    }
    confusion_matrices["RF_Binary_Unweighted_Stratified"] = metrics_unw_strat["confusion_matrix"]
    confusion_matrices["RF_Binary_Unweighted_Friday"] = metrics_unw_temp["confusion_matrix"]

    # ──────────────────────────────────────────────────────────────────────────
    # Step 4 Feature Importances (Top 20 from Weighted Random Forest)
    # ──────────────────────────────────────────────────────────────────────────
    print("\n[Step 6] Extracting Top 20 Feature Importances from Random Forest...")
    rf_model_stage = model_rf_w.stages[-1]
    importances = rf_model_stage.featureImportances.toArray()

    feat_meta_lookup = {r["feature_name"]: r["group"] for r in features_meta}
    ranked_feats = []
    for idx, imp in enumerate(importances):
        fname = full_features[idx]
        ranked_feats.append({
            "rank": 0,
            "feature_name": fname,
            "group": feat_meta_lookup.get(fname, "unknown"),
            "importance": round(float(imp), 6)
        })
    ranked_feats.sort(key=lambda x: x["importance"], reverse=True)
    top20 = ranked_feats[:20]
    for r_idx, row in enumerate(top20, 1):
        row["rank"] = r_idx

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    feat_imp_path = MODELS_DIR / "feature_importance.csv"
    with open(feat_imp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["rank", "feature_name", "group", "importance"])
        writer.writeheader()
        writer.writerows(top20)
    print(f"  Top 20 feature importances saved to {feat_imp_path}")
    for r in top20[:5]:
        print(f"    Rank {r['rank']}: {r['feature_name']} ({r['group']}) = {r['importance']}")

    # ──────────────────────────────────────────────────────────────────────────
    # Step 4 Ablation Study: Flow vs Flow+Packet vs Flow+Packet+Payload
    # ──────────────────────────────────────────────────────────────────────────
    print("\n[Step 7] Running Feature Group Ablation Study for Binary RF...")
    ablation_results = []

    ablation_sets = [
        ("Flow_Only", flow_features),
        ("Flow_Packet", flow_packet_features),
        ("Flow_Packet_Payload", full_features),
    ]

    for set_name, feature_subset in ablation_sets:
        t0 = time.time()
        asm = VectorAssembler(inputCols=feature_subset, outputCol="features")
        rf_ab = RandomForestClassifier(
            featuresCol="features",
            labelCol="is_attack",
            weightCol="weight",
            numTrees=50,
            maxDepth=10,
            maxBins=32,
            seed=42
        )
        pipe_ab = Pipeline(stages=[asm, rf_ab])
        mod_ab = pipe_ab.fit(train_strat_weighted)
        train_time = round(time.time() - t0, 2)

        # Eval Stratified
        p_strat = mod_ab.transform(test_strat)
        m_strat = evaluate_predictions(p_strat, label_col="is_attack", is_binary=True)

        # Eval Friday
        p_temp = mod_ab.transform(test_temp)
        m_temp = evaluate_predictions(p_temp, label_col="is_attack", is_binary=True)

        row_strat = {
            "feature_set": set_name,
            "feature_count": len(feature_subset),
            "test_split": "stratified_80_20",
            "train_time_seconds": train_time,
            "accuracy": m_strat["accuracy"],
            "weighted_f1": m_strat["weighted_f1"],
            "macro_f1": m_strat["macro_f1"],
            "roc_auc": m_strat.get("roc_auc", 0.0),
            "pr_auc": m_strat.get("pr_auc", 0.0),
        }
        row_temp = {
            "feature_set": set_name,
            "feature_count": len(feature_subset),
            "test_split": "friday_holdout",
            "train_time_seconds": train_time,
            "accuracy": m_temp["accuracy"],
            "weighted_f1": m_temp["weighted_f1"],
            "macro_f1": m_temp["macro_f1"],
            "roc_auc": m_temp.get("roc_auc", 0.0),
            "pr_auc": m_temp.get("pr_auc", 0.0),
        }
        ablation_results.extend([row_strat, row_temp])
        print(f"  {set_name} ({len(feature_subset)} feats) -> Strat F1: {m_strat['weighted_f1']}, Friday F1: {m_temp['weighted_f1']}")

    ablation_path = MODELS_DIR / "ablation.csv"
    with open(ablation_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["feature_set", "feature_count", "test_split", "train_time_seconds", "accuracy", "weighted_f1", "macro_f1", "roc_auc", "pr_auc"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(ablation_results)
    print(f"  Ablation results saved to {ablation_path}")

    # Save metrics JSON
    metrics_path = MODELS_DIR / "metrics_binary.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(binary_metrics, f, indent=2)
    print(f"  Binary metrics saved to {metrics_path}")

    # Update confusion matrices JSON
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
    print(f"  Confusion matrices saved to {cm_path}")

    print(f"\nBinary Model Training Finished in {time.time() - t_start:.2f}s")
    spark.stop()

if __name__ == "__main__":
    main()
