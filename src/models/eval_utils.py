"""
eval_utils.py - Model evaluation utilities for Spark MLlib classifiers.
Computes accuracy, weighted & macro precision/recall/F1, ROC-AUC, PR-AUC,
per-class metrics, and confusion matrix.
"""

from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator,
    BinaryClassificationEvaluator,
)

def evaluate_predictions(
    predictions: DataFrame,
    label_col: str = "label",
    prediction_col: str = "prediction",
    probability_col: str = "probability",
    is_binary: bool = True,
    label_names: List[str] = None,
) -> Dict[str, Any]:
    """
    Evaluates predictions DataFrame and returns a comprehensive metrics dictionary.
    """
    # Multiclass evaluators
    eval_acc = MulticlassClassificationEvaluator(
        labelCol=label_col, predictionCol=prediction_col, metricName="accuracy"
    )
    eval_f1_weighted = MulticlassClassificationEvaluator(
        labelCol=label_col, predictionCol=prediction_col, metricName="weightedFMeasure"
    )
    eval_prec_weighted = MulticlassClassificationEvaluator(
        labelCol=label_col, predictionCol=prediction_col, metricName="weightedPrecision"
    )
    eval_rec_weighted = MulticlassClassificationEvaluator(
        labelCol=label_col, predictionCol=prediction_col, metricName="weightedRecall"
    )

    acc = eval_acc.evaluate(predictions)
    f1_w = eval_f1_weighted.evaluate(predictions)
    prec_w = eval_prec_weighted.evaluate(predictions)
    rec_w = eval_rec_weighted.evaluate(predictions)

    results = {
        "accuracy": round(float(acc), 4),
        "weighted_precision": round(float(prec_w), 4),
        "weighted_recall": round(float(rec_w), 4),
        "weighted_f1": round(float(f1_w), 4),
    }

    # Binary specific metrics: ROC-AUC and PR-AUC
    if is_binary and probability_col in predictions.columns:
        try:
            eval_roc = BinaryClassificationEvaluator(
                labelCol=label_col, rawPredictionCol=probability_col, metricName="areaUnderROC"
            )
            eval_pr = BinaryClassificationEvaluator(
                labelCol=label_col, rawPredictionCol=probability_col, metricName="areaUnderPR"
            )
            results["roc_auc"] = round(float(eval_roc.evaluate(predictions)), 4)
            results["pr_auc"] = round(float(eval_pr.evaluate(predictions)), 4)
        except Exception as e:
            results["roc_auc"] = 0.0
            results["pr_auc"] = 0.0

    # Per-class metrics and confusion matrix via Spark aggregation
    cm_pairs = (
        predictions.groupBy(label_col, prediction_col)
        .count()
        .collect()
    )

    # Distinct labels
    distinct_labels = sorted(list(set([int(r[label_col]) for r in cm_pairs] + [int(r[prediction_col]) for r in cm_pairs])))
    
    # Map label indices to names if provided
    def get_name(idx):
        if label_names and 0 <= idx < len(label_names):
            return label_names[idx]
        return str(idx)

    # Build confusion matrix dictionary
    cm_dict = {}
    for actual in distinct_labels:
        cm_dict[get_name(actual)] = {get_name(pred): 0 for pred in distinct_labels}
    
    total_samples = 0
    for r in cm_pairs:
        act = int(r[label_col])
        pred = int(r[prediction_col])
        cnt = int(r["count"])
        total_samples += cnt
        cm_dict[get_name(act)][get_name(pred)] = cnt

    # Per-class precision, recall, F1, and support
    per_class = {}
    f1_list = []
    
    for actual in distinct_labels:
        act_name = get_name(actual)
        tp = cm_dict[act_name].get(act_name, 0)
        support = sum(cm_dict[act_name].values())
        pred_total = sum(cm_dict[get_name(other_act)].get(act_name, 0) for other_act in distinct_labels)
        
        prec = (tp / pred_total) if pred_total > 0 else 0.0
        rec = (tp / support) if support > 0 else 0.0
        f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) > 0 else 0.0
        
        per_class[act_name] = {
            "precision": round(float(prec), 4),
            "recall": round(float(rec), 4),
            "f1_score": round(float(f1), 4),
            "support": int(support),
        }
        f1_list.append(f1)

    macro_f1 = sum(f1_list) / len(f1_list) if f1_list else 0.0
    results["macro_f1"] = round(float(macro_f1), 4)
    results["per_class"] = per_class
    results["confusion_matrix"] = cm_dict

    return results
