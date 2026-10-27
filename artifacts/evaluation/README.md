# Task 4 evaluation outputs

Source: saved held-out predictions in HDFS for task4_20261027_quick1. The evaluator reads those predictions and saved StringIndexer mappings; it does not retrain models.

## Files

- model_metrics.csv — one row per model with accuracy, balanced accuracy, micro/macro/weighted precision, recall, and F1, ROC-AUC and PR-AUC where applicable, log loss, Brier score, MCC, Cohen’s kappa, and binary sensitivity/specificity/FPR/FNR.
- per_class_metrics.csv — class support, precision, recall, F1, specificity, one-vs-rest ROC-AUC, and average precision.
- confusion_matrices.json — labeled matrices for each model.
- evaluation_metrics.json — all scalar metrics plus matrices and ROC/precision-recall curve coordinates.
- confusion_matrix_*.html, roc_curve_*.html, pr_curve_*.html — interactive charts for all three models.

## Summary (reduced seeded sample)

| Task / model | Test rows | Accuracy | Macro precision | Macro recall | Macro F1 | ROC-AUC |
|---|---:|---:|---:|---:|---:|---:|
| Binary Random Forest | 767 | 0.9387 | 0.9646 | 0.8433 | 0.8888 | 0.9765 |
| Binary Gradient-Boosted Trees | 767 | 0.9309 | 0.9187 | 0.8536 | 0.8812 | 0.9081 |
| Multiclass Random Forest | 766 | 0.9086 | 0.7153 | 0.7326 | 0.7139 | 0.9939 macro one-vs-rest |

### Binary confusion matrices

Rows are actual classes; columns are predicted classes. Class order is ATTACK, BENIGN.

**Binary Random Forest**

|  | Predicted ATTACK | Predicted BENIGN |
|---|---:|---:|
| Actual ATTACK | 617 | 0 |
| Actual BENIGN | 47 | 103 |

**Binary Gradient-Boosted Trees**

|  | Predicted ATTACK | Predicted BENIGN |
|---|---:|---:|
| Actual ATTACK | 605 | 12 |
| Actual BENIGN | 41 | 109 |

## Limits

These results come from the saved reduced sample (seed 20261027), not full-dataset evaluation. The multiclass model has 12 classes in its fitted sample; rare classes absent from that sample are not evaluated. Binary positive class is ATTACK; multiclass AUC/PR curves use one-vs-rest. See docs/phase_4_report.md.
