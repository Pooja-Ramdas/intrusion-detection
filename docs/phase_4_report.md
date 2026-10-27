# Phase 4 execution status

## Verified input and safety

| Item | Observed |
|---|---|
| Input | Existing `/bda/features/flow_features_full` Parquet; it includes `label_multi`, `label_binary`, `is_attack`, `split_temporal`, and `split_stratified` |
| Model-safe feature filter | Excludes IDs, IPs, ports, timestamps, label/split names, `is_attack`, and packet-presence flag; first attempt exposed `is_attack` and was stopped before fitting; it was then excluded |
| D: free before training attempt | 72.36 GB; guard floor 12 GB passed |
| Training output | None verified |

## Attempt and limitations

- Added `src/models/task4_quick_train.py` for seeded, capped binary RF, binary GBT, and multiclass RF experiments using existing split assignments, with output paths under `/bda/models/task4_20261027_quick1`, `/bda/_meta/results/task4_20261027_quick1`, and `/bda/results/predictions/task4_20261027_quick1`.
- Submitted with `docker exec app /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.memory=600m --conf spark.local.dir=/spark-tmp /tmp/task4_quick_train.py` after `scripts/disk_guard.ps1`.
- Spark 3.5.3 connected to the existing cluster and printed the source schema and safe candidate feature names. The per-class row-capping stage did not finish by the deadline; the process was stopped. No model fitting, evaluation, persistence, prediction output, reload check, or metric generation completed.
- HDFS checks after stopping found no `/bda/models/task4_20261027_quick1`, `/bda/_meta/results/task4_20261027_quick1`, or `/bda/results/predictions/task4_20261027_quick1` output. No scores are reported.
- External UNSW validation, confusion matrices, ROC/PR, per-class metrics, class weighting/undersampling comparison, duplicate check, and shuffled-label control were not run. The prior Phase 3 report confirms the intended `/bda/features/phase3_v1` and fitted preprocessing pipeline were not produced; the run used the preserved legacy feature table only after checking its labels and split columns.

## Files changed

- Added `src/models/task4_quick_train.py`.
- No existing model files or HDFS data were overwritten. Task 5 was not started.

## Next blocker

The first corrected run was also stopped at the request cutoff before training. A subsequent bounded `sampleBy` run completed as recorded below.

## Subsequent bounded run (2026-10-27)

Sampling used 2% per label with seed `20261027`, followed by per-class caps. The first run was stopped before fitting when `is_attack` was found among candidate inputs. The corrected run excludes `is_attack`, label/split fields, identifiers, endpoint IPs/ports, timestamps, and `has_packet_features`. Spark 3.5.3 ran through `spark-submit`; the disk guard passed at 72.37 GB free.

| Model | Train rows | Test rows | Features | Accuracy | Weighted precision | Weighted recall | Weighted F1 | Runtime at log (s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Binary Random Forest | 1,799 | 767 | 76 | 0.938722 | 0.943060 | 0.938722 | 0.934155 | 195.89 |
| Binary GBT | 1,799 | 767 | 76 | 0.930900 | 0.929550 | 0.930900 | 0.927996 | 241.96 |
| Multiclass Random Forest | 1,799 | 766 | 76 | 0.908616 | 0.903562 | 0.908616 | 0.901720 | 277.64 |

Small sampled results only; do not generalize to full data. The generated field `macro_f1` used Spark's default weighted F1 evaluator, so it is mislabeled and must not be used as macro-F1. ROC/PR AUC, per-class metrics, confusion matrices, class-weight/undersampling comparison, shuffled-label control, and UNSW external validation were not measured. Top-10 RF importance was printed in the Spark driver log but not separately persisted.

The job printed `TASK4_DONE`: 1,799 training rows, 767 test rows, 76 features, elapsed 288.54 seconds. It wrote to `/bda/models/task4_20261027_quick1`, `/bda/_meta/results/task4_20261027_quick1`, and `/bda/results/predictions/task4_20261027_quick1`. Independent reload/read-back verification is pending.
