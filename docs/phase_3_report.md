# Phase 3 execution status

## Scope and safety

| Item | Verified fact |
|---|---|
| Scope attempted | Reduced CIC flow sample (10% sample then 2,000-row cap) plus at most 2,000 rows from existing 169,017,675-byte packet slice; no download and no full packet files |
| D: free before Spark | 72.36 GB; guard threshold 12 GB |
| HDFS `/bda` before Spark | 23,571,638,444 B logical; 24,431,596,108 B physical |
| Cluster | Existing `spark-master`, two workers and HDFS services reported healthy before run |
| Models trained | No |

## Work and results

- Added `src/features/phase3_bounded.py`, intended to sample CIC flow and packet data, derive a small numeric feature vector, derive SQL-based 16-bin byte frequencies/entropy, fit Imputer → VectorAssembler → StandardScaler on the train split, and write versioned feature/metadata/model paths.
- Added a leakage-name guard and `tests/test_feature_vector_leakage.py` covering forbidden identifier, label, and split names.
- Added `scripts/benchmark_scaling.py` as a time-budgeted cluster benchmark runner; no benchmark configuration was run.
- Added the proposed CIC/UNSW mapping in `docs/feature_dictionary.md`.
- The job was submitted using `docker exec app /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=2 --conf spark.executor.memory=600m --conf spark.local.dir=/spark-tmp /tmp/phase3_bounded.py`.
- First attempt stopped at a Hadoop path-existence-check exception before reading data. Second attempt connected to Spark 3.5.3 and obtained two executors, but remained active past the 5-minute deadline without output. It was terminated. Therefore no measured row count, feature dimension, runtime, Parquet, fitted preprocessing model, or benchmark result is claimed.
- HDFS listing after stop showed only existing `/bda/features/flow_features_full`; `/bda/features/phase3_v1` was absent. No verified `/bda/models/preprocess/phase3_v1` output was observed.

## Validation and incomplete requirements

- `python -m py_compile src/features/phase3_bounded.py` passed before submission.
- `git diff --check` passed (line-ending warnings only).
- Unit tests were added but not run. Feature job completion, output readability, label/split preservation, packet aggregates, UNSW feature output, pipeline persistence, replication 2, and benchmark CSV are unverified/incomplete.
- Packet timestamps are absent from the documented packet schema, so packet inter-arrival features cannot be computed from that source. Payload-derived statistics require a completed job and source-field confirmation.
- Next safe action: correct/diagnose the bounded job using its full Spark driver error, rerun disk guard, then run and independently verify outputs before treating Task 3 as complete. Do not train classifiers or start Task 4.
