# Rubric traceability (current verified state)

| Requirement | Implementation / evidence | Verification | Status |
|---|---|---|---|
| Distributed HDFS + Spark | Compose services, phase reports, Spark submissions | `docker ps`; `hdfs fsck /bda` | Present; scope bounded |
| MongoDB query layer | `src/mongo/queries.py`, `src/mongo/benchmark_queries.py` | `scripts/smoke_test.ps1`; `results/mongo_query_benchmarks.csv` | Indexes verified; collection has 0 flow documents; timings are empty-collection smoke results |
| Streamlit dashboard | `app/app.py`, Compose `app` service | `scripts/smoke_test.ps1` | Local health HTTP 200; no public HTTPS URL verified |
| Data profiling / EDA | Phase 2 CSV and PNG artifacts | `D:\bda\_meta\profile\EDA` | Bounded outputs documented |
| Features / preprocessing | Phase 3 report | HDFS `/bda/features/phase3_v1` | Not completed |
| ML models / evaluation | `src/models/task4_quick_train.py`, `docs/model_comparison.md` | HDFS `/bda/models/task4_20261027_quick1` and result/prediction paths | Three reduced-sample models and weighted metrics saved; AUC/per-class/confusion outputs incomplete |
| Public HTTPS hosting | No tunnel executable/token found | Requires external host or tunnel account | Not deployed |
| Disk safeguards | `scripts/disk_guard.ps1` | `scripts/smoke_test.ps1` | Passed, D: 72.37 GB free |
