# Verified results index

No new Task 5 benchmarks or MongoDB ingestion counts are claimed.

| Result | Measured value / artifact |
|---|---|
| Task 1 verified source bytes | 22,542,663,105 bytes; see `docs/phase_1_report.md` for file checksums and scope |
| Phase 2 bounded EDA | 10,000 rows per profiled source; see `docs/phase_2_report.md` and CSVs in `D:\bda\logs\profile\phase2_v1\csv` |
| EDA PNGs | `docs/figures/` copied from `D:\bda\_meta\profile\EDA` during Task 5 |
| HDFS last recorded | 23,571,638,444 B logical; 24,431,596,108 B physical (disk guard snapshot) |
| Task 3 features | No verified Phase 3 versioned output; see `docs/phase_3_report.md` |
| Task 4 metrics | No verified result at last report; check current Spark run/HDFS output before adding metrics |
| MongoDB query timings | Not measured; no benchmark CSV is represented as completed |

## Subsequent verified Task 4 / Task 5 observations

Task 4's reduced Spark run later completed: 1,799 train rows, 767 test rows, 76 inputs; binary RF accuracy 0.938722 / weighted F1 0.934155, binary GBT accuracy 0.930900 / weighted F1 0.927996, multiclass RF accuracy 0.908616 / weighted F1 0.901720. Full table and limitations: `docs/model_comparison.md`. HDFS output directories were listed after the job. The macro-F1 field is mislabeled in raw output and is not reported as macro.

MongoDB query timings in `results/mongo_query_benchmarks.csv` are live timings against an empty `bda.flows` collection (0 documents); they are smoke measurements only, not representative query benchmarks.
