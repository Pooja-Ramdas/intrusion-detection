# Phase 5 implementation and deployment status

## Verified changes

- Replaced the placeholder `app/app.py` with a six-section Streamlit/Plotly dashboard: Overview, Data Profiling, Detection Results, Traffic Analysis, Investigation, and Scalability. Small CSV aggregates are loaded only; no dataset-sized pandas reads are used.
- Compose now mounts the app source, measured Phase 2 profile CSVs, EDA PNGs, and Task 4 summary area read-only. `scripts/run_all.ps1` uses the D:-storage override; `scripts/smoke_test.ps1` checks disk, services, HDFS, MongoDB, and HTTP health.
- Added Mongo query helpers and bounded nested-document conversion (`packet_summaries` capped at 20); added an actual query benchmark CSV. No source records were loaded into MongoDB.
- Copied 13 existing Phase 2 PNG figures into `docs/figures/`.
- The Task 4 JSON metrics output was copied to `D:\bda\_meta\results\task4_20261027_quick1_metrics.json` and repository `artifacts/task4_metrics.json` for the dashboard.

## Measured deployment and MongoDB state

| Check | Result |
|---|---|
| Dashboard local URL | `http://localhost:8501`; Streamlit health returned HTTP 200 after deployment |
| Public HTTPS | Not deployed. No `cloudflared`/`ngrok` executable or corresponding tunnel token was present; external hosting credentials were unavailable |
| MongoDB flows | 0 documents in `bda.flows`; no attack/benign sample was loaded |
| Mongo indexes | `_id_`, `label_idx`, `timestamp_idx`, `ip_pair_idx`, `label_time_idx`, `entropy_idx` verified on the D:-backed DB |
| Mongo query CSV | `results/mongo_query_benchmarks.csv`; five actual query timings, each marked `measured_empty_collection` with 0 results/docs examined; not representative performance benchmarks |
| Disk guard | Last observed D: free 72.37 GB, above 12 GB floor |
| Mongo persistence | `docker inspect` showed `/data/db` bind-mounted to `D:/bda/mongo/data` after applying the D: Compose override |
| HDFS/Spark/Mongo | `scripts/smoke_test.ps1` passed: eight containers running, HDFS root listing and 2 live datanodes, Spark UI HTTP 200, Streamlit health HTTP 200, Mongo ping passed |

## Task 4 measurements displayed

Dashboard Detection Results uses the reduced Task 4 summary; model metrics and limitations are in `docs/model_comparison.md`. Model reload verification, per-class metrics, AUC, confusion matrices, and UNSW validation were not completed. The generated `macro_f1` field is actually weighted F1 and is not shown as macro F1.

## Incomplete / blockers

- MongoDB data ingestion, stratified benign sampling, attack-flow completeness, and 20-packet nested summaries were not executed; only document-conversion and query helpers were implemented. MongoDB row count and sample ratio therefore remain 0 / not applicable.
- Public HTTPS requires a tunnel/hosting provider and user credentials or an installed tunnel client. No public URL is claimed.
- The dashboard is local-only. Investigation searches can return no data because MongoDB contains zero flow documents. Scalability and Mongo benchmark results are absent/empty-collection-only.
- No destructive volume operation was used. Docker Compose was started with the D: overlay to reconnect `/data/db` to the existing D: path; the default named volume was not deleted.

## Commands

`scripts\disk_guard.ps1`; `docker compose -f docker-compose.yml -f docker-compose.storage-d.yml up -d app mongodb spark-master`; `docker exec app python3 /tmp/benchmark_queries.py`; `Invoke-WebRequest http://localhost:8501/_stcore/health`; HDFS model/result/prediction directory listings; Mongo count/index operations. Full system reset and data deletion were not run.
