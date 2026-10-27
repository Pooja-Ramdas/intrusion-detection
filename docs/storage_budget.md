# Storage budget — measured current state

## Approved selection — stopped after verified File 13 per user cutoff

The approved planned selection is CIC `Packet-Bytes` files **1, 2, 3, 6, 9, 10, 11, 12, 13, 14, 15, 16, 17**. The full planned selection spans Monday–Friday and all 14 published CIC attack classes, including SSH-Patator and the five required rare classes. File 18 is excluded because the preserved working slice was generated from that source shard. The user cutoff stopped after File 13; File 14 (Infiltration) is not a complete HDFS file, so packet-level coverage is incomplete in the verified snapshot.

| Source set | Bytes | State |
|---|---:|---|
| CIC Packet-Bytes selected files | 30,076,375,901 | Approved total; 22,158,691,332 B verified; 7,917,684,569 B not completed |
| CIC flow file | 369,814,810 | Existing HDFS file; size/SHA-256 independently verified; replication 1 |
| UNSW train + test | 14,156,963 | Existing HDFS files; sizes/SHA-256 independently verified; replication 1 |
| Total selected source files | 30,460,347,674 | Approved scope; 22,542,663,105 B verified including existing flow/UNSW |
| Preserved packet working slice | 169,017,675 | Existing; excluded from source target |
| File 18 | 393,316,834 | Excluded due legacy-slice overlap risk |

The 4.263 GB File 1 uses 512 MB resumable HTTP byte ranges to D: staging, then a verified HDFS partial append. The staging cap remains 4,000,000,000 B. Progress is in `D:\bda\_meta\ingest_log.jsonl`.

Final snapshot 2026-10-27 01:54 UTC: nine packet files (1, 2, 3, 6, 9, 10, 11, 12, 13) are fully ingested and SHA-256 verified, totaling 22,158,691,332 B. File 13 independently matched size 2,232,734,751 B, replication 1, and manifest SHA-256. Existing flow/UNSW files independently matched their sizes and SHA-256, totaling 383,971,773 B. Verified unique source total: 22,542,663,105 B. The user directed ingestion to stop after File 13; Files 14–17 remain uncompleted. File 14 has a preserved 943,718,400 B D: cache partial, excluded from totals. HDFS `/bda`: 23,571,614,709 B logical / 24,431,548,638 B physical; Mongo volume 318.9 MB; staging 943,718,651 B. Current HDFS+Mongo+staging measured/observed sum is about 25.69 GB, below the 70 GB cap. Final guard: D: 72.39 GB free; C: 27.60 GB free. Final fsck HEALTHY; raw 13 files all replication 1; clean/features/models 44 files all replication 2. No downloader remains active.

## Estimated D: peak and hard budget

| Component | Bytes / estimate | Basis |
|---|---:|---|
| Current active HDFS + Mongo | 24,750,448,638 B | Final HDFS physical 24,431,548,638 B plus Mongo 318.9 MB |
| Retained rollback Docker volumes | 2.613 GB | Docker-reported rounded usage; preserved |
| Remaining Packet-Bytes selection | 7,917,684,569 B | Source-file bytes not completed by the user cutoff; raw replication 1 |
| Peak local staging allowance | 4,000,000,000 B | Existing hard cap |
| Peak after remaining source selection | ~39.281 GB | Current HDFS+Mongo, remaining packet source bytes, 4 GB staging allowance, and 2.613 GB preserved rollback volume estimate |
| Remaining to 70 GB physical cap | ~30.719 GB | Budget for new clean/features, Spark temp, Mongo growth, models, and other data; estimates only |

The remaining 30.719 GB is an estimate, not a measured forecast. New processed outputs and scratch sizes are unmeasured; enforce the 12 GB D: free-space gate before each heavy phase and stop if the 70 GB footprint cap would be exceeded. D: free space was 72.39 GB at the final guard; Docker VHDX and WSL distro image remain on D:.

Snapshot: 2026-10-27. Values are current observations; proposal/projections are explicitly separated. Existing flow/UNSW row counts were measured below; proposed packet rows are published metadata.

### Packet-Bytes published per-file metadata (files 1–17)

Counts below are published `nids-datasets` metadata, not local reads of the proposed files. Row totals equal the sum of listed label counts. Selected IDs are 1, 2, 3, 6, 9, 10, 11, 12, 13, 14, 15, 16, 17; other inventory rows are not selected.

| File | Bytes | Rows | Published label counts |
|---|---:|---:|---|
| 1 | 4,262,871,602 | 4,822,905 | BENIGN 4,822,905 |
| 2 | 2,379,512,484 | 3,571,097 | BENIGN 3,571,097 |
| 3 | 2,356,734,269 | 3,558,479 | BENIGN 3,556,526; FTP-Patator 1,953 |
| 4 | 2,363,877,447 | 2,588,707 | BENIGN 2,575,904; FTP-Patator 12,803 |
| 5 | 2,631,339,391 | 3,075,291 | BENIGN 3,007,086; FTP-Patator 68,205 |
| 6 | 1,882,476,553 | 2,705,079 | BENIGN 2,637,797; FTP-Patator 28,602; SSH-Patator 38,680 |
| 7 | 1,928,053,380 | 2,737,246 | BENIGN 2,612,615; SSH-Patator 124,631 |
| 8 | 2,367,342,043 | 2,649,076 | BENIGN 2,649,076 |
| 9 | 2,418,770,036 | 3,015,813 | BENIGN 2,948,428; DoS slowloris 47,519; DoS Slowhttptest 19,866 |
| 10 | 862,724,453 | 1,901,467 | BENIGN 422,225; DoS Slowhttptest 19,675; DoS Hulk 1,459,567 |
| 11 | 3,399,128,755 | 5,359,118 | BENIGN 4,418,585; DoS slowloris 41; DoS Hulk 786,683; Heartbleed 49,296; DoS GoldenEye 104,513 |
| 12 | 2,363,738,429 | 2,663,758 | BENIGN 2,651,388; Web Brute Force 12,370 |
| 13 | 2,232,734,751 | 3,136,779 | BENIGN 3,109,436; Web Brute Force 17,873; XSS 9,344; SQL Injection 126 |
| 14 | 1,958,805,809 | 2,905,004 | BENIGN 2,845,250; Infiltration 59,754 |
| 15 | 2,307,399,740 | 2,558,990 | BENIGN 2,558,990 |
| 16 | 2,238,521,483 | 3,195,638 | BENIGN 3,182,740; Bot 12,853; PortScan 45 |
| 17 | 1,412,957,537 | 3,184,487 | BENIGN 1,920,222; PortScan 321,386; DDoS 942,879 |

Flow/UNSW row and class counts are locally measured from existing HDFS Parquet files with read-only Spark at git revision `04682e812a94f5500637316269bbafaf0343663e`: CIC flow 2,827,677 rows, all 15 CIC classes; UNSW train 82,332 rows and test 175,341 rows, both containing the 10 published `attack_cat` categories. Exact per-file class counts are recorded in `artifacts/ingest_plan.json`. These are observations of the existing files, not published metadata for proposed packet shards.

## Initial host and Docker audit (historical; current free space is in the final snapshot above)

| Item | Measured size / capacity | Location / notes |
|---|---:|---|
| C: free | 27.89 GB | `Get-PSDrive` |
| D: free | 97.86 GB | `scripts/disk_guard.ps1`; 12 GB minimum |
| Docker Desktop WSL data VHDX | 18,416,140,288 B | `D:\DockerDesktopWSL\disk\docker_data.vhdx` |
| Docker Desktop WSL distro VHDX | 109,051,904 B | `D:\DockerDesktopWSL\main\ext4.vhdx` |
| Docker images | 5.826 GB | `docker system df` |
| Docker containers | 536.6 kB | `docker system df` |
| Docker local volumes | 2.613 GB | Five inactive named/anonymous rollback volumes; retained |
| Docker build cache | 2.594 GB | `docker system df`; not pruned |
| Hugging Face cache | 0.67 GB | Existing default path on C:, not moved |
| pip cache | 0 GB | Existing default path on C: |
| npm cache | 0.748 GB | Existing default path on C:, not moved |
| TEMP/TMP directory | 1,160,296,601 B (~1.08 GiB) | Existing default path on C: |

No individual file above 200 MiB was found in the inspected cache/TEMP folders. A full C: filesystem baseline was not established. Host `HF_HOME`, `HF_HUB_CACHE`, `PIP_CACHE_DIR`, and `NPM_CONFIG_CACHE` were unset. Docker data VHDX is on D:. Spark local temp is `/spark-tmp` in containers, bind-mounted to `D:\bda\spark-tmp` by `docker-compose.storage-d.yml`.

## Initial HDFS and MongoDB snapshot (historical; superseded by final snapshot above)

| Dataset / zone | HDFS logical bytes | HDFS physical bytes | Replication | Row count |
|---|---:|---:|---:|---|
| `/bda/raw` total | 552,989,448 | 552,989,448 | 1 | Not measured |
| CIC-IDS2017 flow file | 369,814,810 | Included above | 1 | Not measured |
| CIC-IDS2017 packet working slice | 169,017,675 | Included above | 1 | Not measured |
| UNSW-NB15 train + test | 14,156,963 | Included above | 1 | Not measured |
| `/bda/clean` | 574.2 MiB (rounded CLI display) | 1.1 GiB (rounded CLI display) | 2 | Not measured |
| `/bda/features` | 245.2 MiB (rounded CLI display) | 490.3 MiB (rounded CLI display) | 2 | Not measured |
| `/bda/models` | 746.9 KiB (rounded CLI display) | 1.5 MiB (rounded CLI display) | 2 | Not measured |
| `/bda` total | 1,412,923,377 | 2,272,857,306 | Mixed (raw1, other zones2) | Not measured |
| MongoDB host data files | — | 318,942,755 | Single local bind data dir | Not measured |

Zone values are as displayed by `hdfs dfs -du -h`; they are rounded. Use exact `hdfs dfs -du -s` and disk-guard output for exact current totals. HDFS fsck: HEALTHY, 2 DataNodes, 48 files, 67 blocks, 0 under-replicated, 0 missing, 0 corrupt. The four source-file sizes sum to raw's 552,989,448 logical bytes.

Current measured physical dataset footprint (active HDFS + Mongo bind data) is 2,591,800,061 B. Including retained Docker named-volume rollback copies reported by Docker, 5.205 GB total (rounded); both values are current-state observations and exclude Docker image/build-cache sizes because these are runtime artifacts, not dataset payload. Staging was 0 B.

## Historical 40 GB ingest proposal (superseded by the approved 30 GB selection above)

| Dataset | Proposed source bytes | Cap | Published rows | Notes |
|---|---:|---:|---:|---|
| CIC-IDS2017 packet shards | 16,819,901,400 | 17,000,000,000 | 27,253,778* | 9 whole files, all 5 days, all 5 required rare classes |
| Preserved packet working slice | 169,017,675 | Counted toward packet cap | Not measured | Existing raw data; no overwrite |
| Packet total after proposal | 16,988,919,075 | 17,000,000,000 | Not a local measured count | Leaves 11,080,925 B headroom |
| CIC-IDS2017 flow | 369,814,810 | 3,000,000,000 | Not measured | Entire available flow file; already present |
| UNSW-NB15 train + test | 14,156,963 | 6,000,000,000 | Not measured | Both files; already present |
| Largest staged file | 2,691,287,304 | 4,000,000,000 | — | One-at-a-time staging |

*The sum of published per-file metadata counts; not verified by locally reading the Parquet files. `SSH-Patator` is omitted because the whole-file shard containing it would push packet bytes above the cap when the preserved slice is counted. The proposed manifest retains the whole files containing Infiltration, Heartbleed, SQL Injection, XSS, and Web Brute Force.

## D: data layout

| Path | Purpose |
|---|---|
| `D:\bda\hdfs\nn` | NameNode persistent data |
| `D:\bda\hdfs\dn1`, `dn2` | DataNode persistent blocks |
| `D:\bda\mongo\data`, `D:\bda\mongo\configdb` | MongoDB persistent data/config |
| `D:\bda\spark-tmp` | Spark scratch/local blocks |
| `D:\bda\staging` | One-file download staging, ≤4 GB |
| `D:\bda\models` | Model artifacts |
| `D:\bda\logs` | Runtime and ingestion logs |
| `D:\bda\_meta` | Ingest plan and JSONL ingest log |
| `D:\DockerDesktopWSL\disk\docker_data.vhdx` | Docker Desktop engine disk image |
| `D:\DockerDesktopWSL\main\ext4.vhdx` | Docker Desktop WSL distro disk |
| `D:\wsl\swap.vhdx` | `.wslconfig` swap target; applies after WSL restart |

## Commands used for current measurements

`scripts/disk_guard.ps1`; `docker compose -f docker-compose.yml -f docker-compose.storage-d.yml config --quiet`; `docker compose -f docker-compose.yml -f docker-compose.storage-d.yml ps --all`; `docker system df`; `docker exec namenode hdfs dfs -du -s /bda`; `docker exec namenode hdfs fsck /bda`; PowerShell `Get-PSDrive`, `Get-Item`, and recursive file-size checks on the inspected cache folders. No dataset ingest has run; final acceptance measurements are pending.
