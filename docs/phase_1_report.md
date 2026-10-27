# Task 1 final snapshot — user cutoff after File 13 (2026-10-27 01:54 UTC)

| Item | Measured / planned fact |
|---|---|
| Selected new files | CIC `Packet_Bytes_File_{1,2,3,6,9,10,11,12,13,14,15,16,17}.parquet`; File 18 excluded due overlap risk |
| Completed source/destination | Hugging Face `rdpahalavan/CIC-IDS2017` revision `eee96b6abc2c4bb621fd67679a4aa24bddc4be6a`, `Packet-Bytes/Packet_Bytes_File_{1,2,3,6,9,10,11,12,13}.parquet` → `/bda/raw/cicids2017_packet/` |
| Git / timestamps | HEAD `04682e812a94f5500637316269bbafaf0343663e`; File 13 logged verified 2026-10-27 01:44:21 UTC; cutoff logged 01:53:59 UTC |
| New packet bytes planned | 30,076,375,901 B in 13 whole files; published metadata covers all days and all 14 CIC attack classes |
| Existing flow + UNSW | 383,971,773 B in three existing files; verify-and-skip; not re-downloaded |
| New packet files complete | 9 of 13 selected: 1, 2, 3, 6, 9, 10, 11, 12, 13 |
| New packet bytes verified | 22,158,691,332 B; nine `uploaded_verified` events, SHA-256 from plan/log |
| Existing CIC flow + UNSW verified | 383,971,773 B; flow/train/test sizes and SHA-256 independently matched plan; replication 1 |
| Actual unique source bytes verified | 22,542,663,105 B (new packet files plus existing flow/UNSW); preserved packet slice excluded |
| File 13 final verification | 2,232,734,751 B, replication 1; independent HDFS SHA-256 `43177632aabc61d1fb95c530904877c843ca582c663f225426e235274362829b` matched plan |
| Unfinished selection | Files 14–17 not ingested. File 14 has a preserved 943,718,400 B D: cache partial, not counted; Files 15–17 untouched |
| Packet class limitation | File 14 (Infiltration source) is incomplete; packet-level Infiltration is not confirmed in this cutoff dataset. |
| Final disk guard | D: 72.39 GB free; HDFS `/bda` logical 23,571,614,709 B, physical 24,431,548,638 B; Mongo volume 318.9 MB; staging 943,718,651 B including metadata |
| Physical footprint snapshot | HDFS physical + Mongo volume + D: staging = about 25.69 GB; below 70 GB cap. HDFS and staging are on D: |
| C: / D: free | C: 27.60 GB, D: 72.39 GB. Download/cache/temp paths point to D:; no dataset payload was directed to C: |
| HDFS health/replication | Final fsck HEALTHY: 2 datanodes, 237 blocks, 0 missing/corrupt/under-replicated/mis-replicated. Raw: 13 files, 0 replication mismatches (all rep1). Clean/features/models: 44 files, 0 mismatches (all rep2). |
| Services/storage | All 8 containers healthy. `docker inspect` confirmed NN, both DN and Mongo bind mounts under `D:\bda`; app mounts logs/models/spark-tmp/staging under `D:\bda`. |
| Run state | Ingestion stopped immediately after verified File 13; no downloader worker remains. User-directed cutoff recorded in `D:\bda\_meta\ingest_log.jsonl`. |

Commands run: `scripts\disk_guard.ps1`; `scripts\ingest_to_hdfs.ps1 -Approved -DataVersion eee96b6abc2c4bb621fd67679a4aa24bddc4be6a -Seed 0`; `docker exec namenode hdfs fsck /bda`; HDFS `-stat`, `-ls`, and independent `-cat | sha256sum` checks for File 13, CIC flow, UNSW train/test; replication audit over `/bda/raw`, `/bda/clean`, `/bda/features`, `/bda/models`; `docker ps`; `docker inspect`; `git diff --check` (PASS). File 11 used six verified byte ranges after the full-file downloader stalled; File 14 download was stopped and its partial preserved. Earlier interrupted File 11 cache (2,222,981,621 B) remains under `D:\bda\_meta\interrupted_hf_cache\20261027T004138Z`, not counted.

Recovery notes: Hadoop `appendToFile` initially rejected an HDFS temporary path as a local source; the range uploader now streams the verified chunk with `hdfs dfs -cat | hdfs dfs -appendToFile -`. Hugging Face retained repository subfolders in local staging; verified files were moved within D: to the uploader’s expected path. No destination was overwritten.

### Verified file sizes and SHA-256

Packet hashes are `uploaded_verified` records comparing the full HDFS file to the approved source SHA-256. CIC flow and UNSW hashes were independently recomputed from HDFS in the final check.

| HDFS path | Bytes | SHA-256 |
|---|---:|---|
| `/bda/raw/cicids2017_packet/Packet_Bytes_File_1.parquet` | 4,262,871,602 | `d1ad8847c528c679d267e8c70a5dd59c902f037161c28c42b5e5b4697fd7ebe2` |
| `/bda/raw/cicids2017_packet/Packet_Bytes_File_2.parquet` | 2,379,512,484 | `7a02bc067ee7da8aca8ade572ca4b1a8f17ee732919c929b29c1db4b11c9d481` |
| `/bda/raw/cicids2017_packet/Packet_Bytes_File_3.parquet` | 2,356,734,269 | `8a377b66d73f70443b326e451b61f50f4b4c929154a7fd65aceea747d2c9dc18` |
| `/bda/raw/cicids2017_packet/Packet_Bytes_File_6.parquet` | 1,882,476,553 | `c4451e7757f074fabe8cb15e4dab83ba6d3b1c58177b7e9a979b919e62b20137` |
| `/bda/raw/cicids2017_packet/Packet_Bytes_File_9.parquet` | 2,418,770,036 | `673151271d4d37b9d62eafc399cf2b2f7e6dd1e164e8b89b90a98c4ac39b47f0` |
| `/bda/raw/cicids2017_packet/Packet_Bytes_File_10.parquet` | 862,724,453 | `d18f2fdb1ae688696e31cce616380f0d25f7a8285a19d1d5809dc9e5b60b33f4` |
| `/bda/raw/cicids2017_packet/Packet_Bytes_File_11.parquet` | 3,399,128,755 | `ab4c99b09e8969125c7f5147d2850cc0c0e265b14279ad2c6a7d6829cf139b16` |
| `/bda/raw/cicids2017_packet/Packet_Bytes_File_12.parquet` | 2,363,738,429 | `576770ab4c40e981d22754a163d429664f28edd2eaa275dd64e752b22a3f6860` |
| `/bda/raw/cicids2017_packet/Packet_Bytes_File_13.parquet` | 2,232,734,751 | `43177632aabc61d1fb95c530904877c843ca582c663f225426e235274362829b` |
| `/bda/raw/cicids2017_flow/CICIDS_Flow.parquet` | 369,814,810 | `e64f6adfbcb7f96b018cf36c731be64ef4de3353ccd7340b190590734a0dda35` |
| `/bda/raw/unswnb15/train.parquet` | 4,538,929 | `a0270aeb2219aaa686551cdf6d4f94c4478b69f819225176149606cd1492d5e1` |
| `/bda/raw/unswnb15/test.parquet` | 9,618,034 | `f6989e65032e75770f37a5fa64d1d556effd6ac6240c99b0ab4df73b490c1918` |

Selected 30 GB manifest is intentionally incomplete at the user-directed File 13 cutoff: verified packet bytes are 22,158,691,332 B; selected Files 14–17 (7,917,684,569 B) are not complete. The File 14 cache partial is preserved and excluded. Existing packet working slice (169,017,675 B) is preserved and excluded from the source total. No Task 2 or model training started. Remaining optional work requires explicit user direction to resume ingestion; do not resume automatically.

## Superseded 40 GB target assessment

## Decision

| Item | Finding |
|---|---|
| D: free / guard | 97.86 GB / passed 12 GB floor |
| C: free | 27.89 GB |
| Docker VHDX / WSL distro VHDX | `D:\DockerDesktopWSL\disk\docker_data.vhdx` / `D:\DockerDesktopWSL\main\ext4.vhdx` |
| Source selection | CIC `Packet-Bytes` files 1–17; file 18 excluded because legacy working slice was derived from it; all days/classes including SSH-Patator covered |
| Proposed packet bytes | 39,366,985,162 B (new source files) |
| Existing CIC flow + UNSW | 383,971,773 B, already in HDFS; measured and verify/skip |
| Unique source bytes in safe candidate | 39,750,956,935 B; short of 40,000,000,000 B target by 249,043,065 B |
| Existing packet slice | 169,017,675 B; preserved, excluded from target because it is a first-65%-rows slice sourced from Packet-Bytes File 18 |
| Maximum selected source file / staging cap | File 1: 4,262,871,602 B; current ingester rejects files >4,000,000,000 B; chunked D:-only transfer is required |
| Peak before new processing outputs | ~48.572 GB, including existing physical baseline, proposed packet bytes, rounded rollback volumes, and 4 GB staging cap |
| Headroom under 70 GB footprint cap | ~21.428 GB for new clean/features, Spark temp, Mongo growth, model outputs, and other data; these outputs are unmeasured |

The prior 17 GB packet cap is superseded by the new 40 GB source target; the plan sets a 40 GB packet budget but remains blocked below the target. No file payloads were downloaded. Current disk free space is sufficient, but no 40 GB completion claim is made. A verified non-overlapping source of at least 249,043,065 B is not present in the current inventory. Do not count or re-ingest the legacy File 18 slice as source bytes.

## Existing-file row/class measurements

Read-only Spark job against existing HDFS Parquet at repository HEAD `04682e812a94f5500637316269bbafaf0343663e`; data version is the current HDFS raw-path snapshot (its source revision was not reverified during this count). No random seed applies to exact counts.

| Existing file | Measured rows | Measured class distribution summary |
|---|---:|---|
| CICIDS_Flow.parquet | 2,827,677 | All 15 CIC classes; exact counts in `artifacts/ingest_plan.json` |
| UNSW train.parquet | 82,332 | Normal 37,000; Generic 18,871; Exploits 11,132; Fuzzers 6,062; DoS 4,089; Recon 3,496; Analysis 677; Backdoor 583; Shellcode 378; Worms 44 |
| UNSW test.parquet | 175,341 | Normal 56,000; Generic 40,000; Exploits 33,393; Fuzzers 18,184; DoS 12,264; Recon 10,491; Analysis 2,000; Backdoor 1,746; Shellcode 1,133; Worms 130 |

Packet per-file rows and published class distributions are in `artifacts/ingest_plan.json`; those values are published `nids-datasets` metadata, not local measurements. The read-only Spark command initially used `Label` and failed schema resolution; it was corrected to `attack_label` for CIC and `attack_cat` for UNSW. Successful command used `docker exec app python3 -c` with Spark master `spark://spark-master:7077`, reading the three existing HDFS Parquet paths and calling `count()` plus `groupBy(label).count()`. Disk guard passed immediately before the successful read.

## Remaining limitations at the user-directed cutoff

- The 30 GB approved packet selection is incomplete: Files 14–17 (7,917,684,569 B) are not fully ingested. The partial File 14 staging file is preserved and not counted. No resume is authorized by this cutoff instruction.
- The verified packet subset does not confirm packet-level Infiltration coverage because File 14 was not completed. The existing CIC flow file was independently verified and its published/previously measured class summary includes the CIC classes.
- Future Task 2 outputs and scratch growth are not measured; enforce the 12 GB D: floor and 70 GB physical cap before each heavy step.
- No pre-run C: filesystem-wide >200 MB baseline exists, so exact C: growth cannot be attributed. Current C: free space and D: locations are measured above.

# Phase 1, Section A — Read-only environment and storage audit

Audit date: 2026-10-27 (Asia/Calcutta)
Repository: `D:\Downloads\BDA_Lab_ESE\bda-ids`
Scope: Repository state, Compose configuration, live containers, persistent storage, HDFS usage, MongoDB storage, Docker disk/cache reporting, WSL configuration, and repository cache paths. No files or runtime state were changed during data collection.

## Execution and repository state

- PowerShell `Get-Date`, `Get-Location`, and `git status --short --branch` succeeded from the requested repository path.
- Git branch: `master`; worktree status: `?? src/models/`.
- The untracked directory contains eight files: `benchmark_worker.py` (2,453 B), `eval_utils.py` (4,476 B), `evaluate_unsw.py` (8,144 B), `inspect_unsw.py` (396 B), `run_scalability.py` (3,511 B), `test_mllib_params.py` (376 B), `train_binary.py` (18,887 B), and `train_multiclass.py` (11,152 B). These were preserved.
- `.env` exists (159 B); its variable names are `SPARK_WORKER_MEMORY`, `SPARK_WORKER_CORES`, and `COMPOSE_PROJECT_NAME`. Values are intentionally not reproduced.
- Tracked `docs/evidence/phase1.md` documents an earlier HDFS state and earlier evidence; current measurements below supersede its storage numbers where they differ.

## Compose and live containers

`docker-compose.yml` declares eight services: `namenode`, `datanode1`, `datanode2`, `spark-master`, `spark-worker-1`, `spark-worker-2`, `mongodb`, and `app`. All eight were `Up`; NameNode, both DataNodes, MongoDB, and Spark master showed healthy status. The Compose project volumes are `namenode_data`, `datanode1_data`, `datanode2_data`, and `mongo_data`. HDFS replication is configured as 2; block size is 16 MiB. Spark workers are configured for 1,500 MiB and two cores each. The app has a host bind mount of `artifacts` to `/artifacts` in read-only mode.

Docker Engine is 29.9.2, Docker Desktop operating system, and Docker root directory reported inside the engine is `/var/lib/docker`. Eight containers are active. `docker system df -v` reported:

- Images: 4, 5.826 GB total, 0 B reclaimable.
- Containers: 8, 684 kB reported; all active.
- Local volumes: 5, 1.431 GB total, 0 B reclaimable.
- Build cache: 2.594 GB; the summary reported it reclaimable (cache detail listed 23 entries).
- Named volume sizes: `bda_namenode_data` 3.162 MB; `bda_datanode1_data` 1.424 GB; `bda_datanode2_data` 1.424 GB; `bda_mongo_data` 318.6 MB. A separate anonymous Mongo `/data/configdb` volume was 0 B in Docker's volume usage listing.

## HDFS

`hdfs dfsadmin -report` reported 2 live DataNodes, configured capacity 2.16 TB, present capacity 2.03 TB, DFS remaining 2.027 TB, DFS used 2,848,698,368 bytes (2.65 GiB / 0.14%). Each DataNode reported 1,424,35x,xxx bytes used (about 1.33 GiB), consistent with two replicas. There were 67 blocks per DataNode. The report showed 10 under-replicated blocks, zero corrupt blocks, zero missing blocks, and zero pending deletion blocks. This differs from the older phase1 evidence that recorded zero under-replicated blocks.

HDFS root listing showed `/bda`; `hdfs dfs -du -h /` reported 1.3 G logical and 2.6 G space consumed after replication. My first `-du -h -d 2` query failed because this Hadoop CLI does not support `-d`; the corrected supported query succeeded. No HDFS mutation was run.

## MongoDB

MongoDB 7 was healthy, with `bda_mongo_data` mounted read/write at `/data/db` (318.6 MB reported by Docker). A read-only `mongosh` database listing/stats call succeeded without authentication. It listed only `admin`, `config`, and `local`, with storage sizes 20,480 B, 24,576 B, and 36,864 B respectively; no application database was listed. The apparent difference between those database stats and the 318.6 MB volume usage is volume allocation/other on-disk overhead not explained by the listing; exact collection-level project data was not observed.

## Docker disk image, WSL, and caches

- `wsl --list --verbose` showed `docker-desktop` running as WSL version 2.
- The previously readable user `.wslconfig` content specifies memory 8 GB, processors 6, swap 4 GB, `sparseVhd=true`, and `autoMemoryReclaim=gradual`.
- The Docker engine reports its Linux root as `/var/lib/docker`, but the actual Windows VHDX file path and allocated/maximum size were not established. The command session's `LOCALAPPDATA` resolved to a sandbox package path, not the host Docker data path; WSL/Docker host disk path inspection therefore remains unknown.
- Docker build cache is measured above at 2.594 GB. A repository search found no `__pycache__`, `.pytest_cache`, `.mypy_cache`, `.ruff_cache`, `.sparkStaging`, or `*.log` paths.
- `data/staging` exists and contains no files. Repository `artifacts/` contains dataset/staging manifests, feature CSV/JSON outputs, profile JSON outputs, timings for phases 2 and 3, and an `artifacts/models` directory. No data were downloaded or altered.

## Errors and remaining unknowns

- Initial sandbox calls to Docker and WSL failed with `permission denied` / `Wsl/E_ACCESSDENIED`; the checks succeeded after using an elevated terminal command. This was a command-session access boundary, not a reported Docker Engine outage.
- A recursive Docker WSL VHDX search could not be completed against the host Docker data path because this session's environment maps `LOCALAPPDATA` to a sandbox package location. Thus Docker disk-image filename, location, and actual VHDX allocated capacity remain unknown.
- Current host free space on C: and D: was not measured in this audit. The old phase1 evidence contains historical free-space figures; they should not be treated as current.
- HDFS currently reports 10 under-replicated blocks. No block-level fsck was run in this audit, so exact affected files/blocks are unknown.
- MongoDB showed no project/application database among the listed databases. It remains unknown whether the reported volume use includes reusable files, oplog/diagnostic state, or other storage not represented by database stats.

## Commands used

Read-only commands included `Get-Date`, `Get-Location`, `git status`, `git ls-files`, `Get-ChildItem`, `Get-Content`, `docker compose ps --all`, `docker info`, `docker system df -v`, `docker ps -aq`, `docker inspect`, `docker volume inspect`, `docker exec ... hdfs dfsadmin -report`, `docker exec ... hdfs dfs -ls -h /`, `docker exec ... hdfs dfs -du -h /`, read-only `mongosh` database listing/stats, and `wsl --list --verbose`. No `docker compose up/down`, restart/recreate, prune, HDFS write/delete, dataset download, volume migration, or configuration edit was performed. This report is the only file written after the measurements were collected.

## Historical configuration and manifest checkpoint (superseded by final snapshot above)

This records the pre-approval checkpoint only. Later measurements, changes, and ingestion are recorded in the final snapshot above. Current user files under `src/models/` remain preserved (eight files). The worktree includes configuration/scripts/docs additions; see `git status --short`.

### Initial host and service measurements (historical)

| Measurement | Value | Source |
|---|---:|---|
| C: free | 27.89 GB | PowerShell `Get-PSDrive` |
| D: free | 97.86 GB | `scripts/disk_guard.ps1`; above 12 GB floor |
| Docker Desktop disk image | 18,416,140,288 B | `D:\DockerDesktopWSL\disk\docker_data.vhdx` |
| Docker Desktop distro image | 109,051,904 B | `D:\DockerDesktopWSL\main\ext4.vhdx` |
| Docker images / containers | 5.826 GB / 536.6 kB | `docker system df` |
| Docker local volumes | 2.613 GB | Five inactive rollback volumes, preserved |
| Docker build cache | 2.594 GB | `docker system df`; not pruned |
| HDFS `/bda` logical | 1,412,923,377 B | `hdfs dfs -du -s /bda` |
| HDFS `/bda` physical | 2,272,857,306 B | `scripts/disk_guard.ps1` |
| Mongo host data files | 318,942,755 B | PowerShell under `D:\bda\mongo\data` |
| Mongo database `sizeOnDisk` | admin 40,960; config 122,880; local 73,728 B | Final `scripts/disk_guard.ps1` output |
| Staging | 0 B | `scripts/disk_guard.ps1` |

Docker image and WSL distro files are on D:. WSL reports `docker-desktop` as the WSL2 distro. Host `HF_HOME`, `HF_HUB_CACHE`, `PIP_CACHE_DIR`, and `NPM_CONFIG_CACHE` were unset. Default C: caches measured Hugging Face 0.67 GB, pip 0 GB, npm 0.748 GB; TEMP/TMP directory totaled 1,160,296,601 B (~1.08 GiB). No individual file larger than 200 MiB was found in the inspected cache/TEMP folders at inspection time. These pre-existing cache directories were not moved. Physical host RAM query was denied; CPU count reported 12. `.wslconfig` retains 8 GB memory and 6 processors; swap is configured as `D:\wsl\swap.vhdx`, with activation pending next WSL restart.

All eight existing Compose services were healthy in the last `docker compose ... ps --all` check. Compose config validation passed. Current HDFS `fsck /bda`: HEALTHY, 2 DataNodes, 48 files, 67 blocks, zero under-replicated/missing/corrupt blocks, default replication 2, average replication 1.4626865. `hdfs dfs -du -h /bda`:

| Zone | Logical | Physical |
|---|---:|---:|
| `/bda/clean` | 574.2 MiB | 1.1 GiB |
| `/bda/features` | 245.2 MiB | 490.3 MiB |
| `/bda/models` | 746.9 KiB | 1.5 MiB |
| `/bda/raw` | 527.4 MiB | 527.4 MiB |

Raw uses replication 1; clean/features/models use 2. Existing HDFS raw files: CIC flow 369,814,810 B; CIC packet working slice 169,017,675 B; UNSW train 4,538,929 B; UNSW test 9,618,034 B. Existing row counts are not measured.

### Changes and preservation record

- `.env`: `BDA_ROOT=D:/bda`, Spark worker 700m/1 core, compose name `bda`.
- `docker-compose.yml` was extended with 128 MiB HDFS blocks, 5 GiB DataNode reserved space, replication 2, Spark 3.5.3 limits/temp setting, MongoDB WiredTiger cache limit 2 GiB, healthchecks/restart policies/resource limits. Existing service names retained; `app` pre-existed. No new Streamlit service was added.
- `docker-compose.storage-d.yml` maps HDFS, MongoDB, Spark temp, staging, models, and logs under D:\bda. `scripts/up.ps1` and `scripts/down.ps1` use the storage override. Original named volume definitions/data remain as rollback copies.
- `scripts/migrate_volumes_to_d.ps1 -Apply` copied the existing HDFS/Mongo volumes to D: and restarted services using the override. Stable NameNode VERSION/fsimage files: 5 files / 15,917 B, SHA-256 matched. DataNode block/meta checks matched: DN1 134 files / 1,423,962,378 B; DN2 62 files / 866,652,438 B. Mongo configdb was empty in both copies. Live Mongo WiredTiger file hashes were not compared after startup; ping, DB names/stats, and service health were checked.
- Old named volumes remain inactive rollback copies; Docker reports 2.613 GB total. NameNode edit logs advanced after startup, so the old named volume is a frozen snapshot. No volume pruning, HDFS formatting, `/bda` deletion, or `down -v` occurred.
- `scripts/hdfs_zones.ps1` applied zone replication. `scripts/disk_guard.ps1` reports D free, HDFS logical/physical use, Mongo stats/volume, staging, and fails below the floor. `.wslconfig` backup is `C:\Users\pooja\.wslconfig.bda-backup`; swap setting is not active until WSL restarts.

Commands for the refreshed checks: `scripts/disk_guard.ps1`; `docker compose -f docker-compose.yml -f docker-compose.storage-d.yml config --quiet`; same Compose `ps --all`; `docker system df`; `docker exec namenode hdfs dfs -du -s /bda`; `docker exec namenode hdfs fsck /bda`. These read-only calls required elevated Docker Desktop access. Sandbox-only Docker calls were denied and changed nothing.

### Manifest-first proposal (approval required; no downloads performed)

Metadata came from `HfApi.list_repo_tree/get_paths_info` plus `nids_datasets.DatasetInfo` published per-file counts. CIC repo `rdpahalavan/CIC-IDS2017`, revision `eee96b6abc2c4bb621fd67679a4aa24bddc4be6a`; UNSW repo `wwydmanski/UNSW-NB15`, revision `961c58a6aab1f4736f6a34be968d672a515a886e`.

| Dataset | Source file(s) proposed | Bytes | Published row metadata | Day / attack-class metadata |
|---|---|---:|---:|---|
| CIC packet | Payload_Bytes_File_2.parquet | 1,977,480,777 | 3,571,097 | Monday; BENIGN |
| CIC packet | Payload_Bytes_File_4.parquet | 2,188,802,450 | 2,588,707 | Tuesday; BENIGN, FTP-Patator |
| CIC packet | Payload_Bytes_File_9.parquet | 2,185,352,994 | 3,015,813 | Wednesday; BENIGN, DoS slowloris, DoS Slowhttptest |
| CIC packet | Payload_Bytes_File_11.parquet | 2,691,287,304 | 5,359,118 | Wednesday; BENIGN, DoS slowloris, DoS Hulk, Heartbleed, DoS GoldenEye |
| CIC packet | Payload_Bytes_File_12.parquet | 2,183,748,223 | 2,663,758 | Thursday; BENIGN, Web Attack – Brute Force |
| CIC packet | Payload_Bytes_File_13.parquet | 1,884,029,359 | 3,136,779 | Thursday; BENIGN, Web Attack – Brute Force, XSS, SQL Injection |
| CIC packet | Payload_Bytes_File_14.parquet | 1,568,030,050 | 2,905,004 | Thursday; BENIGN, Infiltration |
| CIC packet | Payload_Bytes_File_16.parquet | 1,857,787,437 | 3,195,638 | Friday; BENIGN, Bot, PortScan |
| CIC packet | Payload_Bytes_File_18.parquet | 283,382,806 | 817,864 | Friday; BENIGN, DDoS |
| CIC flow | CICIDS_Flow.parquet (all available) | 369,814,810 | Not measured | Entire available flow file |
| UNSW-NB15 | train.parquet; test.parquet | 14,156,963 | Not measured | Both available validation files |

Selected packet shards total 16,819,901,400 B. Preserved packet working slice adds 169,017,675 B: packet bytes total 16,988,919,075 B, below the 17,000,000,000 B cap. Selection includes every file containing required rare classes (Infiltration, Heartbleed, SQL Injection, XSS, Web Brute Force) and covers all five days. `SSH-Patator` is a coverage gap because adding its file would exceed the cap with the existing slice. Whole source files are selected; no row slicing/padding. CIC flow and both UNSW files already exist in HDFS and will be verified then skipped if identical. Largest proposed file is 2,691,287,304 B; staging maximum is 4,000,000,000 B. Published row metadata is not a locally measured result.

At this checkpoint, `D:\bda\_meta\ingest_plan.json` (repository copy `artifacts\ingest_plan.json`) was proposed. It was later approved and executed; see the final snapshot for actual verified files, sizes, checksums, and storage measurements.

### Unresolved / not claimed

- No complete pre-run C: filesystem-wide baseline of files over 200 MiB was established; therefore exact C: growth cannot be attributed. Current C: free space is measured in the final snapshot.
- Physical host RAM remains unknown.
- WSL swap-file relocation takes effect only after WSL restart.
- The final selected packet shards include SSH-Patator. The remaining approved files were not ingested after the user-directed File 13 cutoff.
