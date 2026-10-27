# Phase 2 execution record

## Scope and safety

| Item | Recorded value |
|---|---|
| Git HEAD | `04682e812a94f5500637316269bbafaf0343663e` |
| Input scope | CIC flow: bounded 50,000-row prefix; CIC packet: existing `cicids2017_packet_slice.parquet` only, bounded to 50,000 rows; UNSW: configured 50,000-row prefix |
| Excluded | All larger CIC packet Parquet files; no download or model training |
| D: free after stop | 72.37 GB; 12 GB floor passed |
| HDFS `/bda` last measured during this run | 23,571,614,709 B logical; 24,431,548,638 B physical |
| MongoDB volume / staging last measured | 318.9 MB / 943,718,651 B |

HDFS `dfs -du -s` at stop: `/bda/raw` 22,711,680,780 B logical / physical; `/bda/clean` 602,101,567 / 1,204,203,134 B; `/bda/features` 257,067,537 / 514,135,074 B; `/bda/models` 764,825 / 1,529,650 B. No Phase 2 output paths were added.

## Cluster job results

| Job | Result | Measured facts |
|---|---|---|
| `BDA-Phase2-SourceSchemaProbe` | Completed, read-only | CIC flow: 84 columns; packet file probe: 1,609 columns; UNSW: 36 columns |
| `BDA-Phase2-Profile` bounded run | Stopped at the 10-minute limit; no profile tables committed | CIC flow prefix: 50,000 rows, 84 source columns, 0 exact duplicate rows, 170.28 s. CIC packet slice column-quality pass reached 1,513 columns; complete packet summary was not emitted. UNSW summary was not emitted. |
| `clean.py` | Not run | No clean output written |
| `make_splits.py` | Not run | No split assignments written |
| `validate_labels.py` | Not run | No join result or disagreement sample written |

The earlier full-data profile attempt measured CIC flow at 2,827,677 rows, 84 columns, and 0 exact duplicate rows in 394.26 s, but it was stopped before packet processing and committed no output. It is not used as a Phase 2 deliverable.

## Files and outputs

- Added cluster scripts: `src/spark/common.py`, `profile.py`, `clean.py`, `validate_labels.py`, and `make_splits.py`.
- Added read-only schema probe: `src/spark/inspect_sources.py`.
- Added high-resolution PNG generator: `scripts/plot_phase2.ps1`.
- Python syntax compilation and PowerShell plot-script parsing passed.
- At the 03:12 cutoff, `/bda/_meta/profile/phase2_v1` and the requested EDA files had not yet been generated. The later bounded EDA rerun and its outputs are recorded below.
- Existing raw files, HDFS data, volumes, and `src/models/` were preserved. No write to raw data and no downloads occurred.

## Commands and timestamps (UTC, 2026-10-27)

| Time | Command/action | Outcome |
|---|---|---|
| 02:22–02:25 | `spark-submit --master spark://spark-master:7077 /tmp/phase2_inspect_sources.py` | Schema probe completed |
| 02:32 | `spark-submit --master spark://spark-master:7077 /tmp/phase2_profile.py` | Full packet scan cancelled for excessive runtime; no profile output committed |
| 02:50 | Re-submitted profile with 256-column batches | Stopped before output commit while switching to reduced scope |
| 03:02:36 | `spark-submit --master spark://spark-master:7077 /tmp/phase2_profile.py` (50,000-row cap) | Flow sample completed; stopped at deadline before metadata writes |
| 03:02 and 03:12 | `scripts/disk_guard.ps1` | Passed; D: free 72.38 GB before run and 72.37 GB after |
| 03:12 | `git diff --check`; Python `py_compile`; PowerShell AST parse | Passed |

The final `hdfs dfs -du`/`fsck` recheck did not return within the stop window. The last measured HDFS totals above are from the disk guard during the run; Phase 1 had previously reported a healthy cluster. No new HDFS data was committed by this phase.

## Incomplete acceptance items

- Full-dataset profiling remains incomplete; the quick EDA tables and sample label map below cover only 10,000-row prefixes and selected columns.
- EDA PNGs are saved under `D:\bda\_meta\profile\EDA` (the user-requested directory); the earlier `graphs` subdirectory was not created.
- Run cleaning, label validation, and split assignment via `spark-submit`; verify HDFS outputs and replication.
- Packet scope is a bounded working-slice sample; larger packet files and full-dataset statistics remain unprocessed. Sample counts and class distributions must not be generalized to the full source datasets.

## Bounded EDA run (2026-10-27 03:30 UTC)

| Dataset | Rows sampled | Selected columns incl. derived label/day | Observed classes | Profile seconds |
|---|---:|---:|---|---:|
| CIC flow | 10,000 | 11 | BENIGN 10,000 | 20.59 |
| CIC packet working slice | 10,000 | 9 | BENIGN 3,286; DDoS 6,714 | 7.63 |
| UNSW-NB15 | 10,000 | 7 | BENIGN 10,000 | 7.49 |

The sample is a deterministic 10,000-row prefix per input after projecting the EDA columns. It is intentionally small, not class-stratified, and not representative of full-dataset class balance. Packet day could not be mapped from the sampled flow IDs and is `UNKNOWN`; no day-specific attack conclusion is made. Only the existing packet slice was read; the large packet files were not processed.

| EDA measure | Observed sample result |
|---|---|
| Packet protocol | TCP 9,999; UDP 1 |
| Packet size bins | 1–64: 2,684; 65–128: 6; 129–256: 42; 257–512: 198; 513–1,024: 238; 1,025+: 6,832 |
| Max-class / class ratio | CIC flow 1.0 (BENIGN only); CIC packet 2.0432; UNSW 1.0 (BENIGN only) |
| Null / NaN / Inf | 0 in every selected profile column (15 columns across these samples) |
| Packet comparison | 10,000 before; 10,000 after exact deduplication; 10,000 after valid payload-size rule |
| Spark EDA job runtime | 115.68 s, `spark-submit`, application `app-20261027033037-0012` |

EDA comparison rules counted exact duplicate selected packet observations, then required `payload_length` in `[1,65535]`. This is a sample EDA comparison only; `clean.py` was not run and no cleaned dataset was written.

### Generated files

- PNG directory: `D:\bda\_meta\profile\EDA` (13 files, each 2400×1600 pixels at 300 DPI).
- PNGs: `class_distribution.png`, `class_distribution_cicids2017_flow.png`, `class_distribution_unswnb15.png`, `class_imbalance.png`, `class_imbalance_cicids2017_flow.png`, `class_imbalance_unswnb15.png`, `missing_rates.png`, `missing_rates_cicids2017_packet.png`, `missing_rates_unswnb15.png`, `protocol_distribution.png`, `packet_size_distribution.png`, `attacks_by_day.png`, `cleaning_comparison.png`.
- Supporting CSV tables: `D:\bda\_meta\profile\csv`.
- Supporting Parquet tables: `/bda/_meta/profile/phase2_v1/`.
- Sample label map: `/bda/_meta/label_map.json`.
- CSV tables: `rows_by_dataset_day_label`, `class_imbalance_by_class`, `column_quality`, `protocol_distribution`, `packet_size_distribution`, `eda_before_after`, and `profile_summary`.
- HDFS verification: `hdfs fsck /bda/_meta/profile/phase2_v1 -files` reported `HEALTHY`, 19 files, 12 validated blocks, zero missing/corrupt/under-replicated/mis-replicated blocks; average block replication 2.0. `hdfs dfs -ls -R` confirmed replication 2 after `hdfs dfs -setrep -R 2`.
- Final `scripts/disk_guard.ps1` after replication adjustment: D: free 72.36 GB (12 GB floor passed); `/bda` 23,571,638,444 B logical / 24,431,596,108 B physical.

The first quick EDA attempt committed only the small `protocol_distribution` Parquet table; its CSV write failed because executor containers do not mount `/logs`. `common.py` now writes small summary CSVs from the Spark driver to the D:-backed app bind mount. The successful rerun reused the committed Parquet table and emitted the remaining outputs. An empty failed-attempt `protocol_distribution.csv.tmpdir` directory remains under `D:\bda\logs\profile\phase2_v1\csv`; it was preserved.

The EDA run did not run cleaning, split assignment, or label validation. The 10,000-row samples contain no attack labels for flow or UNSW, and packet day is unknown; use the graphs only as exploratory summaries of this bounded sample.

Executed: `scripts/disk_guard.ps1`; `docker exec app /opt/spark/bin/spark-submit --master spark://spark-master:7077 /tmp/eda_quick.py` (executor memory 700 MiB, one core each); `scripts/plot_phase2.ps1 -InputDirectory D:\bda\logs\profile\phase2_v1\csv -OutputDirectory D:\bda\_meta\profile\EDA`; `hdfs dfs -setrep -R 2 /bda/_meta/profile/phase2_v1`; `hdfs dfs -setrep 2 /bda/_meta/label_map.json`; `hdfs fsck /bda/_meta/profile/phase2_v1 -files`.

Recovery detail: the initial quick run treated a local CSV path as HDFS and failed at HDFS `/`; the next run used `file://` but worker containers lack the app-only `/logs` bind mount. The final helper writes only compact summary CSV rows from the Spark driver to the D:-backed app mount. Application `app-20261027033037-0012` ended with `EDA_COMPLETE`; all table, label-map, and PNG outputs were subsequently listed and verified.
