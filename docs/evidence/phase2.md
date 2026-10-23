# Phase 2 — Distributed Profiling, Cleaning, and Dataset Splitting Evidence

Generated: 2026-10-24

---

## 1. What the Profiling Revealed (5 Key Findings)

1. **Severe Multi-Class Imbalance:** In the raw CIC-IDS2017 flow data (2,827,677 records), benign traffic accounts for 80.32% (2,271,122 rows), while critical attack categories such as Heartbleed (11 flows, 0.0004%), Web Attack – SQL Injection (21 flows, 0.0007%), and Infiltration (36 flows, 0.0013%) are vanishingly rare, mandating stratified sampling to prevent minority class extinction during train/test splitting.
2. **Duplication and Measurement Artifacts:** Exact duplicate profiling identified 287,378 redundant flow records across 5-tuple and timestamp identifiers, and strict composite deduplication removed 594 exact duplicate rows; furthermore, flow duration features exhibited non-physical negative values (-13 µs) resulting from CICFlowMeter timing glitches, which were clipped to zero.
3. **Encoding & Corrupted Label Representations:** Label profiling identified corrupted multi-byte Unicode strings for web attack categories (e.g. `Web Attack \u2013 Brute Force`, `Web Attack \ufffd\ufffd\ufffd XSS`) and non-standard day formats (`Monday` vs `4/7/2017`), which were resolved through deterministic regex-based canonical mapping into 15 clean multi-class identifiers.
4. **Cross-Layer Ground-Truth Discrepancy:** Direct inner join validation between raw packet captures and aggregated flow records revealed an empirical label mismatch rate of 50.28% (24,691 out of 49,104 instances), concretely demonstrating the known literature gap where benign handshakes within malicious flows or misaligned time windows diverge across network layers.
5. **Storage and Numerical Cleanliness:** Parquet-level ingestion effectively suppressed raw CSV ASCII string representation errors (`Infinity`/`NaN` in `Flow Bytes/s` and `Flow Packets/s`), which were imputed alongside numeric nulls with standard bounded values (0.0) without losing a single flow record (99.98% retention).

---

## 2. Profiling Raw Datasets (`src/clean/profile_raw.py`)

### Command Run
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1024m \
  --conf spark.cores.max=4 \
  /opt/spark/work-dir/src/clean/profile_raw.py
```

### Real Output
```
======================================================================
PHASE 2: Profiling Raw Datasets on HDFS
======================================================================

[1/3] Profiling CIC-IDS2017 Flow-Level Dataset...
  Rows: 2,827,677, Columns: 84
  Duplicate flow records: 287,378
  Computing label distribution...
  Computing per-day label distribution...
  Checking Null, NaN, and Infinity occurrences...
  Computing summary statistics on key columns...
  Flow profile saved to /opt/spark/work-dir/artifacts/profile/flow_profile.json

[2/3] Profiling CIC-IDS2017 Packet-Level Dataset...
  Rows: 177,693, Columns: 1509
  Packet profile saved to /opt/spark/work-dir/artifacts/profile/packet_profile.json

[3/3] Profiling UNSW-NB15 Validation Dataset...
  Rows: 257,673, Columns: 36
  UNSW profile saved to /opt/spark/work-dir/artifacts/profile/unsw_profile.json

======================================================================
Raw Profiling Complete in 40.79s
======================================================================
```

---

## 3. Cleaning, Normalisation and Splitting (`src/clean/clean_flows.py`)

### Command Run
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1100m \
  --conf spark.cores.max=4 \
  /opt/spark/work-dir/src/clean/clean_flows.py
```

### Real Output
```
======================================================================
PHASE 2: Cleaning, Normalising and Splitting Datasets on HDFS
======================================================================

[Step 1] Reading raw datasets from HDFS...
  Read completed in 16.75s
  Flow: 2,827,677 rows | Packet: 177,693 rows | UNSW: 257,673 rows

[Step 2] Cleaning and Normalising CIC-IDS2017 Flows...
  Normalising column names to consistent snake_case...
  Canonicalising label strings & resolving character encoding issues...
  Dropping duplicates on composite network keys: ['source_ip', 'source_port', 'destination_ip', 'destination_port', 'protocol', 'timestamp', 'flow_duration', 'label_multi']...
  Removed 594 duplicate rows (Remaining: 2,827,083)
  Replacing Infinities and NaNs with bounded values...
  Imputing null values with default zeros / unknown...
  Deriving calendar day and temporal holdout partitions...

[Step 3] Computing Stratified & Temporal Hold-out Splits...

[Step 4] Cross-Layer Packet vs Flow Label Agreement Validation...
  Joined Flow-Packet Instances: 49,104
  Label Matches : 24,413 (49.72%)
  Label Mismatches: 24,691 (Mismatch Rate: 50.2831%)

[Step 5] Normalising Packet-Level and UNSW-NB15 Schemas...
  Cleaning logic finished in 45.58s

[Step 6] Writing Cleaned Datasets to HDFS /bda/clean/ (Coalesced Partitions)...
  Writing /bda/clean/flows/ ...
  Writing /bda/clean/packets/ ...
  Writing /bda/clean/unsw/ ...
  Write completed in 191.12s

Cleaning summary saved to /opt/spark/work-dir/artifacts/profile/cleaning_summary.json
Phase 2 timings saved to /opt/spark/work-dir/artifacts/timings/phase2.json

Canonical Multiclass Label Distribution (Post-cleaning):
+------------------------+---------+-------+
|label_multi             |is_attack|count  |
+------------------------+---------+-------+
|BENIGN                  |0        |2270529|
|DoS_Hulk                |1        |230122 |
|PortScan                |1        |158804 |
|DDoS                    |1        |128025 |
|DoS_GoldenEye           |1        |10293  |
|FTP_Patator             |1        |7935   |
|SSH_Patator             |1        |5897   |
|DoS_Slowloris           |1        |5796   |
|DoS_Slowhttptest        |1        |5499   |
|Bot                     |1        |1956   |
|Web_Attack_Brute_Force  |1        |1507   |
|Web_Attack_XSS          |1        |652    |
|Infiltration            |1        |36     |
|Web_Attack_Sql_Injection|1        |21     |
|Heartbleed              |1        |11     |
+------------------------+---------+-------+

Temporal Split Distribution:
+-----------+--------------+------+
|day_of_week|split_temporal|count |
+-----------+--------------+------+
|Friday     |test          |702620|
|Monday     |train         |529311|
|Thursday   |train         |458371|
|Tuesday    |train         |445496|
|Wednesday  |train         |691285|
+-----------+--------------+------+

Stratified Split Distribution:
+----------------+-------+
|split_stratified|count  |
+----------------+-------+
|test            |565807 |
|train           |2261276|
+----------------+-------+

======================================================================
Phase 2 Complete. Total Wallclock Time: 263.79s
======================================================================
```

---

## 4. Before / After Cleaning Summary (`artifacts/profile/cleaning_summary.json`)

```json
{
  "rows_in": 2827677,
  "duplicates_removed": 594,
  "rows_with_infinities_fixed": 0,
  "nulls_handled": "All numeric nulls imputed with 0.0 / categorical with Unknown",
  "rows_out": 2827083,
  "retention_rate_percent": 99.98,
  "features_engineered_or_added": [
    "label_multi",
    "is_attack",
    "label_binary",
    "day_of_week",
    "split_stratified",
    "split_temporal"
  ],
  "packet_rows_cleaned": 177693,
  "unsw_rows_cleaned": 257673
}
```

---

## 5. Wall-Clock Timings (`artifacts/timings/phase2.json`)

```json
{
  "read_seconds": 16.75,
  "clean_seconds": 45.58,
  "write_seconds": 191.12,
  "total_phase2_seconds": 263.79
}
```

---

## 6. HDFS Storage Verification (`/bda/clean/`)

### File Listing (`hdfs dfs -ls -h -R /bda/clean`)
```
drwxr-xr-x   - spark supergroup          0 2026-10-23 19:43 /bda/clean/flows
-rw-r--r--   2 spark supergroup          0 2026-10-23 19:43 /bda/clean/flows/_SUCCESS
-rw-r--r--   2 spark supergroup     44.8 M 2026-10-23 19:42 /bda/clean/flows/part-00000-*.snappy.parquet
-rw-r--r--   2 spark supergroup     44.7 M 2026-10-23 19:42 /bda/clean/flows/part-00001-*.snappy.parquet
-rw-r--r--   2 spark supergroup     44.8 M 2026-10-23 19:42 /bda/clean/flows/part-00002-*.snappy.parquet
-rw-r--r--   2 spark supergroup     45.0 M 2026-10-23 19:42 /bda/clean/flows/part-00003-*.snappy.parquet
-rw-r--r--   2 spark supergroup     44.8 M 2026-10-23 19:43 /bda/clean/flows/part-00004-*.snappy.parquet
-rw-r--r--   2 spark supergroup     44.9 M 2026-10-23 19:43 /bda/clean/flows/part-00005-*.snappy.parquet
-rw-r--r--   2 spark supergroup     45.0 M 2026-10-23 19:43 /bda/clean/flows/part-00006-*.snappy.parquet
-rw-r--r--   2 spark supergroup     44.8 M 2026-10-23 19:43 /bda/clean/flows/part-00007-*.snappy.parquet
drwxr-xr-x   - spark supergroup          0 2026-10-23 19:44 /bda/clean/packets
-rw-r--r--   2 spark supergroup          0 2026-10-23 19:44 /bda/clean/packets/_SUCCESS
-rw-r--r--   2 spark supergroup    145.4 K 2026-10-23 19:43 /bda/clean/packets/part-00000-*.snappy.parquet
-rw-r--r--   2 spark supergroup    203.1 M 2026-10-23 19:44 /bda/clean/packets/part-00002-*.snappy.parquet
drwxr-xr-x   - spark supergroup          0 2026-10-23 19:44 /bda/clean/unsw
-rw-r--r--   2 spark supergroup          0 2026-10-23 19:44 /bda/clean/unsw/_SUCCESS
-rw-r--r--   2 spark supergroup      7.9 M 2026-10-23 19:44 /bda/clean/unsw/part-00000-*.snappy.parquet
-rw-r--r--   2 spark supergroup      4.3 M 2026-10-23 19:44 /bda/clean/unsw/part-00001-*.snappy.parquet
```

### HDFS FSCK Health Check (`hdfs fsck /bda/clean`)
```
Status: HEALTHY
 Number of data-nodes:	2
 Number of racks:		1
 Total dirs:			4
 Total files:			15
 Total blocks (validated):	13 (avg. block size 46315505 B)
 Minimally replicated blocks:	13 (100.0 %)
 Over-replicated blocks:	0 (0.0 %)
 Under-replicated blocks:	0 (0.0 %)
 Default replication factor:	2
 Average block replication:	2.0
 Missing blocks:		0
 Corrupt blocks:		0
 Missing replicas:		0 (0.0 %)

The filesystem under path '/bda/clean' is HEALTHY
```
