# Phase 3 — Distributed Feature Engineering Evidence

Generated: 2026-10-24

---

## 1. Feature Engineering Overview & Summary

Phase 3 engineered a unified, leakage-free feature table combining flow-level traffic metrics, aggregated packet statistics, and payload-byte representations across the distributed Spark standalone cluster:

- **Total Rows Processed:** 2,827,083 network flows
- **Output Storage Path:** `hdfs://namenode:9000/bda/features/flow_features_full/` (8 Snappy-compressed Parquet partitions, 257.1 MB total)
- **HDFS Replication:** Replicated across both DataNodes (`datanode1` and `datanode2`), status: **HEALTHY**
- **Feature Set Breakdown:**
  - **Flow Features (46):** Network timing, packet size, inter-arrival time, TCP flag counts, window sizes, protocol code, and destination port.
  - **Packet Aggregate Features (11):** Packet count, total/mean/std/min/max payload lengths, payload presence ratio, directional forward packet ratio, and IAT placeholders.
  - **Payload Features (20):** Total/mean/max payload bytes, Shannon entropy (in bits, base-2), and a 16-bucket normalized byte histogram ($[0..15], [16..31], \dots, [240..255]$).
  - **Linkage Flag (1):** `has_packet_features` (1 for linked flows, 0 for unlinked).
  - **Total Model Features:** 78
  - **Preserved Target & Split Columns (8):** `flow_id`, `label_raw`, `label_multi`, `is_attack`, `label_binary`, `day_of_week`, `split_temporal`, `split_stratified`.
  - **Total Output Columns:** 86

---

## 2. Linkage Analysis and Imputation Strategy

Cross-layer linkage analysis between flow records and packet captures:
- **Total Flow Records:** 2,827,083
- **Total Packet Records:** 177,693
- **Distinct `flow_id` in Flows:** 2,827,083 (100% unique primary key)
- **Distinct `flow_id` in Packets:** 34,548
- **Flows WITH Linked Packets:** 34,548 (**1.2220%** link rate)
- **Flows WITHOUT Linked Packets:** 2,792,535 (**98.7780%**)
- **Packets WITH Linked Flows:** 177,693 (**100.0000%** packet coverage)
- **Packets WITHOUT Linked Flows:** 0 (0.0000%)

### Imputation Rule:
Flows lacking linked packet captures retain their full flow features while packet and payload metrics (`pkt_count`, `payload_entropy`, `payload_hist_bin_*`, etc.) are imputed with default zero values (`0.0`), marked explicitly by `has_packet_features = 0`.

---

## 3. Dropped Features and Leakage Treatment

### 3.1 Identifier & Leakage Removal
To prevent shortcut learning and ensure generalization across different networks and capture environments, the following columns were excluded from the model feature set:
- `source_ip` & `destination_ip`: Host IP addresses leak network topology and specific testbed host identities.
- `source_port`: Client ephemeral ports are dynamically assigned by the host OS stack and carry no attack-specific semantics.
- `timestamp`: Wall-clock timestamps leak temporal sequence and day-specific attack schedules.
- `flow_id`: Database surrogate key.

**Treatment of Destination Port (`destination_port`):** Retained as a model feature. Unlike ephemeral source ports, destination port identifies the intended target service (e.g. 80/HTTP, 443/HTTPS, 22/SSH, 21/FTP, 53/DNS), which provides critical structural semantics for distinguishing network services under attack without revealing host identity.

### 3.2 Zero-Variance Features (8 Dropped)
Features exhibiting zero variance across all records were dropped:
- `bwd_psh_flags`
- `bwd_urg_flags`
- `fwd_avg_bytes_bulk`
- `fwd_avg_packets_bulk`
- `fwd_avg_bulk_rate`
- `bwd_avg_bytes_bulk`
- `bwd_avg_packets_bulk`
- `bwd_avg_bulk_rate`

### 3.3 Highly Correlated Features (24 Dropped, $|r| > 0.95$)
To eliminate severe multicollinearity and prevent redundant feature weighting during model training:
- `subflow_bwd_packets` ($r = 1.0000$ with `total_backward_packets`)
- `subflow_fwd_bytes` ($r = 1.0000$ with `total_length_of_fwd_packets`)
- `subflow_fwd_packets` ($r = 1.0000$ with `total_fwd_packets`)
- `subflow_bwd_bytes` ($r = 1.0000$ with `total_length_of_bwd_packets`)
- `avg_fwd_segment_size` ($r = 1.0000$ with `fwd_packet_length_mean`)
- `avg_bwd_segment_size` ($r = 1.0000$ with `bwd_packet_length_mean`)
- `fwd_header_length` ($r = 0.9999$ with `total_fwd_packets`)
- `bwd_header_length` ($r = 0.9999$ with `total_backward_packets`)
- `total_backward_packets` ($r = 0.9991$ with `total_fwd_packets`)
- `fwd_packets_s` ($r = 0.9998$ with `flow_packets_s`)
- `bwd_packet_length_mean` ($r = 0.9669$ with `bwd_packet_length_max`)
- `bwd_packet_length_std` ($r = 0.9702$ with `bwd_packet_length_max`)
- `fwd_packet_length_std` ($r = 0.9736$ with `fwd_packet_length_max`)
- `packet_length_std` ($r = 0.9575$ with `max_packet_length`)
- `average_packet_size` ($r = 0.9634$ with `packet_length_mean`)
- `fwd_iat_total` ($r = 0.9984$ with `flow_duration`)
- `fwd_iat_max` ($r = 0.9712$ with `flow_iat_max`)
- `total_length_of_bwd_packets` ($r = 0.9961$ with `total_length_of_fwd_packets`)
- `idle_max` ($r = 0.9985$ with `flow_iat_max`)
- `idle_min` ($r = 0.9856$ with `flow_iat_max`)
- `idle_mean` ($r = 0.9991$ with `flow_iat_max`)
- `syn_flag_count` ($r = 1.0000$ with `fwd_psh_flags`)
- `cwe_flag_count` ($r = 1.0000$ with `fwd_urg_flags`)
- `ece_flag_count` ($r = 1.0000$ with `rst_flag_count`)

---

## 4. Commands Run and Terminal Outputs

### 4.1 Schema Inspection and Linkage Check
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1100m \
  --conf spark.cores.max=4 \
  /opt/spark/work-dir/inspect_schemas.py
```
**Output:**
```
======================================================================
PHASE 3: Inspecting Cleaned Schemas and Flow-Packet Linkage
======================================================================
Total Flows: 2,827,083
Total Packets: 177,693
'flow_id' in flows: True
'flow_id' in packets: True

--- Linkage Analysis ---
Distinct flow_id in Flows: 2,827,083
Distinct flow_id in Packets: 34,548
Flows WITH linked packets: 34,548 (1.2220%)
Flows WITHOUT linked packets: 2,792,535 (98.7780%)
Packets WITH linked flows: 177,693 (100.0000%)
Packets WITHOUT linked flows: 0 (0.0000%)
```

### 4.2 Feature Engineering Pipeline Execution
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1100m \
  --conf spark.cores.max=4 \
  --py-files /opt/spark/work-dir/src/features/flow_features.py,/opt/spark/work-dir/src/features/packet_aggregates.py,/opt/spark/work-dir/src/features/payload_features.py \
  /opt/spark/work-dir/src/features/build_features.py
```
**Output:**
```
======================================================================
PHASE 3: Distributed Feature Engineering on Spark Cluster
======================================================================

[Step 1] Reading cleaned flows and packets from HDFS...
  Read completed in 5.96s

[Step 2] Extracting and filtering flow-level features...
  Selected flow features (46): ['destination_port', 'protocol_num', 'flow_duration', 'total_fwd_packets', 'total_length_of_fwd_packets']...

[Step 3] Computing per-flow packet aggregates...
  Computed packet aggregates (11): ['pkt_count', 'pkt_len_total', 'pkt_len_mean', 'pkt_len_std', 'pkt_len_min', 'pkt_len_max', 'pkt_payload_fraction', 'pkt_fwd_ratio', 'pkt_iat_mean', 'pkt_iat_std', 'pkt_iat_max']

[Step 4] Computing payload-byte statistics, Shannon entropy & 16-bin histogram...
  Computed payload features (20): ['payload_bytes_total', 'payload_bytes_mean', 'payload_bytes_max', 'payload_entropy'] + 16 bins

[Step 5] Joining features into single per-flow feature table...
  Join and imputation finished in 0.31s

[Step 6] Writing /bda/features/flow_features_full/ to HDFS (8 coalesced partitions)...
  Write completed in 50.75s

[Step 7] Generating feature catalog and summary statistics...
  Total Rows: 2,827,083
  Linked Flows (has_packet_features=1): 34,548 (1.22%)
  Unlinked Flows (has_packet_features=0): 2,792,535 (98.78%)
  Feature catalog saved to /opt/spark/work-dir/artifacts/features/feature_catalog.csv (78 features)
  Feature stats saved to /opt/spark/work-dir/artifacts/features/feature_stats.json
  Phase 3 timings saved to /opt/spark/work-dir/artifacts/timings/phase3.json

Sample Output Records (Cleaned Features + Metadata):
+-------+----------------+-------------------+---------+---------------+-----------+----------------+
|flow_id|destination_port|has_packet_features|pkt_count|payload_entropy|label_multi|split_stratified|
+-------+----------------+-------------------+---------+---------------+-----------+----------------+
| 192926|           443.0|                  0|      0.0|            0.0|     BENIGN|           train|
| 426889|            53.0|                  0|      0.0|            0.0|     BENIGN|            test|
| 192922|         59264.0|                  0|      0.0|            0.0|     BENIGN|           train|
| 454593|           443.0|                  0|      0.0|            0.0|     BENIGN|           train|
| 197777|           123.0|                  0|      0.0|            0.0|     BENIGN|           train|
+-------+----------------+-------------------+---------+---------------+-----------+----------------+
only showing top 5 rows

======================================================================
Phase 3 Complete. Total Wallclock Time: 78.23s
======================================================================
```

### 4.3 Unit Test Suite Execution
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master local[2] \
  /opt/spark/work-dir/tests/test_features.py
```
**Output:**
```
============================================================
RUNNING UNIT TESTS FOR FEATURE ENGINEERING PIPELINE
============================================================
[Test 1] Testing Shannon entropy on known byte distributions...
  ✓ Monotonic byte string entropy == 0.0 bits
  ✓ Two equal byte values entropy == 1.0 bit
  ✓ Four equal byte values entropy == 2.0 bits

[Test 2] Testing 16-bucket byte histogram normalization...
  ✓ 16-bucket histogram normalization and bounds verified

[Test 3] Testing Null-Safety and Join Row-Count Preservation...
  ✓ Exact row count (100) preserved after left join
  ✓ has_packet_features correctly flagged (10 linked, 90 unlinked)
  ✓ Zero nulls present in all numeric feature columns post-imputation

============================================================
ALL UNIT TESTS PASSED SUCCESSFULLY!
============================================================
```

### 4.4 HDFS Verification and Block Health
```bash
docker exec namenode hdfs dfs -ls -h /bda/features/flow_features_full/
docker exec namenode hdfs fsck /bda/features/flow_features_full/ -files -blocks -locations
```
**Output:**
```
Found 9 items
-rw-r--r--   2 spark supergroup          0 2026-10-23 20:21 /bda/features/flow_features_full/_SUCCESS
-rw-r--r--   2 spark supergroup     30.5 M 2026-10-23 20:21 /bda/features/flow_features_full/part-00000-...
-rw-r--r--   2 spark supergroup     30.7 M 2026-10-23 20:21 /bda/features/flow_features_full/part-00001-...
-rw-r--r--   2 spark supergroup     30.7 M 2026-10-23 20:21 /bda/features/flow_features_full/part-00002-...
-rw-r--r--   2 spark supergroup     30.7 M 2026-10-23 20:21 /bda/features/flow_features_full/part-00003-...
-rw-r--r--   2 spark supergroup     30.6 M 2026-10-23 20:21 /bda/features/flow_features_full/part-00004-...
-rw-r--r--   2 spark supergroup     30.6 M 2026-10-23 20:21 /bda/features/flow_features_full/part-00005-...
-rw-r--r--   2 spark supergroup     30.7 M 2026-10-23 20:21 /bda/features/flow_features_full/part-00006-...
-rw-r--r--   2 spark supergroup     30.6 M 2026-10-23 20:21 /bda/features/flow_features_full/part-00007-...

Status: HEALTHY
Total size: 257067537 B (257.1 MB across 8 blocks)
Replication: 2 across datanode1 and datanode2
Corrupt/Missing blocks: 0
```

---

## 5. Artifacts Produced

1. `artifacts/features/feature_catalog.csv`: Full registry of 78 model features categorized by group with descriptive annotations.
2. `artifacts/features/feature_stats.json`: Descriptive statistics (count, mean, stddev, min, max), zero-variance drop list, and high-correlation drop list.
3. `artifacts/timings/phase3.json`: Wall-clock pipeline timings (read: 5.96s, write: 50.75s, total: 78.23s).
4. `docs/evidence/phase3_schema_notes.md`: Detailed schema inspection and empirical flow-packet linkage verification.
5. `tests/test_features.py`: Automated unit test suite verifying entropy calculation, histogram binning, null-safety, and row-count preservation.
