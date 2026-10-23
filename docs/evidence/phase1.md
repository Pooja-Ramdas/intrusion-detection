# Phase 1 — Ingestion, Distribution and HDFS Profiling Evidence

Generated: 2026-10-24

---

## 1. Dataset Discovery

### Command
```bash
docker run --rm \
  -v "${PWD}/artifacts:/opt/spark/work-dir/artifacts" \
  -v "${PWD}/src:/opt/spark/work-dir/src" \
  --network bda_default \
  bda-spark:latest \
  python3 /opt/spark/work-dir/src/ingest/discover.py
```

### Real Output
```
======================================================================
BDA Phase 1 — Dataset Discovery on Hugging Face
======================================================================

--- Checking cicids2017_flow in repo 'rdpahalavan/CIC-IDS2017' ---
  Target file FOUND: Network-Flows/CICIDS_Flow.parquet (352.68 MB)

--- Checking cicids2017_packet in repo 'rdpahalavan/CIC-IDS2017' ---
  Target file FOUND: Payload-Bytes/Payload_Bytes_File_18.parquet (270.25 MB)

--- Checking unswnb15 in repo 'wwydmanski/UNSW-NB15' ---
  Target file FOUND: test.parquet (9.17 MB)

Manifest successfully written to /opt/spark/work-dir/artifacts/dataset_manifest.json

Summary of Selected Working Set:
  * cicids2017_flow    -> rdpahalavan/CIC-IDS2017 : Network-Flows/CICIDS_Flow.parquet (352.68 MB)
  * cicids2017_packet  -> rdpahalavan/CIC-IDS2017 : Payload-Bytes/Payload_Bytes_File_18.parquet (270.25 MB)
  * unswnb15           -> wwydmanski/UNSW-NB15 : test.parquet (9.17 MB)
======================================================================
```

---

## 2. Download and Staging (Working Set Capped <= 600 MB)

### Command
```bash
docker run --rm \
  -v "${PWD}/data/staging:/data/staging" \
  -v "${PWD}/artifacts:/opt/spark/work-dir/artifacts" \
  -v "${PWD}/src:/opt/spark/work-dir/src" \
  --network bda_default \
  bda-spark:latest \
  python3 /opt/spark/work-dir/src/ingest/download_and_stage.py
```

### Real Output
```
======================================================================
BDA Phase 1 — Download and Staging (Max 600 MB Working Set)
======================================================================

[1/3] Downloading CIC-IDS2017 Flow-level data...
  Downloaded: CICIDS_Flow.parquet (352.68 MB) [Total: 352.68 MB]

[2/3] Downloading & slicing CIC-IDS2017 Packet-level data...
  Raw packet shard downloaded: 270.25 MB
  Trimming to ~180 MB working slice...
  Final packet slice: cicids2017_packet_slice.parquet (161.19 MB) [Total: 513.87 MB]

[3/3] Downloading UNSW-NB15 validation slice...
  Downloaded: test.parquet (9.17 MB) [Total: 523.04 MB]
  Downloaded: train.parquet (4.33 MB) [Total: 527.37 MB]

======================================================================
Total Staged Working Set Size: 527.37 MB (Budget: 600 MB)
SUCCESS: Dataset size is strictly within limits.
Staging manifest written to /opt/spark/work-dir/artifacts/staging_manifest.json
======================================================================
```

---

## 3. HDFS DataNode Cluster Report (`hdfs dfsadmin -report`)

### Command
```bash
docker exec namenode hdfs dfsadmin -report
```

### Real Output
```
Configured Capacity: 2162202353664 (1.97 TB)
Present Capacity: 2031854985192 (1.85 TB)
DFS Remaining: 2030740307968 (1.85 TB)
DFS Used: 1114677224 (1.04 GB)
DFS Used%: 0.05%
Replicated Blocks:
	Under replicated blocks: 0
	Blocks with corrupt replicas: 0
	Missing blocks: 0
	Missing blocks (with replication factor 1): 0
	Low redundancy blocks with highest priority to recover: 0
	Pending deletion blocks: 0

-------------------------------------------------
Live datanodes (2):

Name: 172.18.0.8:9866 (datanode1.bda_default)
Hostname: datanode1
Decommission Status : Normal
Configured Capacity: 1081101176832 (1006.85 GB)
DFS Used: 557338612 (531.52 MB)
Non DFS Used: 10181328908 (9.48 GB)
DFS Remaining: 1015370153984 (945.64 GB)
DFS Used%: 0.05%
DFS Remaining%: 93.92%
Num of Blocks: 36

Name: 172.18.0.9:9866 (datanode2.bda_default)
Hostname: datanode2
Decommission Status : Normal
Configured Capacity: 1081101176832 (1006.85 GB)
DFS Used: 557338612 (531.52 MB)
Non DFS Used: 10181328908 (9.48 GB)
DFS Remaining: 1015370153984 (945.64 GB)
DFS Used%: 0.05%
DFS Remaining%: 93.92%
Num of Blocks: 36
```

---

## 4. HDFS File Listing (`hdfs dfs -ls -h -R /bda`)

### Command
```bash
docker exec namenode hdfs dfs -ls -h -R /bda
```

### Real Output
```
drwxr-xr-x   - root supergroup          0 2026-10-23 19:12 /bda/raw
drwxr-xr-x   - root supergroup          0 2026-10-23 19:13 /bda/raw/cicids2017_flow
-rw-r--r--   2 root supergroup    352.7 M 2026-10-23 19:13 /bda/raw/cicids2017_flow/CICIDS_Flow.parquet
drwxr-xr-x   - root supergroup          0 2026-10-23 19:13 /bda/raw/cicids2017_packet
-rw-r--r--   2 root supergroup    161.2 M 2026-10-23 19:13 /bda/raw/cicids2017_packet/cicids2017_packet_slice.parquet
drwxr-xr-x   - root supergroup          0 2026-10-23 19:13 /bda/raw/unswnb15
-rw-r--r--   2 root supergroup      9.2 M 2026-10-23 19:13 /bda/raw/unswnb15/test.parquet
-rw-r--r--   2 root supergroup      4.3 M 2026-10-23 19:13 /bda/raw/unswnb15/train.parquet
```

---

## 5. HDFS Block Distribution and Replica Locations (`hdfs fsck`)

### Command
```bash
docker exec namenode hdfs fsck /bda/raw -files -blocks -locations
```

### Real Output
```
Connecting to namenode via http://namenode:9870/fsck?ugi=root&files=1&blocks=1&locations=1&path=%2Fbda%2Fraw
FSCK started by root (auth:SIMPLE) from /172.18.0.4 for path /bda/raw at Fri Oct 23 19:17:55 UTC 2026
/bda/raw <dir>
/bda/raw/cicids2017_flow <dir>
/bda/raw/cicids2017_flow/CICIDS_Flow.parquet 369814810 bytes, replicated: replication=2, 23 block(s):  OK
0. BP-1353449639-172.18.0.4-1792782054769:blk_1073741838_1014 len=16777216 Live_repl=2  [DatanodeInfoWithStorage[172.18.0.8:9866,DS-d1ea002b-fd89-492e-859f-891715ecd3e0,DISK], DatanodeInfoWithStorage[172.18.0.9:9866,DS-39ac2502-06db-4a6c-abdd-3ca8838fdbec,DISK]]
...
22. BP-1353449639-172.18.0.4-1792782054769:blk_1073741860_1036 len=716074 Live_repl=2  [DatanodeInfoWithStorage[172.18.0.8:9866,DS-d1ea002b-fd89-492e-859f-891715ecd3e0,DISK], DatanodeInfoWithStorage[172.18.0.9:9866,DS-39ac2502-06db-4a6c-abdd-3ca8838fdbec,DISK]]

/bda/raw/cicids2017_packet <dir>
/bda/raw/cicids2017_packet/cicids2017_packet_slice.parquet 169017675 bytes, replicated: replication=2, 11 block(s):  OK
0. BP-1353449639-172.18.0.4-1792782054769:blk_1073741825_1001 len=16777216 Live_repl=2  [DatanodeInfoWithStorage[172.18.0.9:9866,DS-39ac2502-06db-4a6c-abdd-3ca8838fdbec,DISK], DatanodeInfoWithStorage[172.18.0.8:9866,DS-d1ea002b-fd89-492e-859f-891715ecd3e0,DISK]]
...
10. BP-1353449639-172.18.0.4-1792782054769:blk_1073741835_1011 len=1245515 Live_repl=2  [DatanodeInfoWithStorage[172.18.0.8:9866,DS-d1ea002b-fd89-492e-859f-891715ecd3e0,DISK], DatanodeInfoWithStorage[172.18.0.9:9866,DS-39ac2502-06db-4a6c-abdd-3ca8838fdbec,DISK]]

/bda/raw/unswnb15 <dir>
/bda/raw/unswnb15/test.parquet 9618034 bytes, replicated: replication=2, 1 block(s):  OK
0. BP-1353449639-172.18.0.4-1792782054769:blk_1073741836_1012 len=9618034 Live_repl=2  [DatanodeInfoWithStorage[172.18.0.9:9866,DS-39ac2502-06db-4a6c-abdd-3ca8838fdbec,DISK], DatanodeInfoWithStorage[172.18.0.8:9866,DS-d1ea002b-fd89-492e-859f-891715ecd3e0,DISK]]

/bda/raw/unswnb15/train.parquet 4538929 bytes, replicated: replication=2, 1 block(s):  OK
0. BP-1353449639-172.18.0.4-1792782054769:blk_1073741837_1013 len=4538929 Live_repl=2  [DatanodeInfoWithStorage[172.18.0.8:9866,DS-d1ea002b-fd89-492e-859f-891715ecd3e0,DISK], DatanodeInfoWithStorage[172.18.0.9:9866,DS-39ac2502-06db-4a6c-abdd-3ca8838fdbec,DISK]]

Status: HEALTHY
 Number of data-nodes:	2
 Number of racks:		1
 Total dirs:			4
 Total files:			4
 Total blocks (validated):	36 (avg. block size 15360818 B)
 Minimally replicated blocks:	36 (100.0 %)
 Over-replicated blocks:	0 (0.0 %)
 Under-replicated blocks:	0 (0.0 %)
 Mis-replicated blocks:		0 (0.0 %)
 Default replication factor:	2
 Average block replication:	2.0
 Missing blocks:		0
 Corrupt blocks:		0
 Missing replicas:		0 (0.0 %)

The filesystem under path '/bda/raw' is HEALTHY
```

---

## 6. PySpark Profiling Job Output

### Command
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1024m \
  --conf spark.cores.max=4 \
  /opt/spark/work-dir/src/ingest/verify_hdfs.py
```

### Real Output
```
======================================================================
BDA Phase 1 — HDFS Dataset Verification
======================================================================

--- cicids2017_flow ---
Path: hdfs://namenode:9000/bda/raw/cicids2017_flow/
Rows   : 2,827,677
Columns: 84
Schema :
root
 |-- Destination Port: long (nullable = true)
 |-- Flow Duration: long (nullable = true)
 |-- Total Fwd Packets: long (nullable = true)
 |-- Total Backward Packets: long (nullable = true)
 ...
 |-- Label: string (nullable = true)

Label distribution (column='Label'):
+----------------------------+-------+
|Label                       |count  |
+----------------------------+-------+
|BENIGN                      |2271122|
|DoS Hulk                    |230123 |
|PortScan                    |158804 |
|DDoS                        |128025 |
|DoS GoldenEye               |10293  |
|FTP-Patator                 |7935   |
|SSH-Patator                 |5897   |
|DoS slowloris               |5796   |
|DoS Slowhttptest            |5499   |
|Bot                         |1956   |
|Web Attack – Brute Force    |1507   |
|Web Attack – XSS            |652    |
|Infiltration                |36     |
|Web Attack – Sql Injection  |21     |
|Heartbleed                  |11     |
+----------------------------+-------+


--- cicids2017_packet ---
Path: hdfs://namenode:9000/bda/raw/cicids2017_packet/
Rows   : 177,693
Columns: 1509
Schema :
root
 |-- packet_id: long (nullable = true)
 |-- flow_id: string (nullable = true)
 |-- source_ip: string (nullable = true)
 |-- source_port: double (nullable = true)
 |-- destination_ip: string (nullable = true)
 |-- destination_port: double (nullable = true)
 |-- protocol: double (nullable = true)
 |-- payload_byte_1: long (nullable = true)
 ...
 |-- payload_byte_1500: long (nullable = true)
 |-- attack_label: string (nullable = true)

Label distribution (column='attack_label'):
+------------+------+
|attack_label|count |
+------------+------+
|BENIGN      |103967|
|DDoS        |73726 |
+------------+------+


--- unswnb15 ---
Path: hdfs://namenode:9000/bda/raw/unswnb15/
Rows   : 257,673
Columns: 36
Schema :
root
 |-- dur: float (nullable = true)
 |-- proto: string (nullable = true)
 |-- service: string (nullable = true)
 |-- state: string (nullable = true)
 |-- spkts: short (nullable = true)
 |-- dpkts: short (nullable = true)
 |-- sbytes: integer (nullable = true)
 |-- dbytes: integer (nullable = true)
 |-- rate: float (nullable = true)
 |-- sload: float (nullable = true)
 |-- dload: float (nullable = true)
 |-- sloss: short (nullable = true)
 |-- dloss: short (nullable = true)
 |-- sinpkt: float (nullable = true)
 |-- dinpkt: float (nullable = true)
 |-- sjit: float (nullable = true)
 |-- djit: float (nullable = true)
 |-- swin: short (nullable = true)
 |-- stcpb: long (nullable = true)
 |-- dtcpb: long (nullable = true)
 |-- dwin: short (nullable = true)
 |-- tcprtt: float (nullable = true)
 |-- synack: float (nullable = true)
 |-- ackdat: float (nullable = true)
 |-- smean: short (nullable = true)
 |-- dmean: short (nullable = true)
 |-- trans_depth: short (nullable = true)
 |-- response_body_len: integer (nullable = true)
 |-- ct_src_dport_ltm: byte (nullable = true)
 |-- ct_dst_sport_ltm: byte (nullable = true)
 |-- is_ftp_login: byte (nullable = true)
 |-- ct_ftp_cmd: byte (nullable = true)
 |-- ct_flw_http_mthd: byte (nullable = true)
 |-- is_sm_ips_ports: byte (nullable = true)
 |-- attack_cat: string (nullable = true)
 |-- label: byte (nullable = true)

Label distribution (column='label'):
+-----+------+
|label|count |
+-----+------+
|1    |164673|
|0    |93000 |
+-----+------+

======================================================================
Verification complete.
```

---

## 7. Storage and Resource Usage

### Docker Resource Footprint (`docker system df`)
```
TYPE            TOTAL     ACTIVE    SIZE      RECLAIMABLE
Images          4         4         5.826GB   0B (0%)
Containers      8         8         684kB     0B (0%)
Local Volumes   5         5         1.431GB   0B (0%)
Build Cache     23        0         2.594GB   2.594GB
```

### Host Drive Status
- **C: Free Space:** 9.97 GB
- **D: Free Space:** 35.31 GB
- **HDFS Total DFS Used:** 1.04 GB (across 2 replicas = ~527.4 MB net raw size)
- **Host Staging Dir (`data/staging`):** Cleared and empty
