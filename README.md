# Distributed Multiclass Intrusion Detection and Traffic Profiling

Distributed network flow and packet-level intrusion detection system using **Hadoop HDFS**, **Apache Spark MLlib**, **MongoDB**, and **Docker**.

---

## 3-Command Quickstart

```powershell
# 1. Build the unified Spark container image
docker compose build

# 2. Start the distributed cluster (HDFS, Spark Standalone, Mongo, Dashboard)
docker compose up -d

# 3. Run Phase 1 data pipeline (Discover, Download & Stage, HDFS Ingest & Profile)
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 1g --executor-memory 1024m /opt/spark/work-dir/src/ingest/verify_hdfs.py
```

---

## Web UIs & Service Endpoints

| Service | Host Port | Internal Address | Purpose |
|---|---|---|---|
| **Spark Master Web UI** | [http://localhost:8080](http://localhost:8080) | `spark-master:8080` | Cluster overview, workers, application progress |
| **HDFS NameNode UI** | [http://localhost:9870](http://localhost:9870) | `namenode:9870` | HDFS filesystem browser, DataNode status |
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | `app:8501` | Interactive analytics & model performance |
| **Spark Application UI** | [http://localhost:4040](http://localhost:4040) | `spark-master:4040` | Active job DAG, task stages |
| **HDFS RPC** | `localhost:9000` | `namenode:9000` | Hadoop filesystem RPC |
| **MongoDB** | `localhost:27017` | `mongodb:27017` | Document store for flow-packet investigations |

---

## Architecture & Distribution

- **HDFS Cluster:** 1 NameNode + 2 DataNodes, replication factor = 2, block size = 16 MB.
- **Spark Cluster:** Standalone Master + 2 Workers (`1500m` memory, `2` cores each).
- **Storage Paths:**
  - `/bda/raw/cicids2017_flow/`: 2,827,677 flow records across all 5 days and 15 attack classes.
  - `/bda/raw/cicids2017_packet/`: 177,693 packets with 1,500 payload byte statistics.
  - `/bda/raw/unswnb15/`: 257,673 UNSW-NB15 flow records for cross-validation.

---

## Utility Scripts

- `scripts\up.ps1` — Start all services with automated health checks.
- `scripts\down.ps1` — Gracefully stop all containers while preserving data volumes.
- `scripts\cleanup.ps1` — Reclaim build cache and dangling images without touching dataset volumes.
