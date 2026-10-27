#!/usr/bin/env bash
set -euo pipefail

# Run inside namenode with: docker exec -i namenode bash -s < scripts/hdfs_zones.sh
hdfs dfs -mkdir -p /bda/raw /bda/clean /bda/features /bda/models
hdfs dfs -setrep -R 1 /bda/raw
for zone in clean features models; do
  hdfs dfs -setrep -R 2 "/bda/${zone}"
  hdfs dfs -setrep 2 "/bda/${zone}"
done

echo "Configured HDFS zone replication."
hdfs dfs -du -h /bda
