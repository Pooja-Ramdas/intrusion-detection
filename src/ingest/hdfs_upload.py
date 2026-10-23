"""
hdfs_upload.py — Upload staged files to HDFS then print verification.
Run inside namenode container after download_and_stage.py completes.
Usage: docker exec namenode python /tmp/hdfs_upload.py (copy this script in first)
"""
# This script is invoked via shell commands in scripts/up.ps1 / phase1 steps.
# The actual upload uses hdfs dfs -put via docker exec.
# See scripts/ingest_phase1.ps1 for the orchestration.
