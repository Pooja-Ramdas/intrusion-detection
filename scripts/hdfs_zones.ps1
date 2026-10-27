$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)

powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot 'disk_guard.ps1')
if ($LASTEXITCODE -ne 0) { throw 'Disk guard failed; HDFS zone settings were not changed.' }

docker cp scripts/hdfs_zones.sh namenode:/tmp/hdfs_zones.sh
if ($LASTEXITCODE -ne 0) { throw 'Could not copy hdfs_zones.sh to the NameNode.' }
docker exec namenode bash /tmp/hdfs_zones.sh
if ($LASTEXITCODE -ne 0) { throw 'HDFS zone replication command failed.' }
