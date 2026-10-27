$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
& (Join-Path $PSScriptRoot 'disk_guard.ps1')
if ($LASTEXITCODE -ne 0) { throw 'disk_guard failed' }
foreach ($n in @('namenode','datanode1','datanode2','spark-master','spark-worker-1','spark-worker-2','mongodb','app')) {
  $s = docker inspect --format '{{.State.Status}}' $n
  if ($LASTEXITCODE -ne 0 -or $s -ne 'running') { throw "$n is not running ($s)" }
  Write-Host "PASS container $n running"
}
docker exec namenode hdfs dfs -ls /bda | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'HDFS listing failed' }
docker exec namenode hdfs dfsadmin -report | Select-String 'Live datanodes' | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'HDFS report failed' }
foreach ($u in @('http://localhost:8080','http://localhost:8501/_stcore/health')) {
  $r=Invoke-WebRequest -UseBasicParsing $u -TimeoutSec 8
  if ($r.StatusCode -ne 200) { throw "HTTP check failed $u" }
  Write-Host "PASS HTTP $u $($r.StatusCode)"
}
$mongo = docker exec mongodb mongosh --quiet --eval "db.adminCommand('ping').ok"
if ($LASTEXITCODE -ne 0 -or $mongo.Trim() -ne '1') { throw 'Mongo ping failed' }
Write-Host 'PASS MongoDB ping'
$idx = docker exec mongodb mongosh --quiet --eval "db.getSiblingDB('bda').flows.getIndexes().map(x=>x.name).join(',')"
foreach ($n in @('label_idx','timestamp_idx','ip_pair_idx','label_time_idx','entropy_idx')) {
  if ($idx -notmatch [regex]::Escape($n)) { throw "Missing Mongo index $n" }
}
Write-Host "PASS Mongo indexes: $idx"
$count = docker exec mongodb mongosh --quiet --eval "db.getSiblingDB('bda').flows.countDocuments({})"
Write-Host "NOTICE bda.flows documents: $count (no dataset ingestion performed)"
