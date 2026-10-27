param(
    [double]$MinimumFreeGB = 12.0
)

$ErrorActionPreference = 'Continue'
$drive = Get-Volume -DriveLetter D -ErrorAction SilentlyContinue
$freeBytes = if ($drive) { [double]$drive.SizeRemaining } else {
    # Get-Volume can be unavailable in restricted PowerShell hosts; PSDrive uses
    # the same mounted volume and provides a direct free-byte measurement.
    $psDrive = Get-PSDrive -Name D -ErrorAction SilentlyContinue
    if ($psDrive) { [double]$psDrive.Free } else { $null }
}
if ($null -eq $freeBytes -or $freeBytes -lt 0) {
    Write-Error 'D: free space could not be measured; aborting.'
    exit 2
}
$freeGB = [math]::Round($freeBytes / 1GB, 2)
Write-Output ("D: free: {0} GB (required floor: {1} GB)" -f $freeGB, $MinimumFreeGB)

$hdfs = docker exec namenode hdfs dfs -du -s /bda 2>$null
if ($LASTEXITCODE -eq 0 -and $hdfs) {
    $cols = ($hdfs -split '\s+')
    if ($cols.Count -ge 3) {
        Write-Output ("HDFS /bda: logical={0} bytes, physical={1} bytes" -f $cols[0], $cols[1])
    } else { Write-Output "HDFS /bda: $hdfs" }
} else { Write-Output 'HDFS /bda: unavailable (not measured)' }

$mongo = docker exec mongodb mongosh --quiet --eval "JSON.stringify(db.adminCommand({listDatabases:1}).databases.map(x=>({name:x.name,sizeOnDisk:x.sizeOnDisk})))" 2>$null
if ($LASTEXITCODE -eq 0 -and $mongo) {
    Write-Output "MongoDB databases (sizeOnDisk bytes): $mongo"
} else { Write-Output 'MongoDB: unavailable (not measured)' }
$mongoVolume = docker system df -v 2>$null | Where-Object { $_ -match '^\s*bda_mongo_data\s+' } | Select-Object -First 1
if ($mongoVolume) { Write-Output "MongoDB volume (Docker-reported physical): $($mongoVolume.Trim())" }

$root = if ($env:BDA_ROOT) { $env:BDA_ROOT } else { 'D:\bda' }
$staging = Join-Path $root 'staging'
if (Test-Path $staging) {
    $stageBytes = (Get-ChildItem -LiteralPath $staging -File -Recurse -ErrorAction SilentlyContinue | Measure-Object Length -Sum).Sum
    if (-not $stageBytes) { $stageBytes = 0 }
    Write-Output ("D: staging: {0} bytes" -f $stageBytes)
} else { Write-Output 'D: staging: 0 bytes (directory absent)' }

if ($freeGB -lt $MinimumFreeGB) {
    Write-Error "ABORT: D: has less than $MinimumFreeGB GB free."
    exit 1
}
exit 0
