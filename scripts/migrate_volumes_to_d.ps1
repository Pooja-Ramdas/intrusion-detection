param([switch]$Apply, [switch]$VerifyOnly)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$root = 'D:\bda'
Set-Location $repo

if (-not $Apply -and -not $VerifyOnly) {
    Write-Output 'Preview only. Pass -Apply to copy/start, or -VerifyOnly to compare existing named volumes with D: bind copies.'
    exit 0
}

function Invoke-Guard {
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot 'disk_guard.ps1')
    if ($LASTEXITCODE -ne 0) { throw 'Disk guard failed; migration stopped.' }
}

function Invoke-VolumeCopy([string]$Volume, [string]$Target) {
    Invoke-Guard
    $dockerTarget = $Target.Replace('\', '/')
    $copyCommand = 'set -o pipefail; tar -cpf - -C /src . | tar -xpf - -C /dst'
    & docker run --rm --user 0:0 --entrypoint /bin/bash `
        --mount "type=volume,source=$Volume,target=/src,readonly" `
        --mount "type=bind,source=$dockerTarget,target=/dst" `
        bda-spark:latest -lc $copyCommand
    if ($LASTEXITCODE -ne 0) { throw "Copy failed for volume $Volume to $Target" }
}

function Invoke-VolumeVerify([string]$Volume, [string]$Target, [string]$Mode = 'all') {
    Invoke-Guard
    $dockerTarget = $Target.Replace('\', '/')
    $repoDocker = $repo.Replace('\', '/')
    $output = & docker run --rm --user 0:0 --entrypoint /bin/bash `
        --mount "type=volume,source=$Volume,target=/src,readonly" `
        --mount "type=bind,source=$dockerTarget,target=/dst" `
        --mount "type=bind,source=$repoDocker,target=/workspace,readonly" `
        bda-spark:latest /workspace/scripts/verify_volume_copy.sh /src /dst $Mode
    if ($LASTEXITCODE -ne 0) { Write-Output ($output -join "`n"); throw "SHA-256 verification failed for volume $Volume to $Target ($Mode)" }
    Write-Output "$Volume -> $Target : $($output -join ' ')"
}

Invoke-Guard
$destinations = @(
    @{ Volume = 'bda_namenode_data'; Target = 'D:\bda\hdfs\nn' },
    @{ Volume = 'bda_datanode1_data'; Target = 'D:\bda\hdfs\dn1' },
    @{ Volume = 'bda_datanode2_data'; Target = 'D:\bda\hdfs\dn2' },
    @{ Volume = 'bda_mongo_data'; Target = 'D:\bda\mongo\data' },
    @{ Volume = '9d7bf3343d08b462a3d025ef7cab7137632d16935dc34fbcb4844b11f1cff9c7'; Target = 'D:\bda\mongo\configdb' }
)

if ($VerifyOnly) {
    Invoke-VolumeVerify 'bda_namenode_data' 'D:\bda\hdfs\nn' 'hdfs-nn'
    Invoke-VolumeVerify 'bda_datanode1_data' 'D:\bda\hdfs\dn1' 'hdfs-blocks'
    Invoke-VolumeVerify 'bda_datanode2_data' 'D:\bda\hdfs\dn2' 'hdfs-blocks'
    Invoke-VolumeVerify '9d7bf3343d08b462a3d025ef7cab7137632d16935dc34fbcb4844b11f1cff9c7' 'D:\bda\mongo\configdb' 'all'
    Write-Output 'MongoDB data volume is live on D:; source is a retained snapshot. Compare DB names/stats and service health instead of live WiredTiger file hashes.'
    exit 0
}

foreach ($item in $destinations) {
    $null = docker volume inspect $item.Volume
    if ($LASTEXITCODE -ne 0) { throw "Expected current source volume is missing: $($item.Volume)" }
    New-Item -ItemType Directory -Path $item.Target -Force | Out-Null
    if (Get-ChildItem -LiteralPath $item.Target -Force -ErrorAction SilentlyContinue | Select-Object -First 1) {
        throw "Destination is not empty; preserving it and aborting: $($item.Target)"
    }
}

docker compose stop
if ($LASTEXITCODE -ne 0) { throw 'Could not stop Compose services cleanly; no volume copy started.' }
$copyComplete = $false
try {
    foreach ($item in $destinations) { Invoke-VolumeCopy $item.Volume $item.Target }
    foreach ($item in $destinations) { Invoke-VolumeVerify $item.Volume $item.Target }
    $copyComplete = $true

    Invoke-Guard
    docker compose -f docker-compose.yml -f docker-compose.storage-d.yml up -d
    if ($LASTEXITCODE -ne 0) { throw 'Could not start the D: bind-mounted Compose configuration.' }

    foreach ($service in @('namenode','datanode1','datanode2','spark-master','spark-worker-1','spark-worker-2','mongodb','app')) {
        $ready = $false
        for ($attempt = 0; $attempt -lt 24; $attempt++) {
            $state = docker inspect --format='{{.State.Health.Status}}' $service 2>$null
            if ($state -eq 'healthy') { $ready = $true; break }
            Start-Sleep -Seconds 5
        }
        if (-not $ready) { throw "Service did not become healthy: $service" }
    }

    $fsck = docker exec namenode hdfs fsck /bda
    if ($LASTEXITCODE -ne 0 -or ($fsck -join "`n") -notmatch "The filesystem under path '/bda' is HEALTHY") {
        throw 'HDFS post-migration fsck did not report HEALTHY.'
    }
    $mongoPing = docker exec mongodb mongosh --quiet --eval "db.adminCommand('ping').ok"
    if ($LASTEXITCODE -ne 0 -or ($mongoPing -join '').Trim() -ne '1') { throw 'MongoDB post-migration ping failed.' }
    Write-Output 'D: bind-mounted cluster is healthy.'
    docker exec namenode hdfs dfs -du -h /bda
} catch {
    Write-Error $_
    docker compose -f docker-compose.yml -f docker-compose.storage-d.yml stop
    docker compose up -d
    if ($LASTEXITCODE -eq 0) { Write-Warning 'Rolled back to the original named-volume configuration; named volumes were preserved.' }
    throw
}
