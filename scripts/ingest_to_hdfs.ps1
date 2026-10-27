param(
    [switch]$Approved,
    [string]$PlanPath = (Join-Path (Split-Path -Parent $PSScriptRoot) 'artifacts\ingest_plan.json'),
    [string]$DataVersion,
    [int]$Seed = 0
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$root = if ($env:BDA_ROOT) { $env:BDA_ROOT } else { 'D:\bda' }
$stage = Join-Path $root 'staging'
$maxStageBytes = 4000000000L
$logPath = Join-Path $root '_meta\ingest_log.jsonl'
$helper = Join-Path $repo 'src\ingest\download_one.py'

if (-not $Approved) { throw 'No download is allowed until you explicitly approve the printed plan; rerun with -Approved only after approval.' }
if (-not $DataVersion) { throw 'Supply -DataVersion for the approved source revision.' }
if (-not (Test-Path -LiteralPath $PlanPath)) { throw "Plan not found: $PlanPath" }
$plan = Get-Content -LiteralPath $PlanPath -Raw | ConvertFrom-Json
if ($plan.status -notin @('proposed_pending_explicit_approval','approved_by_user')) { throw "Unexpected manifest status: $($plan.status)" }

# Require the host staging directory to be bind-mounted into app before any download.
$mountJson = & docker inspect app --format '{{json .Mounts}}'
if ($LASTEXITCODE -ne 0) { throw 'Could not inspect app mounts; ingestion was not started.' }
$mounts = $mountJson | ConvertFrom-Json
$stageMount = @($mounts | Where-Object { $_.Destination -eq '/bda/staging' }) | Select-Object -First 1
$stageSource = if ($stageMount) { ([string]$stageMount.Source).Replace('\','/').TrimEnd('/') } else { '' }
if (-not $stageMount -or $stageMount.Type -ne 'bind' -or $stageSource -ine 'D:/bda/staging') {
    throw 'The D:\bda\staging bind mount is not active on app; complete the documented storage migration first.'
}
New-Item -ItemType Directory -Path $stage -Force | Out-Null
New-Item -ItemType Directory -Path (Split-Path -Parent $logPath) -Force | Out-Null

function Invoke-Hdfs([string[]]$CmdArgs) {
    $output = & docker exec namenode hdfs dfs @CmdArgs
    if ($LASTEXITCODE -ne 0) { throw "HDFS command failed: hdfs dfs $($CmdArgs -join ' ')" }
    return ($output | Out-String).Trim()
}

function Get-HdfsSha([string]$Path) {
    if ($Path -notmatch '^/([A-Za-z0-9._-]+/)*[A-Za-z0-9._-]+$') { throw "Refusing unexpected HDFS path: $Path" }
    $out = & docker exec namenode bash -c "set -o pipefail; /opt/hadoop-3.2.1/bin/hdfs dfs -cat '$Path' | sha256sum"
    if ($LASTEXITCODE -ne 0) { throw "HDFS checksum failed for $Path" }
    return (($out | Out-String).Trim() -split '\s+')[0]
}

function Write-Log([hashtable]$Record) {
    $json = $Record | ConvertTo-Json -Compress -Depth 20
    [System.IO.File]::AppendAllText($logPath, $json + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))
}

function Send-LocalFileToHdfs([string]$LocalPath, [string]$RemotePath) {
    $procInfo = [Diagnostics.ProcessStartInfo]::new()
    $procInfo.FileName = 'docker'
    $procInfo.Arguments = "exec -i namenode hdfs dfs -put - $RemotePath"
    $procInfo.UseShellExecute = $false
    $procInfo.RedirectStandardInput = $true
    $procInfo.CreateNoWindow = $true
    $proc = [Diagnostics.Process]::Start($procInfo)
    $source = [IO.File]::OpenRead($LocalPath)
    try { $source.CopyTo($proc.StandardInput.BaseStream, 8MB) }
    finally { $source.Dispose(); $proc.StandardInput.Close() }
    $proc.WaitForExit()
    if ($proc.ExitCode -ne 0) { throw "HDFS chunk upload failed; staging retained: $LocalPath" }
}

function Invoke-ChunkedFile([object]$Item, [long]$ExpectedSize, [string]$Destination, [hashtable]$RunInfo) {
    $chunkBytes = 512MB
    $sourceHash = [string]$Item.sha256
    $partial = '/tmp/bda-ingest/' + [IO.Path]::GetFileName($Destination) + '.' + $sourceHash.Substring(0,12) + '.partial'
    $chunkDir = Join-Path (Join-Path $stage 'current_download\chunks') $sourceHash
    New-Item -ItemType Directory -Path $chunkDir -Force | Out-Null
    docker cp (Join-Path $repo 'src\ingest\download_range_chunk.py') app:/tmp/bda_download_range_chunk.py
    if ($LASTEXITCODE -ne 0) { throw 'Could not copy range downloader into app.' }
    $null = Invoke-Hdfs @('-mkdir', '-p', '/tmp/bda-ingest/chunks')
    $null = Invoke-Hdfs @('-mkdir', '-p', [IO.Path]::GetDirectoryName($partial))
    $null = & docker exec namenode hdfs dfs -test -e $partial 2>$null
    $exists = $LASTEXITCODE
    $offset = if ($exists -eq 0) { [long](Invoke-Hdfs @('-stat', '%b', $partial)) } else { 0L }
    if ($offset -gt $ExpectedSize) { throw "HDFS partial exceeds source size: $partial" }
    foreach ($stale in Get-ChildItem -LiteralPath $chunkDir -Filter '*.chunk' -File -ErrorAction SilentlyContinue) {
        if ($stale.BaseName -match '^(\d{12})$' -and ([long]$Matches[1] + $stale.Length) -le $offset) { Remove-Item -LiteralPath $stale.FullName -Force }
    }

    while ($offset -lt $ExpectedSize) {
        $guardText = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot 'disk_guard.ps1')
        $guardCode = $LASTEXITCODE
        $guardText | ForEach-Object { Write-Host $_ }
        if ($guardCode -ne 0) { throw 'Disk guard failed before next range; ingestion paused.' }
        $length = [Math]::Min([long]$chunkBytes, [long]($ExpectedSize - $offset))
        $chunkName = "$($offset.ToString('D12')).chunk"
        $chunkPath = Join-Path $chunkDir $chunkName
        $partPath = "$chunkPath.part"
        $partExisting = if (Test-Path -LiteralPath $partPath) { (Get-Item -LiteralPath $partPath).Length } else { 0 }
        $stagedNow = (Get-ChildItem -LiteralPath $stage -File -Recurse -Force -ErrorAction SilentlyContinue | Measure-Object Length -Sum).Sum
        if (-not $stagedNow) { $stagedNow = 0 }
        if ($stagedNow + [Math]::Max(0, $length - $partExisting) -gt $maxStageBytes) { throw 'Chunk would exceed 4 GB staging cap.' }
        if (-not (Test-Path -LiteralPath $chunkPath)) {
            $containerOut = "/bda/staging/current_download/chunks/$sourceHash/$chunkName"
            $downloadArgs = @('exec','app','python3','/tmp/bda_download_range_chunk.py','--approved','--repo-id',[string]$Item.repo_id,'--filename',[string]$Item.path,'--revision',$DataVersion,'--offset',[string]$offset,'--length',[string]$length,'--total-size',[string]$ExpectedSize,'--output',$containerOut)
            & docker @downloadArgs
            if ($LASTEXITCODE -ne 0) { throw "Range download paused; resumable staging retained: $partPath" }
        }
        if ((Get-Item -LiteralPath $chunkPath).Length -ne $length) { throw "Staged range has unexpected size: $chunkPath" }
        $chunkHash = (Get-FileHash -LiteralPath $chunkPath -Algorithm SHA256).Hash.ToLowerInvariant()
        $chunkHdfs = "/tmp/bda-ingest/chunks/$sourceHash.$offset.chunk"
        $null = & docker exec namenode hdfs dfs -test -e $chunkHdfs 2>$null
        if ($LASTEXITCODE -eq 0) {
            if ([long](Invoke-Hdfs @('-stat','%b',$chunkHdfs)) -ne $length -or (Get-HdfsSha $chunkHdfs) -ne $chunkHash) { throw "HDFS range temp conflicts: $chunkHdfs" }
        } else {
            Send-LocalFileToHdfs $chunkPath $chunkHdfs
            if ([long](Invoke-Hdfs @('-stat','%b',$chunkHdfs)) -ne $length -or (Get-HdfsSha $chunkHdfs) -ne $chunkHash) { throw "HDFS range temp failed verification: $chunkHdfs" }
        }
        if ($offset -eq 0 -and $exists -ne 0) {
            $null = Invoke-Hdfs @('-mv',$chunkHdfs,$partial)
        } else {
            $currentLength = [long](Invoke-Hdfs @('-stat','%b',$partial))
            if ($currentLength -ne $offset) { throw "HDFS partial offset changed unexpectedly: $currentLength vs $offset" }
            $append = & docker exec namenode bash -c "set -o pipefail; /opt/hadoop-3.2.1/bin/hdfs dfs -cat '$chunkHdfs' | /opt/hadoop-3.2.1/bin/hdfs dfs -appendToFile - '$partial'"
            if ($LASTEXITCODE -ne 0) { throw "Could not append verified HDFS range $chunkHdfs to $partial" }
            $null = Invoke-Hdfs @('-rm',$chunkHdfs)
        }
        $exists = 0
        $newOffset = [long](Invoke-Hdfs @('-stat','%b',$partial))
        if ($newOffset -ne $offset + $length) { throw "HDFS partial range size mismatch at $newOffset" }
        Remove-Item -LiteralPath $chunkPath -Force
        Write-Log (@{ event='chunk_verified'; utc=[DateTime]::UtcNow.ToString('o'); path=$Destination; offset=$offset; bytes=$length; chunk_sha256=$chunkHash; hdfs_partial=$partial } + $RunInfo)
        $offset = $newOffset
    }
    if ([long](Invoke-Hdfs @('-stat','%b',$partial)) -ne $ExpectedSize -or (Get-HdfsSha $partial) -ne $sourceHash) { throw "Assembled source failed full size/SHA-256; partial retained: $partial" }
    $null = Invoke-Hdfs @('-mv',$partial,$Destination)
    $null = Invoke-Hdfs @('-setrep','-w','1',$Destination)
    if ([long](Invoke-Hdfs @('-stat','%b',$Destination)) -ne $ExpectedSize -or (Get-HdfsSha $Destination) -ne $sourceHash -or [int](Invoke-Hdfs @('-stat','%r',$Destination)) -ne 1) { throw "Final HDFS file failed verification: $Destination" }
    Write-Log (@{ event='uploaded_verified'; utc=[DateTime]::UtcNow.ToString('o'); path=$Destination; bytes=$ExpectedSize; sha256=$sourceHash; replication=1 } + $RunInfo)
}

$gitHash = (& git -C $repo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Could not read Git hash for ingestion log.' }
$run = @{
    command = "scripts\ingest_to_hdfs.ps1 -Approved -PlanPath `"$PlanPath`" -DataVersion `"$DataVersion`" -Seed $Seed"
    git_hash = $gitHash
    data_version = $DataVersion
    seed = $Seed
}
Write-Log (@{ event = 'run_start'; utc = [DateTime]::UtcNow.ToString('o') } + $run)

# Reuse prior cryptographic verification during resume. File size is still
# checked in HDFS; the final independent audit re-hashes every destination.
$verifiedLog = @{}
if (Test-Path -LiteralPath $logPath) {
    foreach ($line in Get-Content -LiteralPath $logPath) {
        try {
            $record = $line | ConvertFrom-Json
            if ($record.event -in @('uploaded_verified','verified_existing_skip') -and $record.path -and $record.sha256 -and $record.bytes) {
                $verifiedLog[[string]$record.path] = $record
            }
        } catch { }
    }
}

$legacyCache = Join-Path $stage 'current_download\.huggingface'
if (Test-Path -LiteralPath $legacyCache) {
    $recoveryRoot = Join-Path $root '_meta\interrupted_hf_cache'
    New-Item -ItemType Directory -Path $recoveryRoot -Force | Out-Null
    $recoveryPath = Join-Path $recoveryRoot ([DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ'))
    $cacheBytes = (Get-ChildItem -LiteralPath $legacyCache -File -Recurse -Force | Measure-Object Length -Sum).Sum
    Move-Item -LiteralPath $legacyCache -Destination $recoveryPath
    Write-Log (@{ event='preserved_interrupted_cache_outside_staging'; utc=[DateTime]::UtcNow.ToString('o'); bytes=$cacheBytes; path=$recoveryPath } + $run)
}

$files = @($plan.plan.cicids2017_packet.files) + @($plan.plan.cicids2017_flow.files) + @($plan.plan.unswnb15.files)
foreach ($item in $files) {
    $guardText = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot 'disk_guard.ps1')
    $guardCode = $LASTEXITCODE
    $guardText | ForEach-Object { Write-Host $_ }
    if ($guardCode -ne 0) { throw 'Disk guard failed; ingestion aborted before the next file.' }

    $expectedSize = [long]$item.size_bytes
    $destination = [string]$item.destination
    $present = & docker exec namenode hdfs dfs -test -e $destination 2>$null
    $isPresent = ($LASTEXITCODE -eq 0)
    if ($isPresent) {
        $existingSize = [long](Invoke-Hdfs @('-stat', '%b', $destination))
        $priorVerified = $verifiedLog[$destination]
        if ($priorVerified -and [long]$priorVerified.bytes -eq $expectedSize -and [string]$priorVerified.sha256 -eq [string]$item.sha256) {
            if ($existingSize -ne $expectedSize) { throw "Previously verified HDFS file size changed; preserved: $destination" }
            Write-Log (@{ event = 'verified_existing_skip'; utc = [DateTime]::UtcNow.ToString('o'); path = $destination; bytes = $existingSize; sha256 = [string]$item.sha256; verification = 'prior logged SHA-256 plus current HDFS size' } + $run)
            continue
        }
        $existingHash = Get-HdfsSha $destination
        if ($existingSize -ne $expectedSize -or $existingHash -ne [string]$item.sha256) {
            throw "Existing HDFS file conflicts with the source; it was preserved: $destination"
        }
        Write-Log (@{ event = 'verified_existing_skip'; utc = [DateTime]::UtcNow.ToString('o'); path = $destination; bytes = $existingSize; sha256 = $existingHash } + $run)
        $verifiedLog[$destination] = @{ bytes = $existingSize; sha256 = $existingHash; event = 'verified_existing_skip' }
        continue
    }

    # Very large Hub transfers have stalled in hf_hub_download before writing
    # any bytes. Keep those files on the tested 512 MiB resumable range path.
    if ($expectedSize -gt 3000000000L) {
        Invoke-ChunkedFile $item $expectedSize $destination $run
        continue
    }

    $staged = Join-Path $stage ("current_download\" + [IO.Path]::GetFileName([string]$item.path))
    $currentStageBytes = (Get-ChildItem -LiteralPath $stage -File -Recurse -ErrorAction SilentlyContinue | Measure-Object Length -Sum).Sum
    if (-not $currentStageBytes) { $currentStageBytes = 0 }
    if ($currentStageBytes -gt $maxStageBytes) {
        throw 'Existing D: staging exceeds 4 GB; no download started.'
    }
    if (-not (Test-Path -LiteralPath $staged) -and ($currentStageBytes + $expectedSize -gt $maxStageBytes)) {
        $allFiles = @(Get-ChildItem -LiteralPath $stage -File -Recurse -Force -ErrorAction SilentlyContinue)
        $partialFiles = @($allFiles | Where-Object { $_.FullName -match '\\current_download\\\.cache\\huggingface\\download\\' })
        $otherFiles = @($allFiles | Where-Object { $_.FullName -notmatch '\\current_download\\\.cache\\huggingface\\download\\' -and $_.FullName -notmatch '\\.hf\\' })
        if ($partialFiles.Count -eq 0 -or $otherFiles.Count -gt 0 -or ($partialFiles | Measure-Object Length -Sum).Sum -gt $expectedSize) {
            throw 'The next source file would exceed the 4 GB staging cap; no download started.'
        }
    }

    $stagedValid = $false
    if (Test-Path -LiteralPath $staged) {
        $stagedInfo = Get-Item -LiteralPath $staged
        if ($stagedInfo.Length -eq $expectedSize -and (Get-FileHash -LiteralPath $staged -Algorithm SHA256).Hash.ToLowerInvariant() -eq [string]$item.sha256) {
            $stagedValid = $true
        } else {
            Write-Log (@{ event='discard_incomplete_stage_for_resume'; utc=[DateTime]::UtcNow.ToString('o'); path=[string]$item.path; staged_bytes=$stagedInfo.Length } + $run)
            Remove-Item -LiteralPath $staged -Force
        }
    }
    if (-not $stagedValid) {
        docker cp $helper app:/tmp/bda_download_one.py
        if ($LASTEXITCODE -ne 0) { throw 'Could not copy the one-file downloader into the app container.' }
        $containerOut = '/bda/staging/current_download/' + [IO.Path]::GetFileName([string]$item.path)
        $downloadArgs = @(
            'exec','app','python3','/tmp/bda_download_one.py','--approved',
            '--repo-id',[string]$item.repo_id,'--filename',[string]$item.path,
            '--revision',$DataVersion,
            '--output',$containerOut,'--size-bytes',[string]$expectedSize,
            '--sha256',[string]$item.sha256
        )
        & docker @downloadArgs
        if ($LASTEXITCODE -ne 0) { throw "Download failed; any verified staging bytes were retained for resume: $($item.path)" }
    }

    if (-not (Test-Path -LiteralPath $staged)) { throw "Expected D: staging file is not visible; ensure storage override is active: $staged" }
    if ((Get-Item -LiteralPath $staged).Length -ne $expectedSize -or (Get-FileHash -LiteralPath $staged -Algorithm SHA256).Hash.ToLowerInvariant() -ne [string]$item.sha256) {
        throw "Staged file failed size/SHA-256 verification and was retained: $staged"
    }

    $null = Invoke-Hdfs @('-mkdir', '-p', [IO.Path]::GetDirectoryName($destination).Replace('\','/'))
    $null = Invoke-Hdfs @('-mkdir', '-p', '/tmp/bda-ingest')
    $partial = '/tmp/bda-ingest/' + [IO.Path]::GetFileName($destination) + '.' + ([string]$item.sha256).Substring(0,12) + '.partial'
    $partialExists = & docker exec namenode hdfs dfs -test -e $partial 2>$null
    if ($LASTEXITCODE -eq 0) { $null = Invoke-Hdfs @('-rm', '-f', $partial) }

    $procInfo = [Diagnostics.ProcessStartInfo]::new()
    $procInfo.FileName = 'docker'
    $procInfo.Arguments = "exec -i namenode hdfs dfs -put - $partial"
    $procInfo.UseShellExecute = $false
    $procInfo.RedirectStandardInput = $true
    $procInfo.CreateNoWindow = $true
    $proc = [Diagnostics.Process]::Start($procInfo)
    $source = [IO.File]::OpenRead($staged)
    try { $source.CopyTo($proc.StandardInput.BaseStream, 8MB) }
    finally { $source.Dispose(); $proc.StandardInput.Close() }
    $proc.WaitForExit()
    if ($proc.ExitCode -ne 0) { throw "HDFS upload failed; source retained for retry: $staged" }

    $remoteSize = [long](Invoke-Hdfs @('-stat', '%b', $partial))
    $remoteHash = Get-HdfsSha $partial
    if ($remoteSize -ne $expectedSize -or $remoteHash -ne [string]$item.sha256) {
        throw "HDFS temporary upload failed verification; source retained: $staged"
    }
    $null = Invoke-Hdfs @('-mv', $partial, $destination)
    $null = Invoke-Hdfs @('-setrep', '-w', '1', $destination)
    $finalSize = [long](Invoke-Hdfs @('-stat', '%b', $destination))
    $finalHash = Get-HdfsSha $destination
    $finalReplication = [int](Invoke-Hdfs @('-stat', '%r', $destination))
    if ($finalSize -ne $expectedSize -or $finalHash -ne [string]$item.sha256 -or $finalReplication -ne 1) {
        throw "Final HDFS file failed verification; source retained: $staged"
    }

    Write-Log (@{ event = 'uploaded_verified'; utc = [DateTime]::UtcNow.ToString('o'); path = $destination; bytes = $finalSize; sha256 = $finalHash } + $run)
    Remove-Item -LiteralPath $staged
    $cache = Join-Path $stage '.hf'
    if (Test-Path -LiteralPath $cache) { Remove-Item -LiteralPath $cache -Recurse -Force }
}

Write-Log (@{ event = 'run_complete'; utc = [DateTime]::UtcNow.ToString('o') } + $run)
