$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
& (Join-Path $PSScriptRoot 'disk_guard.ps1')
if ($LASTEXITCODE -ne 0) { throw 'Disk guard failed; deployment stopped.' }
docker compose -f docker-compose.yml -f docker-compose.storage-d.yml up -d
if ($LASTEXITCODE -ne 0) { throw 'Compose startup failed.' }
& (Join-Path $PSScriptRoot 'smoke_test.ps1')
