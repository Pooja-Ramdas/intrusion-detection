# down.ps1 — Stop the BDA cluster (preserves named volumes)
$env:PATH = [System.Environment]::GetEnvironmentVariable("PATH","Machine") + ";" +
            [System.Environment]::GetEnvironmentVariable("PATH","User")
Set-Location $PSScriptRoot\..
docker compose -f docker-compose.yml -f docker-compose.storage-d.yml down
Write-Host "Cluster stopped. Named volumes preserved." -ForegroundColor Yellow
