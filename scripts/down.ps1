# down.ps1 — Stop the BDA cluster (preserves named volumes)
$env:PATH = [System.Environment]::GetEnvironmentVariable("PATH","Machine") + ";" +
            [System.Environment]::GetEnvironmentVariable("PATH","User")
Set-Location $PSScriptRoot\..
docker compose down
Write-Host "Cluster stopped. Named volumes preserved." -ForegroundColor Yellow
