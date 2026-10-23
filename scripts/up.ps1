# up.ps1 — Start the BDA cluster
param([switch]$Build)

$ErrorActionPreference = "Stop"
$env:PATH = [System.Environment]::GetEnvironmentVariable("PATH","Machine") + ";" +
            [System.Environment]::GetEnvironmentVariable("PATH","User")

Set-Location $PSScriptRoot\..

if ($Build) {
    Write-Host "Building bda-spark image..." -ForegroundColor Cyan
    docker compose build --no-cache
}

Write-Host "Starting cluster..." -ForegroundColor Cyan
docker compose up -d

Write-Host "Waiting for services to become healthy..." -ForegroundColor Yellow
$services = @("namenode","datanode1","datanode2","spark-master","mongodb")
foreach ($svc in $services) {
    $attempts = 0
    while ($attempts -lt 20) {
        $health = docker inspect --format='{{.State.Health.Status}}' $svc 2>$null
        if ($health -eq "healthy") { Write-Host "  $svc : healthy" -ForegroundColor Green; break }
        Start-Sleep -Seconds 10
        $attempts++
    }
    if ($attempts -ge 20) { Write-Warning "$svc did not become healthy in time." }
}

Write-Host ""
Write-Host "Cluster URLs:" -ForegroundColor Cyan
Write-Host "  HDFS Web UI  : http://localhost:9870"
Write-Host "  Spark Web UI : http://localhost:8080"
Write-Host "  Streamlit    : http://localhost:8501"
Write-Host "  MongoDB      : localhost:27017"
