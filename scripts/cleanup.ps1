# cleanup.ps1
# Reclaims Docker build cache and dangling images.
# NOTE: docker system prune -af --volumes is intentionally NOT used here
#       because it would wipe HDFS and MongoDB named volumes, destroying
#       all cluster data. Only builder cache and dangling images are removed.

$env:PATH = [System.Environment]::GetEnvironmentVariable("PATH","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH","User")

Write-Host "=== Pruning build cache ===" -ForegroundColor Cyan
docker builder prune -af

Write-Host ""
Write-Host "=== Pruning dangling images ===" -ForegroundColor Cyan
docker image prune -f

Write-Host ""
Write-Host "=== Docker disk usage after cleanup ===" -ForegroundColor Cyan
docker system df
