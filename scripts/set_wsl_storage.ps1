param([switch]$Apply)

$ErrorActionPreference = 'Stop'
$guard = Join-Path $PSScriptRoot 'disk_guard.ps1'
powershell.exe -NoProfile -ExecutionPolicy Bypass -File $guard
if ($LASTEXITCODE -ne 0) { throw 'Disk guard failed; WSL storage configuration was not changed.' }

$configPath = Join-Path $env:USERPROFILE '.wslconfig'
$backupPath = "$configPath.bda-backup"
$swapDir = 'D:\wsl'
New-Item -ItemType Directory -Path $swapDir -Force | Out-Null

if (Test-Path $configPath) {
    $content = Get-Content -LiteralPath $configPath -Raw
    if (-not (Test-Path $backupPath)) { Copy-Item -LiteralPath $configPath -Destination $backupPath }
} else {
    $content = "[wsl2]`r`nmemory=8GB`r`nprocessors=6`r`nswap=4GB`r`n"
}

if ($content -match '(?im)^\s*swapFile\s*=') {
    $content = [regex]::Replace($content, '(?im)^\s*swapFile\s*=.*$', 'swapFile=D:\\wsl\\swap.vhdx')
} elseif ($content -match '(?im)^\s*swap\s*=') {
    $content = [regex]::Replace($content, '(?im)^\s*swap\s*=.*$', { param($m) "$($m.Value)`r`nswapFile=D:\\wsl\\swap.vhdx" }, 1)
} else {
    $content = $content.TrimEnd() + "`r`nswap=4GB`r`nswapFile=D:\\wsl\\swap.vhdx`r`n"
}

if ($Apply) {
    Set-Content -LiteralPath $configPath -Value $content -Encoding utf8
    Write-Output "Updated $configPath. Docker Desktop and WSL must be restarted for the D:\wsl\swap.vhdx path to take effect."
} else {
    Write-Output "Preview only. Re-run with -Apply to update $configPath."
    Write-Output $content
}
