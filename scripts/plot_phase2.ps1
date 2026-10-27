param(
    [string]$InputDirectory = 'D:\bda\logs\profile\phase2_v1\csv',
    [string]$OutputDirectory = 'D:\bda\_meta\profile\graphs'
)
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Drawing
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$CsvOutputDirectory = Join-Path (Split-Path -Parent $OutputDirectory) 'csv'
New-Item -ItemType Directory -Force -Path $CsvOutputDirectory | Out-Null
if (Test-Path $InputDirectory) {
    foreach ($sourceCsv in Get-ChildItem -LiteralPath $InputDirectory -Filter '*.csv' -File) {
        $targetCsv = Join-Path $CsvOutputDirectory $sourceCsv.Name
        if (Test-Path $targetCsv) {
            if ((Get-FileHash -LiteralPath $sourceCsv.FullName -Algorithm SHA256).Hash -ne (Get-FileHash -LiteralPath $targetCsv -Algorithm SHA256).Hash) {
                throw "Refusing to replace a different existing profile table: $targetCsv"
            }
        } else { Copy-Item -LiteralPath $sourceCsv.FullName -Destination $targetCsv }
    }
}

function Draw-Bars([string]$Title, [string]$File, [object[]]$Rows, [string]$LabelField, [string]$ValueField, [string]$Unit = '') {
    if (-not $Rows -or $Rows.Count -eq 0) { Write-Warning "No rows for $File"; return }
    $w = 2400; $h = 1600; $bitmap = [System.Drawing.Bitmap]::new($w, $h)
    $g = [System.Drawing.Graphics]::FromImage($bitmap)
    $g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
    $g.Clear([System.Drawing.Color]::White)
    $fontTitle = [System.Drawing.Font]::new('Arial', 42, [System.Drawing.FontStyle]::Bold)
    $font = [System.Drawing.Font]::new('Arial', 21)
    $brush = [System.Drawing.Brushes]::Black
    $g.DrawString($Title, $fontTitle, $brush, 70, 40)
    $plotX = 720; $plotY = 160; $plotW = 1550
    $step = [math]::Min(120, [math]::Max(42, [int](1250 / $Rows.Count)))
    $barH = [math]::Max(20, [math]::Min(48, $step - 12))
    $gap = $step - $barH
    $maxValue = 0.0
    foreach ($r in $Rows) { $n = 0.0; [void][double]::TryParse([string]$r.$ValueField, [ref]$n); if ($n -gt $maxValue) { $maxValue = $n } }
    if ($maxValue -le 0) { $maxValue = 1.0 }
    $i = 0
    foreach ($r in $Rows) {
        $y = $plotY + $i * ($barH + $gap); if ($y + $barH -gt 1510) { break }
        $label = [string]$r.$LabelField; if ($label.Length -gt 48) { $label = $label.Substring(0, 45) + '...' }
        $n = 0.0; [void][double]::TryParse([string]$r.$ValueField, [ref]$n)
        $g.DrawString($label, $font, $brush, 50, $y)
        $bw = [math]::Max(1, [int]($plotW * $n / $maxValue))
        $g.FillRectangle([System.Drawing.Brushes]::SteelBlue, $plotX, $y, $bw, $barH)
        $g.DrawString(([string]::Format('{0:N4}{1}', $n, $Unit)), $font, $brush, [math]::Min($plotX + $bw + 10, 2040), $y)
        $i++
    }
    $bitmap.SetResolution(300, 300)
    $bitmap.Save((Join-Path $OutputDirectory $File), [System.Drawing.Imaging.ImageFormat]::Png)
    $g.Dispose(); $bitmap.Dispose(); $font.Dispose(); $fontTitle.Dispose()
}

$classFile = Join-Path $InputDirectory 'rows_by_dataset_day_label.csv'
if (Test-Path $classFile) {
    $allClass = Import-Csv $classFile
    foreach ($ds in @('cicids2017_packet','cicids2017_flow','unswnb15')) {
        $class = $allClass | Where-Object dataset -eq $ds | Group-Object label_multi | ForEach-Object {
            [pscustomobject]@{ label=$_.Name; rows=($_.Group | Measure-Object -Property rows -Sum).Sum }
        } | Sort-Object { [double]$_.rows } -Descending
        if ($ds -eq 'cicids2017_packet') { $graphName='class_distribution.png' } else { $graphName="class_distribution_$ds.png" }
        Draw-Bars "$ds class distribution" $graphName $class 'label' 'rows'
    }
    $day = $allClass | Where-Object { $_.dataset -eq 'cicids2017_packet' -and $_.label_binary -eq 'ATTACK' } |
        ForEach-Object { [pscustomobject]@{ label=('{0} / {1}' -f $_.day,$_.label_multi); rows=$_.rows } } | Sort-Object { [double]$_.rows } -Descending | Select-Object -First 28
    Draw-Bars 'CIC-IDS2017 packet attacks by day and class' 'attacks_by_day.png' $day 'label' 'rows'
}
$imbFile = Join-Path $InputDirectory 'class_imbalance_by_class.csv'
if (-not (Test-Path $imbFile)) { $imbFile = Join-Path $InputDirectory 'class_imbalance.csv' }
if (Test-Path $imbFile) {
    $allImb = Import-Csv $imbFile
    foreach ($ds in @('cicids2017_packet','cicids2017_flow','unswnb15')) {
        $imb = $allImb | Where-Object dataset -eq $ds | Sort-Object { [double]$_.max_to_class_ratio } -Descending
        $name=if ($ds -eq 'cicids2017_packet') {'class_imbalance.png'} else {"class_imbalance_$ds.png"}
        Draw-Bars "$ds class imbalance (largest class / class)" $name $imb 'label_multi' 'max_to_class_ratio' 'x'
    }
}
$qualityFile = Join-Path $InputDirectory 'column_quality.csv'
if (Test-Path $qualityFile) {
    $allQuality = Import-Csv $qualityFile
    foreach ($ds in @('cicids2017_packet','cicids2017_flow','unswnb15')) {
        $quality = $allQuality | Where-Object dataset -eq $ds | ForEach-Object {
            [pscustomobject]@{ label=$_.column_name; rate=100*[math]::Max([double]$_.null_rate,[math]::Max([double]$_.nan_rate,[double]$_.infinity_rate)) }
        } | Sort-Object rate -Descending | Select-Object -First 28
        $name=if ($ds -eq 'cicids2017_flow') {'missing_rates.png'} else {"missing_rates_$ds.png"}
        Draw-Bars "$ds missing / NaN / Inf rate" $name $quality 'label' 'rate' '%'
    }
}
$protocolFile = Join-Path $InputDirectory 'protocol_distribution.csv'
if (Test-Path $protocolFile) { $r=Import-Csv $protocolFile | Sort-Object { [double]$_.rows } -Descending; Draw-Bars 'CIC-IDS2017 packet protocol distribution' 'protocol_distribution.png' $r 'protocol' 'rows' }
$sizeFile = Join-Path $InputDirectory 'packet_size_distribution.csv'
if (Test-Path $sizeFile) { $r=Import-Csv $sizeFile | Sort-Object { [double]$_.rows } -Descending; Draw-Bars 'CIC-IDS2017 packet size distribution' 'packet_size_distribution.png' $r 'packet_size_bin' 'rows' }
$summaryFile = Join-Path $InputDirectory 'profile_summary.csv'; $auditFile = Join-Path $InputDirectory 'clean_audit.csv'
if ((Test-Path $summaryFile) -and (Test-Path $auditFile)) {
    $pre = Import-Csv $summaryFile | Where-Object dataset -eq 'cicids2017_packet' | Select-Object -First 1
    $post = Import-Csv $auditFile | Where-Object { $_.dataset -eq 'cicids2017_packet' -and $_.reason -eq 'rows_after_cleaning' } | Select-Object -First 1
    if ($pre -and $post) {
        $r=@([pscustomobject]@{label='Before cleaning'; rows=$pre.rows_before},[pscustomobject]@{label='After cleaning'; rows=$post.count})
        Draw-Bars 'CIC-IDS2017 packet rows before vs after cleaning' 'cleaning_comparison.png' $r 'label' 'rows'
    }
}
$edaFile = Join-Path $InputDirectory 'eda_before_after.csv'
if (Test-Path $edaFile) {
    $r = Import-Csv $edaFile | Where-Object dataset -eq 'cicids2017_packet' | ForEach-Object {
        [pscustomobject]@{ label=$_.stage; rows=$_.rows }
    }
    Draw-Bars 'Packet sample: before and after EDA cleaning rules' 'cleaning_comparison.png' $r 'label' 'rows'
}
Get-ChildItem -LiteralPath $OutputDirectory -Filter '*.png' | Select-Object Name,Length
