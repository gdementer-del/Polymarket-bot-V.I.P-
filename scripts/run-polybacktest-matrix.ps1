param(
    [string[]]$Configs = @(
        "config.codex-scalp-v1-raw-light-v3.toml",
        "config.codex-scalp-v1-raw-light-v2.toml",
        "config.codex-scalp-v1-raw.toml",
        "config.codex-v4-champion.toml"
    ),
    [int]$WindowsPerTarget = 60,
    [int[]]$EntryMinutes = @(0, 1),
    [int]$Top = 10,
    [string]$Target = "btc-5m",
    [switch]$Release
)

$ErrorActionPreference = "Stop"

function ConvertTo-CsvCell {
    param([object]$Value)

    $text = [string]$Value
    $escaped = $text.Replace('"', '""')
    return """$escaped"""
}

function Get-SignalCsvRows {
    param([object[]]$Lines)

    $inside = $false
    $rows = @()
    foreach ($line in $Lines) {
        $text = [string]$line
        if ($text -eq "SIGNALS_CSV_BEGIN") {
            $inside = $true
            continue
        }
        if ($text -eq "SIGNALS_CSV_END") {
            $inside = $false
            continue
        }
        if ($inside -and $text.Length -gt 0) {
            $rows += $text
        }
    }
    return $rows
}

function Get-NearMissCsvRows {
    param([object[]]$Lines)

    $inside = $false
    $rows = @()
    foreach ($line in $Lines) {
        $text = [string]$line
        if ($text -eq "NEAR_MISSES_CSV_BEGIN") {
            $inside = $true
            continue
        }
        if ($text -eq "NEAR_MISSES_CSV_END") {
            $inside = $false
            continue
        }
        if ($inside -and $text.Length -gt 0) {
            $rows += $text
        }
    }
    return $rows
}

if (-not $env:POLYBACKTEST_API_KEY) {
    throw "POLYBACKTEST_API_KEY is not set. Set it in this shell before running the matrix."
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runRoot = Join-Path $repoRoot "state\polybacktest-runs\$stamp"
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

$summaryPath = Join-Path $runRoot "summary.csv"
"config,entry_minutes,target,windows,signals,near_misses,expected,realized,hit_rate_pct,status,log" |
    Set-Content -Path $summaryPath -Encoding UTF8
$signalsPath = Join-Path $runRoot "signals.csv"
$signalsHeaderWritten = $false
$nearMissesPath = Join-Path $runRoot "near_misses.csv"
$nearMissesHeaderWritten = $false

Push-Location $repoRoot
try {
    foreach ($config in $Configs) {
        if (-not (Test-Path -LiteralPath $config)) {
            "$(ConvertTo-CsvCell $config),,,$WindowsPerTarget,,,,,missing_config," |
                Add-Content -Path $summaryPath -Encoding UTF8
            continue
        }

        foreach ($entryMinute in $EntryMinutes) {
            $configLabel = [IO.Path]::GetFileNameWithoutExtension($config)
            $logPath = Join-Path $runRoot "$configLabel-entry$entryMinute.log"
            $cargoArgs = @("run")
            if ($Release) {
                $cargoArgs += "--release"
            }
            $cargoArgs += @(
                "--",
                "--config", $config,
                "polybacktest",
                "--windows-per-target", $WindowsPerTarget,
                "--entry-minutes", $entryMinute,
                "--top", $Top,
                "--target", $Target
            )

            Write-Host "Running $config entry=$entryMinute target=$Target windows=$WindowsPerTarget"
            $previousErrorActionPreference = $ErrorActionPreference
            $ErrorActionPreference = "Continue"
            try {
                $output = & cargo @cargoArgs 2>&1
                $exitCode = $LASTEXITCODE
            }
            finally {
                $ErrorActionPreference = $previousErrorActionPreference
            }
            $output | Set-Content -Path $logPath -Encoding UTF8
            $signalRows = @(Get-SignalCsvRows -Lines $output)
            if ($signalRows.Count -gt 0) {
                $perRunSignalsPath = Join-Path $runRoot "$configLabel-entry$entryMinute-signals.csv"
                $signalRows | Set-Content -Path $perRunSignalsPath -Encoding UTF8
                if (-not $signalsHeaderWritten) {
                    "config,entry_minutes,$($signalRows[0])" |
                        Set-Content -Path $signalsPath -Encoding UTF8
                    $signalsHeaderWritten = $true
                }
                foreach ($row in ($signalRows | Select-Object -Skip 1)) {
                    "$(ConvertTo-CsvCell $config),$(ConvertTo-CsvCell $entryMinute),$row" |
                        Add-Content -Path $signalsPath -Encoding UTF8
                }
            }
            $nearMissRows = @(Get-NearMissCsvRows -Lines $output)
            if ($nearMissRows.Count -gt 0) {
                $perRunNearMissesPath = Join-Path $runRoot "$configLabel-entry$entryMinute-near-misses.csv"
                $nearMissRows | Set-Content -Path $perRunNearMissesPath -Encoding UTF8
                if (-not $nearMissesHeaderWritten) {
                    "config,entry_minutes,$($nearMissRows[0])" |
                        Set-Content -Path $nearMissesPath -Encoding UTF8
                    $nearMissesHeaderWritten = $true
                }
                foreach ($row in ($nearMissRows | Select-Object -Skip 1)) {
                    "$(ConvertTo-CsvCell $config),$(ConvertTo-CsvCell $entryMinute),$row" |
                        Add-Content -Path $nearMissesPath -Encoding UTF8
                }
            }

            $status = if ($exitCode -eq 0) { "ok" } else { "failed:$exitCode" }
            $signals = ""
            $nearMisses = ""
            $expected = ""
            $realized = ""
            $hitRate = ""

            foreach ($line in $output) {
                $text = [string]$line
                if ($text -match "^\s*(.+?)\s+(\d+)\s+(\d+)\s+(\d+)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)%") {
                    $signals = $Matches[3]
                    $nearMisses = $Matches[4]
                    $expected = $Matches[5]
                    $realized = $Matches[6]
                    $hitRate = $Matches[7]
                }
            }

            "$(ConvertTo-CsvCell $config),$entryMinute,$Target,$WindowsPerTarget,$signals,$nearMisses,$expected,$realized,$hitRate,$status,$(ConvertTo-CsvCell $logPath)" |
                Add-Content -Path $summaryPath -Encoding UTF8
        }
    }
}
finally {
    Pop-Location
}

Write-Host "Polybacktest matrix complete: $summaryPath"
