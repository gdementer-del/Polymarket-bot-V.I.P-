param(
    [int[]]$Thresholds = @(1, 2, 3, 4),
    [int]$WindowsPerTarget = 20,
    [int[]]$EntryMinutes = @(1),
    [int]$Top = 15,
    [string]$Target = "btc-5m",
    [string]$BaseConfig = "config.codex-sentinel.toml",
    [int]$PerRunTimeoutSec = 240,
    [switch]$IncludeOrderbook,
    [switch]$Release
)

$ErrorActionPreference = "Stop"

function ConvertTo-CsvCell {
    param([object]$Value)

    $text = [string]$Value
    $escaped = $text.Replace('"', '""')
    return """$escaped"""
}

function ConvertTo-ProcessArgument {
    param([object]$Value)

    $text = [string]$Value
    if ($text -match '[\s"]') {
        return '"' + $text.Replace('"', '\"') + '"'
    }
    return $text
}

function Stop-BacktestProcessesForConfig {
    param([string]$ConfigPath)

    Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -in @("cargo.exe", "polymarket_mvp.exe") -and
            $_.CommandLine -like "*$ConfigPath*"
        } |
        ForEach-Object {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        }
}

function Get-MarkedCsvRows {
    param(
        [object[]]$Lines,
        [string]$Begin,
        [string]$End
    )

    $inside = $false
    $rows = @()
    foreach ($line in $Lines) {
        $text = [string]$line
        if ($text -eq $Begin) {
            $inside = $true
            continue
        }
        if ($text -eq $End) {
            $inside = $false
            continue
        }
        if ($inside -and $text.Length -gt 0) {
            $rows += $text
        }
    }
    return $rows
}

function New-SweepConfig {
    param(
        [string]$BaseConfigPath,
        [string]$OutputPath,
        [int]$Threshold,
        [string]$StateDir,
        [bool]$UseOrderbook
    )

    $text = Get-Content -LiteralPath $BaseConfigPath -Raw -Encoding UTF8

    $thresholdPattern = '(?m)^\s*bonereaper_state_v2_min_signal_bps\s*=\s*\d+\s*$'
    if ($text -notmatch $thresholdPattern) {
        throw "Base config must contain bonereaper_state_v2_min_signal_bps."
    }
    $text = [regex]::Replace(
        $text,
        $thresholdPattern,
        "bonereaper_state_v2_min_signal_bps = $Threshold"
    )

    $stateDirPattern = '(?m)^\s*state_dir\s*=\s*".*"\s*$'
    if ($text -notmatch $stateDirPattern) {
        throw "Base config must contain storage.state_dir."
    }
    $text = [regex]::Replace(
        $text,
        $stateDirPattern,
        "state_dir = `"$StateDir`""
    )

    $includeOrderbookPattern = '(?m)^\s*include_orderbook\s*=\s*(true|false)\s*$'
    if ($text -notmatch $includeOrderbookPattern) {
        throw "Base config must contain polybacktest.include_orderbook."
    }
    $includeOrderbookValue = if ($UseOrderbook) { "true" } else { "false" }
    $text = [regex]::Replace(
        $text,
        $includeOrderbookPattern,
        "include_orderbook = $includeOrderbookValue"
    )

    Set-Content -Path $OutputPath -Value $text -Encoding UTF8
}

if (-not $env:POLYBACKTEST_API_KEY) {
    throw "POLYBACKTEST_API_KEY is not set. Set it in this shell before running the sweep."
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$baseConfigPath = Join-Path $repoRoot $BaseConfig
if (-not (Test-Path -LiteralPath $baseConfigPath)) {
    throw "Base config not found: $BaseConfig"
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runRoot = Join-Path $repoRoot "state\polybacktest-runs\$stamp-v2-signal-sweep"
New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

$summaryPath = Join-Path $runRoot "summary.csv"
"threshold,config,entry_minutes,target,windows,signals,near_misses,expected,realized,hit_rate_pct,status,log" |
    Set-Content -Path $summaryPath -Encoding UTF8
$signalsPath = Join-Path $runRoot "signals.csv"
$signalsHeaderWritten = $false
$nearMissesPath = Join-Path $runRoot "near_misses.csv"
$nearMissesHeaderWritten = $false

Push-Location $repoRoot
try {
    foreach ($threshold in $Thresholds) {
        $configPath = Join-Path $runRoot "codex-sentinel-v2-signal-$threshold.toml"
        $stateDir = "state/polybacktest-runs/$stamp-v2-signal-sweep/state-signal-$threshold"
        New-SweepConfig `
            -BaseConfigPath $baseConfigPath `
            -OutputPath $configPath `
            -Threshold $threshold `
            -StateDir $stateDir `
            -UseOrderbook $IncludeOrderbook.IsPresent

        foreach ($entryMinute in $EntryMinutes) {
            $logPath = Join-Path $runRoot "signal-$threshold-entry$entryMinute.log"
            $cargoArgs = @("run")
            if ($Release) {
                $cargoArgs += "--release"
            }
            $cargoArgs += @(
                "--",
                "--config", $configPath,
                "polybacktest",
                "--windows-per-target", $WindowsPerTarget,
                "--entry-minutes", $entryMinute,
                "--top", $Top,
                "--target", $Target
            )

            Write-Host "Running signal_threshold=$threshold entry=$entryMinute target=$Target windows=$WindowsPerTarget"
            $stdoutPath = "$logPath.stdout"
            $stderrPath = "$logPath.stderr"
            $argumentLine = ($cargoArgs | ForEach-Object { ConvertTo-ProcessArgument $_ }) -join " "
            $timedOut = $false
            try {
                $process = Start-Process `
                    -FilePath "cargo.exe" `
                    -ArgumentList $argumentLine `
                    -WindowStyle Hidden `
                    -RedirectStandardOutput $stdoutPath `
                    -RedirectStandardError $stderrPath `
                    -PassThru
                if (-not $process.WaitForExit($PerRunTimeoutSec * 1000)) {
                    $timedOut = $true
                    Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
                    Stop-BacktestProcessesForConfig -ConfigPath $configPath
                    $exitCode = -1
                }
                else {
                    $process.Refresh()
                    $exitCode = if ($null -eq $process.ExitCode) { 0 } else { $process.ExitCode }
                }
            }
            finally {
                $output = @()
                if (Test-Path -LiteralPath $stdoutPath) {
                    $output += Get-Content -LiteralPath $stdoutPath -ErrorAction SilentlyContinue
                }
                if (Test-Path -LiteralPath $stderrPath) {
                    $output += Get-Content -LiteralPath $stderrPath -ErrorAction SilentlyContinue
                }
                if ($timedOut) {
                    $output += "polybacktest run timed out after $PerRunTimeoutSec seconds"
                }
            }
            $output | Set-Content -Path $logPath -Encoding UTF8

            $signalRows = @(Get-MarkedCsvRows -Lines $output -Begin "SIGNALS_CSV_BEGIN" -End "SIGNALS_CSV_END")
            if ($signalRows.Count -gt 0) {
                $perRunSignalsPath = Join-Path $runRoot "signal-$threshold-entry$entryMinute-signals.csv"
                $signalRows | Set-Content -Path $perRunSignalsPath -Encoding UTF8
                if (-not $signalsHeaderWritten) {
                    "threshold,entry_minutes,$($signalRows[0])" |
                        Set-Content -Path $signalsPath -Encoding UTF8
                    $signalsHeaderWritten = $true
                }
                foreach ($row in ($signalRows | Select-Object -Skip 1)) {
                    "$(ConvertTo-CsvCell $threshold),$(ConvertTo-CsvCell $entryMinute),$row" |
                        Add-Content -Path $signalsPath -Encoding UTF8
                }
            }

            $nearMissRows = @(Get-MarkedCsvRows -Lines $output -Begin "NEAR_MISSES_CSV_BEGIN" -End "NEAR_MISSES_CSV_END")
            if ($nearMissRows.Count -gt 0) {
                $perRunNearMissesPath = Join-Path $runRoot "signal-$threshold-entry$entryMinute-near-misses.csv"
                $nearMissRows | Set-Content -Path $perRunNearMissesPath -Encoding UTF8
                if (-not $nearMissesHeaderWritten) {
                    "threshold,entry_minutes,$($nearMissRows[0])" |
                        Set-Content -Path $nearMissesPath -Encoding UTF8
                    $nearMissesHeaderWritten = $true
                }
                foreach ($row in ($nearMissRows | Select-Object -Skip 1)) {
                    "$(ConvertTo-CsvCell $threshold),$(ConvertTo-CsvCell $entryMinute),$row" |
                        Add-Content -Path $nearMissesPath -Encoding UTF8
                }
            }

            $status = if ($timedOut) { "timeout" } elseif ($exitCode -eq 0) { "ok" } else { "failed:$exitCode" }
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

            "$(ConvertTo-CsvCell $threshold),$(ConvertTo-CsvCell $configPath),$entryMinute,$Target,$WindowsPerTarget,$signals,$nearMisses,$expected,$realized,$hitRate,$status,$(ConvertTo-CsvCell $logPath)" |
                Add-Content -Path $summaryPath -Encoding UTF8
        }
    }
}
finally {
    Pop-Location
}

Write-Host "Signal-threshold sweep complete: $summaryPath"
