param(
    [string]$BaselineConfig = "config.codex-scalp-v1.toml",
    [string]$CandidateConfig = "config.codex-scalp-v1.toml",
    [string]$BaselineLabel = "baseline",
    [string]$CandidateLabel = "candidate",
    [string]$Target = "btc-5m",
    [int]$WindowsPerTarget = 100,
    [string]$EntryMinutes = "1",
    [int]$Top = 20,
    [int]$PerRunTimeoutSec = 600,
    [int]$MinSignals = 20,
    [decimal]$MinRealizedImprovement = 0,
    [decimal]$MaxHitRateDropPct = 5,
    [switch]$Release,
    [switch]$ValidateOnly
)

$ErrorActionPreference = "Stop"

function Resolve-RepoPath {
    param([string]$Path)

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }

    return [System.IO.Path]::GetFullPath((Join-Path $repoRoot $Path))
}

function ConvertTo-ProcessArgument {
    param([string]$Value)

    if ($Value -match '[\s"]') {
        return '"' + ($Value -replace '"', '\"') + '"'
    }

    return $Value
}

function Get-MarkedCsvLines {
    param(
        [string]$Text,
        [string]$BeginMarker,
        [string]$EndMarker
    )

    $lines = $Text -split "`r?`n"
    $inside = $false
    $rows = New-Object System.Collections.Generic.List[string]

    foreach ($line in $lines) {
        if ($line.Trim() -eq $BeginMarker) {
            $inside = $true
            continue
        }

        if ($line.Trim() -eq $EndMarker) {
            break
        }

        if ($inside -and $line.Trim().Length -gt 0) {
            $rows.Add($line)
        }
    }

    return @($rows)
}

function ConvertTo-DecimalSafe {
    param($Value)

    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return [decimal]0
    }

    $parsed = [decimal]0
    if ([decimal]::TryParse($text, [System.Globalization.NumberStyles]::Any, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) {
        return $parsed
    }

    return [decimal]0
}

function ConvertTo-EntryMinuteList {
    param([string]$Value)

    $items = New-Object System.Collections.Generic.List[int]
    foreach ($part in (($Value -split '[,\s]+') | Where-Object { $_.Trim().Length -gt 0 })) {
        $items.Add([int]$part)
    }

    if ($items.Count -eq 0) {
        throw "EntryMinutes must contain at least one integer minute."
    }

    return @($items)
}

function Get-PolybacktestSummary {
    param([string]$Text)

    $summary = [ordered]@{
        windows = 0
        signals = 0
        near_misses = 0
        expected = [decimal]0
        realized = [decimal]0
        hit_rate_pct = [decimal]0
    }

    foreach ($line in ($Text -split "`r?`n")) {
        if ($line -match "^\s*(\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)%") {
            $summary.windows = [int]$Matches[2]
            $summary.signals = [int]$Matches[3]
            $summary.near_misses = [int]$Matches[4]
            $summary.expected = ConvertTo-DecimalSafe $Matches[5]
            $summary.realized = ConvertTo-DecimalSafe $Matches[6]
            $summary.hit_rate_pct = ConvertTo-DecimalSafe $Matches[7]
        }
    }

    return [pscustomobject]$summary
}

function Get-SignalMetrics {
    param([string[]]$SignalCsvLines)

    $metrics = [ordered]@{
        signal_count = 0
        realized = [decimal]0
        hit_rate_pct = [decimal]0
        wins = 0
        losses = 0
        avg_trade = [decimal]0
        profit_factor = [decimal]0
        max_win = [decimal]0
        max_loss = [decimal]0
    }

    if ($SignalCsvLines.Count -le 1) {
        return [pscustomobject]$metrics
    }

    $rows = $SignalCsvLines | ConvertFrom-Csv
    $profits = New-Object System.Collections.Generic.List[decimal]
    $grossWins = [decimal]0
    $grossLosses = [decimal]0
    $maxWin = $null
    $maxLoss = $null

    foreach ($row in $rows) {
        $profitText = $row.scalp_realized_profit
        if ([string]::IsNullOrWhiteSpace([string]$profitText)) {
            $profitText = $row.realized_profit
        }

        $profit = ConvertTo-DecimalSafe $profitText
        $profits.Add($profit)

        if ($profit -gt 0) {
            $metrics.wins += 1
            $grossWins += $profit
            if ($null -eq $maxWin -or $profit -gt $maxWin) {
                $maxWin = $profit
            }
        }
        elseif ($profit -lt 0) {
            $metrics.losses += 1
            $grossLosses += [decimal]::Abs($profit)
            if ($null -eq $maxLoss -or $profit -lt $maxLoss) {
                $maxLoss = $profit
            }
        }
    }

    if ($profits.Count -gt 0) {
        $metrics.signal_count = $profits.Count
        $sum = [decimal]0
        foreach ($profit in $profits) {
            $sum += $profit
        }
        $metrics.realized = $sum
        $metrics.avg_trade = $sum / [decimal]$profits.Count
        $metrics.hit_rate_pct = ([decimal]$metrics.wins / [decimal]$profits.Count * [decimal]100)
    }

    if ($grossLosses -gt 0) {
        $metrics.profit_factor = $grossWins / $grossLosses
    }
    elseif ($grossWins -gt 0) {
        $metrics.profit_factor = [decimal]999
    }

    if ($null -ne $maxWin) {
        $metrics.max_win = $maxWin
    }

    if ($null -ne $maxLoss) {
        $metrics.max_loss = $maxLoss
    }

    return [pscustomobject]$metrics
}

function New-FrozenConfig {
    param(
        [string]$SourceConfig,
        [string]$DestinationConfig,
        [string]$StateDirectory
    )

    $raw = Get-Content -Path $SourceConfig -Raw
    $normalizedState = ($StateDirectory -replace "\\", "/")

    if ($raw -notmatch "(?m)^state_dir\s*=") {
        throw "Config $SourceConfig does not contain a state_dir setting."
    }

    $raw = $raw -replace '(?m)^state_dir\s*=\s*".*"\s*$', "state_dir = `"$normalizedState`""
    Set-Content -Path $DestinationConfig -Value $raw -Encoding UTF8
}

function Ensure-Binary {
    $profile = "debug"
    if ($Release) {
        $profile = "release"
    }

    $binary = Join-Path $repoRoot "target\$profile\polymarket_mvp.exe"
    if (Test-Path $binary) {
        return $binary
    }

    $buildArgs = @("build")
    if ($Release) {
        $buildArgs += "--release"
    }

    & cargo @buildArgs
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with exit code $LASTEXITCODE"
    }

    if (-not (Test-Path $binary)) {
        throw "Expected binary was not created: $binary"
    }

    return $binary
}

function Invoke-PolybacktestRun {
    param(
        [string]$Role,
        [string]$Label,
        [string]$ConfigPath,
        [int]$EntryMinute,
        [string]$RunDirectory,
        [string]$Binary
    )

    New-Item -ItemType Directory -Force -Path $RunDirectory | Out-Null

    $stdoutPath = Join-Path $RunDirectory "stdout.log"
    $stderrPath = Join-Path $RunDirectory "stderr.log"
    $args = @(
        "--config", $ConfigPath,
        "polybacktest",
        "--windows-per-target", [string]$WindowsPerTarget,
        "--entry-minutes", [string]$EntryMinute,
        "--top", [string]$Top,
        "--target", $Target
    )
    $argLine = ($args | ForEach-Object { ConvertTo-ProcessArgument ([string]$_) }) -join " "

    $process = Start-Process `
        -FilePath $Binary `
        -ArgumentList $argLine `
        -WorkingDirectory $repoRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru

    $completed = $process.WaitForExit($PerRunTimeoutSec * 1000)
    $status = "ok"

    if (-not $completed) {
        $status = "timeout"
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    }
    elseif ($process.ExitCode -ne 0) {
        $status = "exit_$($process.ExitCode)"
    }

    $stdout = ""
    $stderr = ""

    if (Test-Path $stdoutPath) {
        $stdout = Get-Content -Path $stdoutPath -Raw
    }

    if (Test-Path $stderrPath) {
        $stderr = Get-Content -Path $stderrPath -Raw
    }

    $combined = $stdout + "`n" + $stderr
    $summary = Get-PolybacktestSummary $combined
    $signals = Get-MarkedCsvLines $combined "SIGNALS_CSV_BEGIN" "SIGNALS_CSV_END"
    $nearMisses = Get-MarkedCsvLines $combined "NEAR_MISSES_CSV_BEGIN" "NEAR_MISSES_CSV_END"
    $metrics = Get-SignalMetrics $signals
    $effectiveSignals = $summary.signals
    $effectiveRealized = $summary.realized
    $effectiveHitRatePct = $summary.hit_rate_pct
    if ($metrics.signal_count -gt 0) {
        $effectiveSignals = $metrics.signal_count
        $effectiveRealized = $metrics.realized
        $effectiveHitRatePct = $metrics.hit_rate_pct
    }

    if ($signals.Count -gt 0) {
        $signalsPath = Join-Path $RunDirectory "signals.csv"
        Set-Content -Path $signalsPath -Value $signals -Encoding UTF8
    }

    if ($nearMisses.Count -gt 0) {
        $nearMissPath = Join-Path $RunDirectory "near_misses.csv"
        Set-Content -Path $nearMissPath -Value $nearMisses -Encoding UTF8
    }

    return [pscustomobject]@{
        role = $Role
        label = $Label
        config = $ConfigPath
        entry_minutes = $EntryMinute
        target = $Target
        windows = $summary.windows
        signals = $effectiveSignals
        near_misses = $summary.near_misses
        expected = $summary.expected
        realized = $effectiveRealized
        hit_rate_pct = $effectiveHitRatePct
        wins = $metrics.wins
        losses = $metrics.losses
        avg_trade = $metrics.avg_trade
        profit_factor = $metrics.profit_factor
        max_win = $metrics.max_win
        max_loss = $metrics.max_loss
        status = $status
        log = $stdoutPath
    }
}

function Write-DecisionReport {
    param(
        [object[]]$Rows,
        [string]$OutputPath
    )

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("# Strategy comparison")
    $lines.Add("")
    $lines.Add("Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz')")
    $lines.Add("Target: $Target, windows per target: $WindowsPerTarget, top: $Top.")
    $lines.Add("Gate: candidate needs at least $MinSignals signals, realized improvement >= $MinRealizedImprovement, and hit-rate drop <= $MaxHitRateDropPct pct points.")
    $lines.Add("")
    $lines.Add("| Role | Label | Entry minute | Signals | Near misses | Realized | Hit rate | Avg trade | Profit factor | Status |")
    $lines.Add("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")

    foreach ($row in $Rows) {
        $lines.Add("| $($row.role) | $($row.label) | $($row.entry_minutes) | $($row.signals) | $($row.near_misses) | $($row.realized) | $($row.hit_rate_pct)% | $($row.avg_trade) | $($row.profit_factor) | $($row.status) |")
    }

    $lines.Add("")
    $lines.Add("## Decision")
    $lines.Add("")

    foreach ($entryMinute in $EntryMinuteValues) {
        $baseline = @($Rows | Where-Object { $_.role -eq "baseline" -and $_.entry_minutes -eq $entryMinute })[0]
        $candidate = @($Rows | Where-Object { $_.role -eq "candidate" -and $_.entry_minutes -eq $entryMinute })[0]

        if ($null -eq $baseline -or $null -eq $candidate) {
            $lines.Add("- entry minute `$entryMinute`: NO-GO, incomplete comparison.")
            continue
        }

        $reasons = New-Object System.Collections.Generic.List[string]

        if ($baseline.status -ne "ok") {
            $reasons.Add("baseline status is $($baseline.status)")
        }

        if ($candidate.status -ne "ok") {
            $reasons.Add("candidate status is $($candidate.status)")
        }

        if ([int]$candidate.signals -lt $MinSignals) {
            $reasons.Add("candidate has only $($candidate.signals) signals")
        }

        if ((ConvertTo-DecimalSafe $candidate.realized) -lt ((ConvertTo-DecimalSafe $baseline.realized) + $MinRealizedImprovement)) {
            $reasons.Add("candidate realized PnL is not better than baseline by the required margin")
        }

        if ((ConvertTo-DecimalSafe $candidate.hit_rate_pct) -lt ((ConvertTo-DecimalSafe $baseline.hit_rate_pct) - $MaxHitRateDropPct)) {
            $reasons.Add("candidate hit rate dropped too much")
        }

        if ($reasons.Count -eq 0) {
            $lines.Add("- entry minute `$entryMinute`: PASS, candidate is safe to promote to a longer paper-run.")
        }
        else {
            $lines.Add("- entry minute `$entryMinute`: NO-GO, $($reasons -join '; ').")
        }
    }

    Set-Content -Path $OutputPath -Value $lines -Encoding UTF8
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$baselineSource = Resolve-RepoPath $BaselineConfig
$candidateSource = Resolve-RepoPath $CandidateConfig
$EntryMinuteValues = ConvertTo-EntryMinuteList $EntryMinutes

if (-not (Test-Path $baselineSource)) {
    throw "Baseline config not found: $baselineSource"
}

if (-not (Test-Path $candidateSource)) {
    throw "Candidate config not found: $candidateSource"
}

if ($ValidateOnly) {
    Write-Host "Validation OK"
    Write-Host "repo=$repoRoot"
    Write-Host "baseline=$baselineSource"
    Write-Host "candidate=$candidateSource"
    Write-Host "target=$Target windows=$WindowsPerTarget entry_minutes=$($EntryMinuteValues -join ',') top=$Top"
    exit 0
}

if ([string]::IsNullOrWhiteSpace($env:POLYBACKTEST_API_KEY)) {
    throw "POLYBACKTEST_API_KEY is not set. The script intentionally reads the key from the environment and never stores it in run artifacts."
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runRoot = Join-Path $repoRoot "state\polybacktest-runs\$stamp-strategy-compare"
$baselineRunState = Join-Path $runRoot "state-baseline"
$candidateRunState = Join-Path $runRoot "state-candidate"
$baselineFrozen = Join-Path $runRoot "baseline.freeze.toml"
$candidateFrozen = Join-Path $runRoot "candidate.freeze.toml"

New-Item -ItemType Directory -Force -Path $runRoot | Out-Null
New-Item -ItemType Directory -Force -Path $baselineRunState | Out-Null
New-Item -ItemType Directory -Force -Path $candidateRunState | Out-Null

New-FrozenConfig -SourceConfig $baselineSource -DestinationConfig $baselineFrozen -StateDirectory $baselineRunState
New-FrozenConfig -SourceConfig $candidateSource -DestinationConfig $candidateFrozen -StateDirectory $candidateRunState

$binary = Ensure-Binary
$results = New-Object System.Collections.Generic.List[object]

foreach ($entryMinute in $EntryMinuteValues) {
    $baselineRunDir = Join-Path $runRoot "baseline-entry-$entryMinute"
    $candidateRunDir = Join-Path $runRoot "candidate-entry-$entryMinute"

    Write-Host "Running baseline entry=$entryMinute..."
    $results.Add((Invoke-PolybacktestRun -Role "baseline" -Label $BaselineLabel -ConfigPath $baselineFrozen -EntryMinute $entryMinute -RunDirectory $baselineRunDir -Binary $binary))

    Write-Host "Running candidate entry=$entryMinute..."
    $results.Add((Invoke-PolybacktestRun -Role "candidate" -Label $CandidateLabel -ConfigPath $candidateFrozen -EntryMinute $entryMinute -RunDirectory $candidateRunDir -Binary $binary))
}

$summaryPath = Join-Path $runRoot "summary.csv"
$decisionPath = Join-Path $runRoot "decision.md"

$results | Export-Csv -Path $summaryPath -NoTypeInformation -Encoding UTF8
Write-DecisionReport -Rows @($results) -OutputPath $decisionPath

Write-Host "Comparison complete."
Write-Host "Run root: $runRoot"
Write-Host "Summary: $summaryPath"
Write-Host "Decision: $decisionPath"
