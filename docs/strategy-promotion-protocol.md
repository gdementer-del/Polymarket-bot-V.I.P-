# Strategy Promotion Protocol

Use this protocol before changing the active paper/live strategy. The goal is to stop tuning from one noisy run and only promote changes that beat a frozen baseline on the same data.

## 1. Compare against a frozen baseline

The comparison script snapshots both configs into a run directory, gives each config an isolated `state_dir`, runs the same PolyBacktest window set, and writes a machine-readable summary plus a `PASS` / `NO-GO` decision.

```powershell
$env:POLYBACKTEST_API_KEY = "<set locally, never commit>"
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\compare-strategy-configs.ps1 `
  -BaselineConfig .\config.scalp-v1.toml `
  -CandidateConfig .\config.scalp-v1.toml `
  -BaselineLabel current `
  -CandidateLabel candidate `
  -Target btc-5m `
  -WindowsPerTarget 100 `
  -EntryMinutes 1 `
  -Top 20
```

Artifacts are written under `state\polybacktest-runs\<timestamp>-strategy-compare`.

## 2. Promotion gate

Promote the candidate only when the generated `decision.md` says `PASS`.

Default gate:

- Candidate has at least 20 signals.
- Candidate realized PnL is not worse than baseline.
- Candidate hit rate does not drop by more than 5 percentage points.
- Both runs finish with status `ok`.

For high-variance changes, increase `-WindowsPerTarget` before trusting the result.

## 3. Controlled paper-run

After a `PASS`, run a short controlled paper session before any longer run:

```powershell
.\target\debug\polymarket_mvp.exe --config .\config.scalp-v1.toml run --mode paper --max-runtime-secs 1800 --drain-open-positions --max-drain-secs 300
```

Only extend to 1-2 hours if the paper-run has normal signal frequency, no unexpected risk blocks, no writer errors, and no new loss bucket that was absent in PolyBacktest.
