# Polymarket Research Toolkit

Low-latency Rust toolkit for researching short crypto Up/Down markets on
Polymarket. The application combines Polymarket order books, Binance WebSocket,
Coinbase WebSocket, and the public Polymarket Chainlink RTDS feed. It runs
controlled paper-trading experiments and produces post-run reports.

This project is intended primarily for research and simulation. Positive PnL is
not guaranteed. Before using any strategy with real funds, validate fees,
slippage, liquidity, and out-of-sample stability independently.

## Open Source Status

This repository is maintained as an open-source Rust research toolkit under the
MIT license. It is intended for maintainers, researchers, and developers who
want to study prediction-market data ingestion, paper-trading workflows,
strategy evaluation, and market microstructure tooling.

Useful maintainer documents:

| File | Purpose |
| --- | --- |
| `LICENSE` | MIT license terms |
| `CONTRIBUTING.md` | Contribution workflow, validation checklist, and maintainer expectations |
| `ROADMAP.md` | Planned data, backtesting, architecture, and research work |
| `CHANGELOG.md` | Release notes and user-facing changes |
| `SECURITY.md` | Secret-handling and vulnerability reporting guidance |
| `.github/ISSUE_TEMPLATE/` | Structured bug, feature, and strategy-research triage |

## Quick Start

### 1. Install Rust

Rust `1.96.0` or newer is required. Verify the installation:

```powershell
rustc --version
cargo --version
```

### 2. Build the Application

Open PowerShell in the project directory:

```powershell
cargo build --release
```

The executable will be created here:

```text
target\release\polymarket_mvp.exe
```

### 3. Open the Operator Menu

```powershell
.\target\release\polymarket_mvp.exe
```

You can also open the menu explicitly:

```powershell
.\target\release\polymarket_mvp.exe menu
```

When no arguments are provided, the menu automatically selects
`config.scalp-v1-raw-light-v3.toml` if that research profile is available
next to the application.

## Operator Menu

The main menu is the safe entry point for daily operations:

| Section | Purpose |
| --- | --- |
| `1. Controlled paper run` | Run a simulation for 10, 30, 60, or a custom number of minutes |
| `2. Monitoring` | Watch Binance, Coinbase, and Chainlink RTDS prices, market dashboards, and scans |
| `3. Paper reports` | Inspect run PnL, trade quality, journal entries, positions, and analytics |
| `4. Research and backtesting` | Run local backtests, PolyBacktest sweeps, and the complete wallet-replay workflow |
| `5. Select TOML profile` | Switch between local `config*.toml` profiles |
| `6. Validate current profile` | Check configuration before starting a run |
| `7. Show CLI help` | Display direct-command examples |

The menu intentionally does not send live orders. It controls paper tests,
monitoring, and research so an accidental key press cannot place a real trade.

The wallet research submenu supports public activity monitoring, snapshot
recording, reports, replay timelines, dataset export, cap and cooldown
simulation, export comparison, autotuning, and alert-threshold calibration.
Separate multiple JSON file paths with `;`.

## First Paper Test

Use this flow to verify a fresh installation:

1. Run `.\target\release\polymarket_mvp.exe`.
2. Select `6` to validate the current profile.
3. Select `2`, then `1` to watch realtime prices.
4. Stop the monitor with `Ctrl+C`.
5. Open the menu again and select `1`, then `1` for a 10-minute smoke test.
6. When the run finishes, select `3`, then `1` to inspect the summary.

Press `Ctrl+C` to stop a long-running command. A controlled paper run stops new
entries when its timer expires, briefly drains open paper positions, and flushes
journal entries to disk.

## Direct CLI Commands

The menu covers normal usage, but every operation is also available directly.

```powershell
$bot = ".\target\release\polymarket_mvp.exe"
$config = "config.scalp-v1-raw-light-v3.toml"
```

Watch raw realtime prices:

```powershell
& $bot --config $config price-monitor --refresh-secs 1
```

The realtime monitor redraws a fixed terminal dashboard in place instead of
appending a new line for every quote.

Scan markets once:

```powershell
& $bot --config $config scan --top 10
```

Run a controlled 30-minute paper session:

```powershell
& $bot --config $config run --mode paper --max-runtime-secs 1800 --drain-open-positions --max-drain-secs 60 --status-dashboard
```

Controlled paper runs started from the menu enable a compact terminal dashboard
automatically. The dashboard redraws in place and is rendered off the strategy
hot path, so a slow terminal cannot stall quote processing.

Inspect post-run reports:

```powershell
& $bot --config $config paper-run-summary
& $bot --config $config paper-quality
& $bot --config $config paper-trades
& $bot --config $config paper-positions
```

Show every command:

```powershell
& $bot --help
```

## Configuration Profiles

Profiles are stored in `config*.toml` files next to the project.

| File | Intended use |
| --- | --- |
| `config.example.toml` | Documented baseline template |
| `config.scalp-v1-raw-light-v3.toml` | Current fast multi-asset paper profile for controlled experiments |
| `config.scalp-v1-raw-light-v2.toml` | Previous raw-light profile for comparison |
| `config.scalp-v1-raw.toml` | Raw ablation research profile |
| `config.scalp-v1.toml` | Baseline scalp profile |
| `config.sentinel.toml` | More conservative Sentinel experiment |
| `config.v4-champion.toml` | Historical v4 profile |
| `config.polybacktest-btc.toml` | BTC PolyBacktest profile |

No profile should be considered proven profitable. Compare profiles over the same
time intervals, account for sample size, and validate results out of sample.

## PolyBacktest

Cloud PolyBacktest requires a token in the current PowerShell session:

```powershell
# Set POLYBACKTEST_API_KEY in your shell before running this command.
& $bot --config $config polybacktest --windows-per-target 30 --entry-minutes 1 --top 10 --target btc-5m
```

Never write a real token into TOML, README, or git.

## Secrets and Live Mode

Paper tests do not require Polymarket credentials. If you separately research the
live API, use environment variables in the current PowerShell session:

```powershell
# Set POLYMARKET_API_KEY and related variables in your shell only.
```

Supported variables are listed in `.env.example`. The `.env` file is excluded
from git, but the binary does not load it automatically. If you maintain a local
`.env`, export its values before starting the application. Never commit a private
key, API key, secret, or passphrase. Live execution remains CLI-only and requires
a separate audit before use.

## Runtime Output

Local journals and runtime state are written to `state/`. This directory is
excluded from git. It contains paper trades, cycles, PnL snapshots, and open
position state according to the selected TOML profile.

Useful analysis commands:

```powershell
& $bot --config $config paper-run-summary
& $bot --config $config paper-quality
& $bot --config $config analytics
```

## Validate the Project

Run these commands before publishing or after changing Rust code:

```powershell
cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo build --release --locked
```

## Maintainer Workflow

The project is maintained through public issues, pull requests, release notes,
and reproducible validation. Strategy-related changes should include a clear
hypothesis, sample-size notes, and backtest, replay, or controlled paper-run
evidence. Refactors should preserve paper-first behavior unless the pull request
explicitly documents a behavior change.

Issue triage priorities:

| Priority | Examples |
| --- | --- |
| Correctness | Broken config parsing, stale data handling, journal durability, wrong PnL accounting |
| Data quality | Exchange feed lag, missing market subscriptions, oracle anchor issues |
| Research quality | Strategy hypotheses, backtest comparability, sample-size limits |
| Maintainability | Runner decomposition, tests, docs, config cleanup |

Releases should summarize user-facing changes, config migrations, validation
status, and known limitations. The current release line starts at `v0.1.0`.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| `cargo` is not found | Reopen PowerShell after installing Rust |
| No quotes appear | Check internet access, VPN or firewall settings, and WebSocket access to Binance, Coinbase, and Polymarket |
| Chainlink shows `waiting` | The public RTDS feed may not emit every symbol continuously; compare it with Binance and Coinbase |
| PolyBacktest does not start | Check `$env:POLYBACKTEST_API_KEY` in the current PowerShell session |
| The wrong profile is selected | Use menu item `5`, then validate with item `6` |
| A monitor needs to stop | Press `Ctrl+C` |
| More CLI details are needed | Run `.\target\release\polymarket_mvp.exe --help` |

## Project Layout

```text
src/config.rs            CLI and TOML configuration
src/models/              market, order-book, and paper-state models
src/services/menu.rs     interactive operator menu
src/services/runner.rs   orchestration and runtime loop
src/services/            market data, strategy, execution, journal, and analytics
config*.toml             experiment profiles
docs/                    research notes and protocols
scripts/                 helper PowerShell scripts
```

## Disclaimer

This is research software, not financial advice. Prediction markets and trading
systems involve substantial risk. Paper PnL does not prove a durable edge in live
execution.

## License

This project is licensed under the MIT License. See `LICENSE` for details.
