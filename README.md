# Polymarket MVP

Rust research framework for Polymarket market-data ingestion, order-book analysis, strategy simulation, paper trading, and controlled strategy experiments.

This repository is intended for research and paper trading first. It does not provide financial advice, guaranteed profitability, or a live-trading recommendation.

## Features

- Polymarket market discovery and order-book ingestion.
- Binance and Coinbase market-data integrations for fast crypto window context.
- Strategy modules for event-market research and controlled paper runs.
- Paper-trading journal, PnL snapshots, and post-run reporting.
- PolyBacktest integration for strategy validation and parameter sweeps.
- Wallet-follow research tools for public Polymarket activity analysis.

## Safety

- Keep real API keys and wallet secrets out of git.
- Use `.env` or shell environment variables for secrets.
- Start with paper mode and controlled runtime limits before considering any live execution.
- Treat every strategy config as experimental until validated out-of-sample.

## Quick Start

```powershell
cargo build --release
cargo test
```

Run a controlled paper session:

```powershell
.\target\release\polymarket_mvp.exe --config config.codex-scalp-v1-raw-light-v3.toml run --max-runtime-secs 1800
```

Generate paper reports:

```powershell
.\target\release\polymarket_mvp.exe --config config.codex-scalp-v1-raw-light-v3.toml paper-report
.\target\release\polymarket_mvp.exe --config config.codex-scalp-v1-raw-light-v3.toml paper-trades
```

## Configuration

Example and research configs are stored as `config*.toml`. Secrets are referenced by environment variable name only, for example:

```powershell
$env:POLYBACKTEST_API_KEY = "..."
$env:POLYMARKET_API_KEY = "..."
```

Never commit real keys, private keys, generated journals, or runtime state. The repository `.gitignore` excludes `state/`, `logs/`, `runs/`, `run_logs/`, `tmp/`, `.env`, and build artifacts.

## Project Layout

- `src/models` - domain models for markets, books, opportunities, and paper state.
- `src/services` - market data, execution, strategy, journal, reporting, and research services.
- `scripts` - local research and PolyBacktest helper scripts.
- `docs` - strategy notes and research protocols.
- `config*.toml` - strategy and runtime profiles.

## Disclaimer

This software is for educational, research, and simulation purposes. Prediction markets and trading systems involve substantial risk. Validate assumptions, costs, slippage, latency, and market liquidity before using any strategy outside paper mode.
