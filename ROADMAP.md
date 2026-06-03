# Roadmap

This roadmap describes the intended maintenance direction. It is not a promise
of profitable trading performance.

## Current Status

The project is a Rust research toolkit for low-latency market data ingestion,
paper trading, strategy experimentation, wallet replay, and post-run analytics
for short Polymarket crypto Up/Down markets.

## v0.1 Stabilization

- Keep the public repository safe for contributors by maintaining docs,
  examples, secrets guidance, and paper-first defaults.
- Keep config profiles parseable and covered by tests.
- Improve report clarity for paper PnL, trade quality, and loss buckets.
- Continue separating runtime state from committed source files.

## v0.2 Data Quality And Latency

- Add repeatable latency measurement for Binance, Coinbase, Polymarket, and
  Chainlink RTDS feeds.
- Improve cache-only hot paths and reduce accidental REST calls during reactive
  paper runs.
- Add clearer diagnostics for stale books, stale oracle anchors, and missing
  market subscriptions.

## v0.3 Backtesting And Replay

- Improve PolyBacktest and local replay comparability.
- Add fixtures for strategy boundary cases and historical-window reconstruction.
- Track in-sample and out-of-sample validation separately.

## v0.4 Architecture

- Continue breaking the runtime loop into smaller data, signal, execution,
  reporting, and storage modules.
- Add focused benchmarks for hot-path decision latency.
- Document the strategy promotion process from research profile to controlled
  paper run.

## Long-Term Research

- Better fair-price modeling for short crypto prediction windows.
- More robust order-book and cross-exchange pressure features.
- Safer profile comparison tooling that accounts for sample size, slippage,
  spread, and settlement uncertainty.
