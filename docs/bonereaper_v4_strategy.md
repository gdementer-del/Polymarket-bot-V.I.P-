# Bonereaper V4 Strategy Blueprint

## Goal

Build a Bonereaper-style research and trading stack around short BTC/ETH Polymarket windows without pretending that visible positions alone are a complete strategy.

The intent of `v4` is not "better directional prediction". The intent is:

- inventory-aware execution in BTC/ETH micro-markets;
- explicit maker/taker and fee/rebate accounting;
- strict control of gross inventory and net directional delta;
- event-by-event replay and forensic validation before live trading.

## Working Hypothesis

The public Bonereaper pattern is most consistent with a hybrid microstructure strategy:

- concentrate in BTC/ETH `updown` windows;
- carry both sides of the same binary market at times;
- use fast incremental fills rather than one-shot directional bets;
- optimize execution quality through CLOB/relayer mechanics;
- monetize thin directional edge plus execution economics and possibly maker rebates.

This means `v4` should be built as an execution and inventory system first, and only secondarily as a directional signal engine.

## Non-Goals

- do not clone visible positions blindly;
- do not assume expensive `0.90+` legs are alpha by themselves;
- do not treat public trade history as a complete off-chain order history;
- do not ship live until fee/rebate-adjusted edge is established in replay and paper.

## Core Data Layers

### 1. Public profile layer

- `public-profile`
- `positions`
- `closed-positions`
- `activity`
- `trades`
- `value`
- `traded`
- `comments/user_address`

Must store both wallet address and `proxyWallet`.

### 2. Accounting layer

- official accounting snapshot ZIP
- `positions.csv`
- `equity.csv`

This is the source of truth for equity curve, cost basis, and drawdown reconstruction.

### 3. Market microstructure layer

- market WebSocket feed
- live order books
- last-trade stream
- per-window book state around observed wallet trades

### 4. Chain layer

- Polygon account tx list
- internal tx
- ERC-20 transfers
- ERC-1155 transfers
- contract classification for CTF Exchange / Conditional Tokens / Fee Module / USDC.e

## V4 Architecture

### `services/market_data.rs`

Extend wallet activity ingestion to preserve:

- `activity_type`
- `proxy_wallet`
- enriched order-book context
- trade discount versus ask/mid
- per-window trade index

### `services/runner.rs`

Use wallet-follow reports as the main forensic layer for `v4` research:

- two-sided residual inventory windows
- gross inventory versus net directional delta
- execution quality heuristics from trade discount to live ask
- concentration in micro-windows

### New future module: `services/inventory.rs`

Planned responsibilities:

- gross inventory per slug
- net directional delta by asset and time bucket
- inventory imbalance alerts
- cooldown rules after adverse inventory expansion

### New future module: `services/research.rs`

Planned responsibilities:

- Bonereaper-style public data ETL
- maker/taker attribution joins
- replay datasets
- per-window execution scorecards

## Entry Logic for V4

`v4` should not open a trade from a single directional state score alone.

A valid entry should require:

- eligible BTC/ETH micro-window;
- acceptable book quality and spread;
- bounded gross inventory after fill;
- bounded net directional delta after fill;
- minimum expected edge after fees and safety margin;
- no adverse regime flag on current window.

## Exit Logic for V4

Exit logic should prioritize inventory and execution quality over naive stop-losses.

Planned hierarchy:

1. inventory rebalance exit
2. peak / exhaustion capture
3. hard risk exit only when both micro and slower confirmation support it
4. no early partial reversal unless replay proves it helps

## Risk Framework

Must be explicit from day one:

- max gross inventory per slug
- max total BTC directional delta
- max total ETH directional delta
- max concurrent micro-windows
- stop regime after repeated adverse fills
- no-trade mode on degraded market data
- periodic accounting snapshot export

## Validation Plan

### Phase 1: Forensics

- record enriched wallet activity
- compute Bonereaper-style profile summary
- verify two-sided inventory and execution-quality signals

### Phase 2: Replay

- reconstruct per-window sequences from activity/trades/order books
- compare gross inventory, net delta, and trade timing
- estimate whether edge survives fees and likely rebates

### Phase 3: Paper

- run `v4` only with conservative inventory caps
- compare realized paper performance against replay expectations

### Phase 4: Micro-live

- only after replay and paper agree
- tiny inventory caps
- full ledger reconciliation

## Immediate Next Steps

1. Keep strengthening wallet forensics and accounting fidelity.
2. Add explicit inventory analytics module.
3. Add replay-oriented maker/taker attribution pipeline.
4. Only then introduce a real `OpportunityKind` for `v4`.
