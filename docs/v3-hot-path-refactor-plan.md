# V3 Hot Path Refactor Plan

This note is the follow-up checklist after the current controlled paper run finishes.
Goal: improve data latency and decision speed without changing the core scalp thesis.

## Stage 1: Fast Paper Hot Path

- Keep `revalidate_before_execute = false` for v3 paper tests.
- Keep `reactive_debounce_ms = 0` or test `5` only if event spam becomes unstable.
- Keep Polymarket orderbooks live-only in the entry path and avoid REST fallback during signal execution.
- Prewarm current and next BTC 5m windows so books are already in memory before a signal arrives.
- Avoid `market-by-slug`, `/books`, Binance REST ticker, and Binance REST klines inside the entry hot path.

## Stage 2: In-Memory Revalidate

- Add a fast revalidate path that reads the latest in-memory Polymarket book and Binance/Coinbase state.
- Do not call Gamma or CLOB REST from revalidate in paper v3.
- Reuse the same snapshot that produced the opportunity when its book/quote age is fresh enough.

## Stage 3: Live Signal State

- Maintain a per-symbol `LiveSignalState` updated on every Binance, Coinbase, and Polymarket WS event.
- Precompute 1s, 5s, 15s move, target gap, orderbook pressure, depth imbalance, microprice, and signal score.
- Let strategy selection read ready-made numbers instead of rebuilding context on every snapshot.

## Stage 4: Combined Exchange Streams

- Replace per-symbol Binance trade, kline, and depth sockets with one combined stream handler.
- Keep one parser/dispatcher that updates per-symbol caches.
- Add Coinbase L2/orderbook as a secondary pressure source, not just ticker.

## Stage 5: Measurement

- Track latency buckets for event received, signal state updated, opportunity selected, and paper fill recorded.
- Compare pre-refactor vs post-refactor cycles per minute, open count, missed-book count, and PnL.
- Only promote changes that reduce latency without increasing stale/missing signal decisions.
