# Codex Edge Formula V1

This is the next strategy direction: replace scattered "if this then trade" tuning with one calibrated EV model.

There is no truly ideal formula. The practical goal is a formula that survives out-of-sample backtests, paper runs, spread, latency, slippage, and regime changes.

## Core Decision Rule

Only enter when:

```text
ev_per_share = fair_probability - executable_ask - fee_buffer - slippage_buffer - uncertainty_buffer
ev_per_share > min_edge_per_share
```

Position size should scale with confidence, not with hope:

```text
notional = base_notional * confidence_multiplier
```

Hard caps still apply: max open notional, max per-market notional, max daily/session loss, stale data guard, and max entry price.

## Fair Probability

The first model should be a bounded linear score converted into a probability:

```text
raw_score =
    w_gap      * aligned_target_gap_bps
  + w_15s      * aligned_15s_move_bps
  + w_5s       * aligned_5s_move_bps
  + w_1s       * aligned_1s_move_bps
  + w_accel    * aligned_micro_acceleration_bps
  + w_flow     * aligned_polymarket_flow_bps
  + w_book     * exchange_book_pressure_score
  + w_cross    * recent_target_cross_bonus
  - w_late     * late_window_penalty
  - w_chase    * expensive_entry_penalty
  - w_stale    * data_staleness_penalty

fair_probability = clamp(0.50 + raw_score / score_to_probability_scale, 0.05, max_fair_probability)
```

The important part is not the initial weights. The important part is that every trade has one auditable probability estimate.

## Initial Weight Bias

Based on the old v4 PolyBacktest slice, the strongest early clue was not the 1-second move. It was sustained 15-second alignment.

Observed on the old `config.bonereaper-state-v4.toml` 20-window BTC slice:

- `aligned_15s >= 2 bps`: 40 trades, 72.5% hit rate, +99.05 realized.
- `aligned_15s < 2 bps`: 8 trades, 37.5% hit rate, -23.92 realized.
- `signal 8-12`: 26 trades, 76.92% hit rate, +66.05 realized.
- `ask 0.56-0.62`: 15 trades, 53.33% hit rate, -12.53 realized.
- `gap 2-5 bps`: 27 trades, 74.07% hit rate, +47.01 realized.

This suggests the next formula should favor:

- sustained 15-second alignment over tiny 1-second bursts;
- medium-strength signals over extreme gap chasing;
- executable value over high-price momentum chasing;
- orderbook confirmation only as a freshness/pressure bonus, not as the whole strategy.

## Candidate Entry Shape

The safest next candidate is not "more aggressive everywhere". It is aggressive only when the model says price is wrong.

```text
enter if:
  fair_probability >= executable_ask + 0.08
  aligned_15s_move_bps >= 2.0
  aligned_5s_move_bps >= 1.2
  target_gap_abs_bps between 2.0 and 7.0, unless entry price <= 0.45
  executable_ask <= 0.67
  if executable_ask > 0.56: require stronger fair edge and fresh confirmation
  if aligned_15s_move_bps < 2.0: reject, unless deep discount and book/flow are extreme
```

## What Must Be Removed From Promotion Logic

Do not promote a change just because one 30-minute paper run was positive.

Do not promote a change if it only increases trade count by accepting:

- weak 15-second alignment;
- stale micro moves;
- premium entries without a larger fair edge;
- low gap chop;
- one-side orderbook pressure with no Polymarket price value.

## Validation Gate

A candidate formula can move to paper only if it beats the frozen baseline on the same PolyBacktest data:

```text
signals >= 30
realized_pnl > baseline_realized_pnl
hit_rate >= baseline_hit_rate - 5 percentage points
max_loss_bucket does not get worse
ask 0.56-0.62 bucket is not negative
aligned_15s < 2 bucket is either empty or non-negative
```

After that, run controlled paper for 30-60 minutes and compare live buckets against backtest buckets.
