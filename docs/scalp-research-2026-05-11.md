# Codex Scalp Research - 2026-05-11

## Hypothesis

The edge is not in holding BTC 5m Polymarket positions to resolution. The possible edge is in catching a short repricing lag:

`Coinbase/Binance impulse appears -> Polymarket outcome ask is still not fully repriced -> enter -> exit quickly when the outcome bid catches up.`

## Implemented Research Model

PolyBackTest signals now include an optional `scalp_exit` simulation.

Rules:

| Parameter | Value |
| --- | ---: |
| Take profit | entry price + 0.08 |
| Stop loss | entry price - 0.05 |
| Time stop | 45 seconds |
| Exit price proxy | `1 - opposite ask` |
| Fees | `strategy.assumed_fee_bps` on entry and exit notional |

This is conservative enough for research, but it is still not a real bid/fill model. Live/paper code must use executable bid depth before this can be trusted.

## Runtime Exit Model

Paper runtime now has an explicit `run.early_exit.scalp_exit_enabled` mode for Codex Sentinel positions.

Rules:

| Parameter | Value |
| --- | ---: |
| Take profit | primary entry price + 0.08 |
| Stop loss | primary entry price - 0.05 |
| Time stop | 45 seconds |
| Exit price | executable primary outcome bid depth |
| Depth guard | top mark-to-market bid levels must cover the position shares |

The runtime model is stricter than the PolyBackTest proxy because it refuses the scalp close when visible bid depth cannot cover the full paper position.

## First Config

Created `config.codex-scalp-v1.toml` from `config.codex-sentinel.toml`.

Main changes:

| Parameter | Value | Reason |
| --- | ---: | --- |
| `codex_sentinel_v1_max_entry_price` | 0.67 | Remove expensive late-chase losses. |
| `codex_sentinel_v1_quality_floor_min_target_gap_bps` | 1.20 | Remove weak micro-gap entries. |
| `codex_sentinel_v1_quality_floor_mid_gap_min_flow_bps` | 0 | Keep the test focused on price impulse, not trade-flow availability. |
| `run.early_exit.min_take_profit_usdc` | 0.40 | Faster scalp-style profit capture. |
| `run.early_exit.max_loss_usdc` | 0.80 | Tighter loss cap for short-hold scalps. |

## Results

### Baseline Sentinel, 20 BTC 5m Windows

| Metric | Hold to resolution | Scalp exit model |
| --- | ---: | ---: |
| Signals | 5 | 5 |
| PnL | -12.7273 | +1.4123 |
| Wins | 1/5 | 3/5 |
| Exit reasons | n/a | 3 take-profit, 2 stop-loss |

### Codex Scalp V1, 20 BTC 5m Windows

| Metric | Hold to resolution | Scalp exit model |
| --- | ---: | ---: |
| Signals | 3 | 3 |
| PnL | -4.7273 | +2.6868 |
| Wins | 1/3 | 3/3 |
| Exit reasons | n/a | 3 take-profit |

### Codex Scalp V1, 50 BTC 5m Windows

| Metric | Hold to resolution | Scalp exit model |
| --- | ---: | ---: |
| Signals | 6 | 6 |
| PnL | -10.4773 | +4.9015 |
| Wins | 2/6 | 6/6 |
| Exit reasons | n/a | 6 take-profit |

Run artifact:

`runs/polybacktest-codex-scalp-v1-50w-20260511-152132`

### Codex Scalp V1, 20 BTC 5m Windows After Runtime Exit Wiring

| Metric | Hold to resolution | Scalp exit model |
| --- | ---: | ---: |
| Signals | 2 | 2 |
| PnL | -0.7273 | +1.3412 |
| Wins | 1/2 | 2/2 |
| Exit reasons | n/a | 2 take-profit |

### Controlled Paper Run, 2026-05-11 18:34:28 +07:00

Config: `config.codex-scalp-v1.toml`

Runtime: 30 minutes, paper mode, isolated start, drain enabled.

| Metric | Value |
| --- | ---: |
| Trade events | 4 |
| Opens | 2 |
| Closes | 2 |
| Realized PnL | -2.4220 |
| Win rate | 0% |
| Expectancy | -1.2110 |
| Max drawdown | 2.4220 |

Close categories:

| Category | Count |
| --- | ---: |
| `early_exit_hard_stop_loss` | 1 |
| `early_exit_scalp_stop_loss` | 1 |

Main losing pattern: entries above `0.58` were allowed with weak or stale fresh momentum. One trade entered at `0.66` with 5s confirmation `0.59 bps`; another entered at `0.60` with 1s/5s confirmation at zero. Both were valid under the previous signal/flow-heavy guard, but the real bid side repriced against us quickly.

Post-run config hardening:

| Parameter | New value | Reason |
| --- | ---: | --- |
| `codex_sentinel_v1_stale_micro_max_non_discount_entry_price` | 0.58 | Do not allow stale micro confirmation at premium prices. |
| `codex_sentinel_v1_entry_spread_guard_enabled` | true | Reject scalps where the executable exit bid is already too far below our entry ask. |
| `codex_sentinel_v1_max_entry_spread` | 0.05 | Keep immediate spread loss below most of the 0.08 scalp take-profit. |
| `codex_sentinel_v1_premium_entry_guard_enabled` | true | Require stronger confirmation before paying above fair-ish entry. |
| `codex_sentinel_v1_premium_entry_price` | 0.58 | Treat entries above 0.58 as premium scalps. |
| `codex_sentinel_v1_premium_min_signal_bps` | 0 | Let the existing state signal decide baseline eligibility; do not double-count signal scale differences between runtime and backtest. |
| `codex_sentinel_v1_premium_min_flow_bps` | 0 | Avoid blocking backtest-compatible premium entries solely because historical trade-flow is absent. |
| `codex_sentinel_v1_premium_min_fresh_bps` | 1.50 | Require actual fresh price impulse, not stale lag. |
| `codex_sentinel_v1_attack_size_enabled` | true | Double size only on discounted, confirmed attacks. |
| `codex_sentinel_v1_attack_notional_usdc` | 8 | Raise PnL ceiling while staying under the 10 USDC per-window cap. |
| `codex_sentinel_v1_confidence_sizing_enabled` | true | Add a small multiplier only when the confidence score is high. |
| `scalp_stop_loss_price_delta` | 0.04 | Cut bad scalps earlier. |
| `scalp_time_stop_secs` | 35 | Do not let dead scalps drift into settlement-like risk. |

Sanity check after first hardening: `cargo test` passed, and a compact 9-window PolyBackTest produced 0 signals, confirming that the stricter guard blocks the recent bad premium/stale entries rather than finding forced trades.

Second hardening pass for PnL scalability added a code-level executable spread guard and turned on guarded attack sizing. Validation: `cargo test` passed with 136 tests, including new unit coverage for wide/tight Codex Sentinel scalp entry spreads.

### Controlled Paper Run, 2026-05-12 20:05:28 +07:00

Config: `config.codex-scalp-v1.toml`

Runtime: 30 minutes, paper mode, isolated start, drain enabled.

Artifacts: `runs/codex-scalp-v1-paper-20260512-200528/`

| Metric | Value |
| --- | ---: |
| Trade events | 4 |
| Opens | 2 |
| Closes | 2 |
| Realized PnL | +1.1783 |
| Win rate | 50.00% |
| Expectancy | +0.5892 |
| Profit factor | 8.4016 |
| Max drawdown | 0.1592 |

Close categories:

| Category | Count |
| --- | ---: |
| `early_exit_scalp_take_profit` | 1 |
| `early_exit_scalp_time_stop` | 1 |

Entry profile changed in the intended direction. The previous losing run entered premium asks at `0.60` and `0.66`; this run entered only `0.55` and `0.50`. The attack-sized `0.55` entry closed via scalp take-profit after 4 seconds for `+1.3375`; the `0.50` flowless entry timed out after 35 seconds for `-0.1592`.

The old problematic `gap 1.50-3.00` bucket was positive in this run: 1 close, 1 win, `+1.3375`. The sample is still too small to call this stable edge, but the spread/premium hardening appears to have removed the immediate large-loss pattern observed on 2026-05-11.

Top filters during the run:

| Reason | Count |
| --- | ---: |
| `entry price is already too expensive for codex-sentinel-v1` | 22442 |
| `late entry needs stronger fresh momentum for codex-sentinel-v1` | 8186 |
| `15s confirmation is still too weak for codex-sentinel-v1` | 6016 |
| `entry spread is too wide for codex-sentinel-v1` | 598 |

### Controlled Paper Run, 2026-05-12 21:03:41 +07:00

Config: `config.codex-scalp-v1.toml`

Runtime: 2 hours, paper mode, isolated start, drain enabled.

Artifacts: `runs/codex-scalp-v1-paper-2h-20260512-210341/`

| Metric | Value |
| --- | ---: |
| Trade events | 26 |
| Opens | 13 |
| Closes | 13 |
| Realized PnL | +5.0904 |
| Win rate | 92.31% |
| Expectancy | +0.3916 |
| Profit factor | 10.9419 |
| Max drawdown | 0.5120 |
| Signals | 597 |
| Executed cycles | 13 |

Close categories:

| Category | Count |
| --- | ---: |
| `early_exit_exhaustion` | 8 |
| `early_exit_peak_exit` | 2 |
| `early_exit_scalp_take_profit` | 2 |
| `early_exit_scalp_stop_loss` | 1 |

Entry ask buckets:

| Bucket | Closes | Wins | Losses | PnL |
| --- | ---: | ---: | ---: | ---: |
| `ask <= 0.56` | 5 | 4 | 1 | +1.4419 |
| `ask 0.56-0.62` | 7 | 7 | 0 | +3.2911 |
| `ask 0.62-0.68` | 1 | 1 | 0 | +0.3573 |

Target-gap buckets:

| Bucket | Closes | Wins | Losses | PnL |
| --- | ---: | ---: | ---: | ---: |
| `gap 1.50-3.00` | 5 | 5 | 0 | +2.2942 |
| `gap 3.00-6.00` | 4 | 3 | 1 | +0.9918 |
| `gap >= 6.00` | 4 | 4 | 0 | +1.8043 |

The 2-hour run strongly supports the current direction: the previously problematic `gap 1.50-3.00` bucket stayed positive, the strategy avoided large losses, and the only losing close was a controlled scalp stop-loss of `-0.5120`.

Do not overfit yet. The result is encouraging, but still only 13 closed trades. The next validation target is 50-100 paper closes across different volatility regimes before increasing notional further or considering live mode.

### Boost Attempt And Guarded Follow-Up, 2026-05-13/14

Config: `config.codex-scalp-v1-boost.toml`

The first boost attempt raised bankroll room and selected high-confidence/attack sizing. It did not improve the strategy quality enough to accept as the new default.

| Run | Closed | W/L | PnL | Win rate | Profit factor | Max DD |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1h boost, 2026-05-13 23:08 +07 | 6 | 4/2 | +1.4221 | 66.67% | 1.8066 | 1.7632 |
| 30m safe boost, 2026-05-14 00:12 +07 | 2 | 0/2 | -0.9310 | 0% | 0 | 0.9310 |
| 30m guarded boost, 2026-05-14 00:54 +07 | 0 | 0/0 | 0.0000 | 0% | 0 | 0.0000 |
| Combined boost sample | 8 | 4/4 | +0.4912 | 50.00% | positive but weak | n/a |

Artifacts:

`runs/codex-scalp-v1-boost-paper-1h-20260513-230809/`

`runs/codex-scalp-v1-boost-paper-30m-safe-20260514-001222/`

`runs/codex-scalp-v1-boost-guarded-paper-30m-20260514-005416/`

The boost sample exposed two weak buckets that the baseline had mostly avoided:

| Pattern | Example | Fix |
| --- | --- | --- |
| Mid-gap premium chase | `ask 0.57-0.60`, `gap 1.50-3.00`, weak fresh/signal/flow | Added `codex_sentinel_v1_mid_gap_premium_guard_*`. |
| High ask chase near `0.66` | `ask 0.66`, fresh/swing around `2 bps`, negative MFE | Lowered expensive-entry threshold from `0.67` to `0.65` while keeping `3 bps` confirmation requirement. |

Important: this does not prove higher PnL yet. It only makes the next experiment cleaner by cutting the exact boost failure modes without tightening the high-gap winners that kept the baseline positive.

The guarded 30m smoke was intentionally conservative and produced zero trades. That is acceptable as a safety check but not enough for PnL growth; the next PnL work should restore trade frequency via cheaper/value entries or data/latency improvements, not by reopening the weak mid-gap premium chase. Runtime log spam from temporary empty order-book sides was also downgraded from `warn` to `debug` to avoid disk-heavy warning floods during reactive runs.

### Guarded Baseline Check, 2026-05-14 23:01 +07

Config: `config.codex-scalp-v1.toml`

Runtime: 30 minutes, paper mode, isolated start.

Artifacts: `runs/codex-scalp-v1-guarded-paper-30m-20260514-230119/`

| Metric | Value |
| --- | ---: |
| Trade events | 6 |
| Opens | 3 |
| Closes | 3 |
| Realized PnL | +1.7904 |
| Win rate | 66.67% |
| Expectancy | +0.5968 |
| Profit factor | 5.5981 |
| Max drawdown | 0.3894 |
| Open positions after run | 0 |

Close categories:

| Category | Count |
| --- | ---: |
| `early_exit_exhaustion` | 1 |
| `early_exit_scalp_stop_loss` | 1 |
| `early_exit_scalp_take_profit` | 1 |

The new mid-gap premium guard fired 741 times during the run (`mid-gap premium entry needs stronger quality for codex-sentinel-v1`). That is the intended behavior: the guard is active in live paper conditions and did not prevent all trading. The remaining loss came from `ask 0.64`, `gap 3.36`, weak signal/flow, which is outside the mid-gap guard range. The next improvement candidate is a separate `gap 3.00-6.00` premium guard, but only after more samples because the same bucket is still close to breakeven-positive in this short run.

## Interpretation

This is the first result that supports changing the strategy shape. The same entries are bad when held to resolution but positive when treated as short scalps. That matches the new hypothesis: the signal may predict a short Polymarket repricing, not the final five-minute outcome.

Confidence is still low because the sample is only 50 available windows and 6 signals. This is not live-ready. It is strong enough to continue research.

## Next Step

1. Run another controlled paper session with the hardened premium-entry guard.
2. If trade frequency is too low, add a separate cheap-entry profile instead of weakening premium entries.
3. Compare real bid-depth exits against the PolyBackTest proxy PnL.
4. Do not use live trading until the strategy has 100+ out-of-sample scalp exits with positive expectancy after execution costs.
