# Strategy Kill/Keep Audit - 2026-05-10

## Decision

Live trading remains frozen. The current `bonereaper-state-v4` / v2 momentum line is not proven and should be treated as quarantined for live use.

The fresh 100-window PolyBackTest matrix did not produce a live-ready candidate. It showed that current `v4` and `hot` are effectively over-filtered on the tested slice, while `sentinel` is the only branch still emitting trades, but with a sample that is far too small to validate edge.

The project should not be deleted: the engine, journals, paper accounting, risk checks, Coinbase reactive feed, and reporting are useful research infrastructure. The strategy hypothesis is the weak part, not the whole codebase.

## Paper Evidence

| Config/state | Closed trades | PnL | Win rate | Profit factor | Max drawdown | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `bonereaper-state-v4` | 55 | -11.9140 | 45.45% | 0.7297 | 20.7739 | Kill for live, quarantine for research |
| `codex-sentinel` | 10 | +3.0588 | 70.00% | 2.3434 | 1.3315 | Keep as research candidate only |
| `codex-bonereaper-hot` | 34 | +7.8781 | 64.71% | 2.1490 | 4.1155 | Best current paper candidate, needs proper backtest |
| `codex-bonereaper-aggressive` | 35 | -6.0790 | 48.57% | 0.7980 | 10.4043 | Kill / archive |

## v4 Failure Pattern

`bonereaper-state-v4` is negative across the full accumulated journal despite many incremental guards.

Worst buckets:

| Bucket | Closed trades | PnL | Comment |
| --- | ---: | ---: | --- |
| `sec < 180` | 18 | -10.2055 | Late entries are structurally bad. |
| `gap 1.50-3.00` | 15 | -5.9495 | Mid-gap chop loses even after guards. |
| `gap 3.00-6.00` | 6 | -8.7813 | High-gap chase is too fragile. |
| `ask 0.62-0.68` | 4 | -4.5290 | Expensive entries have no proven edge. |

The latest filters reduced some damage, but did not prove a stable positive edge.

## PolyBackTest Status

Fresh matrix run:

`runs/polybacktest-killkeep-100w-20260510-200325`

The API key was supplied through the process environment only and was not written into config files or committed artifacts. Error logs were empty for all three runs.

| Config | Windows | Signals | W/L | Realized | Hit rate | Status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `config.bonereaper-hot.toml` | 100 | 0 | 0/0 | 0.0000 | 0.00% | Too restrictive / no validation sample |
| `config.sentinel.toml` | 100 | 3 | 2/1 | +3.8805 | 66.67% | Only live branch, but massively under-sampled |
| `config.bonereaper-state-v4.toml` | 100 | 0 | 0/0 | 0.0000 | 0.00% | Too restrictive / no validation sample |

Conclusion: no strategy passes the go/no-go gate. `sentinel` remains the best seed for the next research branch, but it is not proven. `v4` should stay quarantined for live use.

Follow-up attempt:

`runs/polybacktest-sentinel-500w-paged-20260510-233329`

I fixed the local PolyBackTest market pagination bug first: `--windows-per-target 500` was previously capped in our client by one `limit=100&offset=0` request. After the fix, the API still returned only 99 BTC 5m resolved windows on the current access level. Direct offset probing showed the same limit: `offset=0` returned 99 markets, `offset=99` returned 0 markets, and deeper offsets hit rate/payment limits. So a true 500-window validation is not available through the current PolyBackTest access.

The paged rerun matched the available 99-window sample:

| Config | Requested windows | Actual windows | Signals | W/L | Expected | Realized | Hit rate | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `config.sentinel.toml` | 500 | 99 | 4 | 3/1 | +6.8640 | +4.4872 | 75.00% | Positive but still far too under-sampled |

Existing cached run from `state/polybacktest-runs/20260506-225055/summary.csv` is not enough for a release decision:

| Config | Windows | Signals | Realized | Hit rate | Status |
| --- | ---: | ---: | ---: | ---: | --- |
| `config.bonereaper-state-v4.toml` | 20 | 48 | +75.1238 | 68.75% | Old config, conflicts with later paper |
| `config.sentinel.toml` | 20 | 7 | -1.5236 | 57.14% | Too small |
| `config.bonereaper-hot.toml` | 20 | 4 | -11.3793 | 25.00% | Too small, negative |
| `config.bonereaper-state-v2.toml` | 20 | - | - | - | Failed |

The cached run is useful as a smoke test only. It is not statistically reliable and does not match later paper behavior.

## Go/No-Go Criteria

Do not consider live until a strategy passes all of these:

| Criterion | Minimum |
| --- | --- |
| Out-of-sample closed trades | 100+ |
| Profit factor | > 1.15, preferably > 1.25 |
| Expectancy | Positive after fees/slippage |
| Max drawdown | Acceptable versus bankroll, not dominated by one loss cluster |
| Bucket health | No major timing/gap/ask bucket deeply negative |
| Single-trade dependency | No single win should explain most total PnL |
| Paper validation | 4-8 hours controlled paper after backtest, still positive |

## Next Step

Stop judging the strategy from short live/paper runs. The next useful work is a bounded research pass:

1. Use `codex-sentinel` as the seed because it is the only current branch that still emitted positive backtest trades.
2. Tune for signal density first: target 50-150 signals over 500+ out-of-sample BTC 5m windows, not 3 trades over 100 windows.
3. Keep strict kill criteria: profit factor above 1.15, positive expectancy after costs, no single-trade dependency, and no deeply negative timing/gap/entry-price bucket.
4. If the sentinel family cannot pass that gate, retire the Bonereaper-style momentum family and pivot to a new edge hypothesis.

Suggested next matrix:

```powershell
# Set POLYBACKTEST_API_KEY locally before running this command.
target\debug\polymarket_mvp.exe --config config.sentinel.toml polybacktest --windows-per-target 500 --entry-minutes 1 --target btc-5m
```

This command is only useful once the data provider can actually return more than roughly 100 windows. Until then, the project needs either upgraded PolyBackTest access, another historical data source, or a local snapshot collector before a statistically meaningful go/no-go decision can be made.
