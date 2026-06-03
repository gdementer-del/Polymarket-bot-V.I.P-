# Contributing

Thanks for helping improve Polymarket Research Toolkit. The project is focused
on open research, data collection, paper trading, and reproducible strategy
evaluation for short prediction-market windows.

## Ground Rules

- Keep the default workflow paper-first and research-first.
- Do not commit API keys, wallet credentials, private keys, generated journals,
  local state, or exchange data dumps.
- Do not add live-trading behavior without a separate design review, dry-run
  path, reconciliation plan, and explicit maintainer approval.
- Do not propose or implement market manipulation, wash trading, spoofing,
  platform bypasses, or use of non-public information.
- Strategy claims must be evidence-backed. Avoid statements that imply
  guaranteed profit.

## Development Setup

Install Rust `1.96.0` or newer, then run:

```powershell
cargo build --release --locked
```

For local validation before opening a pull request:

```powershell
cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo build --release --locked
```

## Issue Triage

Use issues for:

- reproducible bugs;
- data-source latency or correctness problems;
- strategy research proposals with clear hypotheses;
- documentation gaps;
- refactoring work that improves maintainability.

Good issues include reproduction steps, config profile, command line, expected
behavior, observed behavior, and any sanitized logs needed to diagnose the
problem.

## Pull Request Expectations

Pull requests should explain:

- what changed and why;
- how it was validated;
- whether behavior changes strategy selection, execution, storage, or reporting;
- whether new config fields require migration or documentation;
- whether a paper run, backtest, or unit test supports the change.

For strategy changes, include at least one of:

- unit tests for decision boundaries;
- PolyBacktest or replay summary;
- controlled paper-run report;
- clear reason why the change is refactor-only and should not alter behavior.

## Maintainer Workflow

Maintainers should keep the backlog actionable, close stale or invalid issues,
request evidence for strategy changes, and only promote profiles after
comparable validation. Releases should summarize user-facing changes, config
changes, known limitations, and validation status.
