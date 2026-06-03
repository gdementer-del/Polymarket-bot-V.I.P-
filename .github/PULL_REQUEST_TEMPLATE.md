## Summary

-

## Validation

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo test --locked`
- [ ] `cargo clippy --locked --all-targets -- -D warnings`
- [ ] `cargo build --release --locked`

## Strategy Or Runtime Impact

- [ ] No strategy behavior changes
- [ ] Strategy behavior changed and evidence is attached
- [ ] Runtime/storage/config behavior changed and migration notes are included

## Safety Checklist

- [ ] No secrets, API keys, private keys, generated journals, or local state
- [ ] Paper-first behavior is preserved unless explicitly reviewed
- [ ] New claims about PnL or latency are backed by data
