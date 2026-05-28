# Security

Do not commit secrets, API keys, private keys, wallet credentials, or generated runtime journals.

Use environment variables for credentials:

- `POLYMARKET_PRIVATE_KEY`
- `POLYMARKET_API_KEY`
- `POLYMARKET_API_SECRET`
- `POLYMARKET_API_PASSPHRASE`
- `POLYBACKTEST_API_KEY`

If a secret is ever pasted into chat, committed, or written to logs, revoke or rotate it before publishing the repository.
