# Security

Do not commit secrets, API keys, private keys, wallet credentials, or generated runtime journals.

Use environment variables for credentials:

- `POLYMARKET_PRIVATE_KEY`
- `POLYMARKET_API_KEY`
- `POLYMARKET_API_SECRET`
- `POLYMARKET_API_PASSPHRASE`
- `POLYBACKTEST_API_KEY`

If a secret is ever pasted into chat, committed, or written to logs, revoke or rotate it before publishing the repository.

## Reporting Security Issues

Do not include private keys, API keys, wallet credentials, account identifiers,
or generated state dumps in public issues. Open a sanitized issue with the
minimum reproduction and note that sensitive details are available privately if
needed.

Security reports should include:

- affected command or workflow;
- config profile used;
- expected behavior;
- observed behavior;
- sanitized logs or stack traces;
- whether the issue can affect paper-only mode, live credentials, local files,
  or external services.

## Repository Hygiene

The following paths are intentionally excluded from git:

- `state/`
- `logs/`
- `run_logs/`
- `runs/`
- `tmp/`
- `target/`
- `.env`

Before publishing, run a secret scan over staged files and verify that generated
runtime artifacts are not included.
