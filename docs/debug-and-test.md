# Debug And Test Guide

## Fast test commands

- Full suite:
```bash
python3 -m unittest discover -s tests
```
- CI-equivalent command (same as GitHub Actions workflow):
```bash
python3 -m unittest discover -s tests
```
- Main flow tests:
```bash
python3 -m unittest tests.test_main
```
- Connectors:
```bash
python3 -m unittest tests.test_connectors
```
- Executor parser checks:
```bash
python3 -m unittest tests.test_executor.ParseExecutionResultTests
```
- State store checks:
```bash
python3 -m unittest tests.test_sqlite_store tests.test_state_contract
```
- Split-mode runtime coverage:
```bash
python3 -m unittest tests.test_worker_runtime tests.test_message_handler_runtime tests.test_main_reply
```
- Split-mode smoke e2e:
```bash
python3 -m unittest tests.test_split_mode_e2e -v
```
This skips locally unless `CHATTING_BBMB_SERVER_BIN` points to a built `bbmb-server`.

## Useful runtime inspection commands

- Message-handler runtime help:
```bash
python3 -m app.main_message_handler --help
```
- Worker runtime help:
```bash
python3 -m app.main_worker --help
```
- Immediate reply CLI help:
```bash
python3 -m app.main_reply --help
```
- Admin/query CLI help:
```bash
python3 -m app.cli --help
```
- List runs:
```bash
python3 -m app.cli --db-path /tmp/chatting-state.db --list-runs --limit 50
```
- List audit events:
```bash
python3 -m app.cli --db-path /tmp/chatting-state.db --list-audit-events --limit 50
```
- List dead letters:
```bash
python3 -m app.cli --db-path /tmp/chatting-state.db --list-dead-letters --result-status pending
```
- Replay dead letters with stub executor:
```bash
python3 -m app.cli --db-path /tmp/chatting-state.db --replay-dead-letters --use-stub-executor
```
- List persisted metrics summary:
```bash
python3 -m app.cli --db-path /tmp/chatting-state.db --list-metrics
```

## Common failures and fixes

- `missing IMAP password env var`:
  set `CHATTING_IMAP_PASSWORD` (or matching `imap_password_env`).
- `missing SMTP password env var`:
  set `CHATTING_SMTP_PASSWORD` (or matching `smtp_password_env`).
- `missing Telegram bot token env var`:
  set `CHATTING_TELEGRAM_BOT_TOKEN` (or matching `telegram_bot_token_env`).
- `config contains unknown keys`:
  remove typo keys from JSON config.
- `context_ref/context_refs entries must not be empty`:
  remove blank strings from context refs.
- `schedule job ... unknown keys` or type errors:
  validate schedule JSON against strict message-handler schedule-file parsing.

## Logging behavior

Key log lines:
- `retry_scheduled ...` transient executor failures
- `dead_letter ...` retries exhausted
- `dead_letter_recorded ...` persisted to DLQ table
- `run_observed ...` final per-run summary

Use these with DB queries to correlate outcomes.

## Suggested local debug loop

1. Run one failing envelope intentionally (`AlwaysFailExecutor` path in tests is a good reference).
2. Query runs + audit + dead letters.
3. Replay dead letters with stub executor.
4. Confirm replay result via `--list-runs` and dead-letter status via `--list-dead-letters`.

## CI notes

- Workflow file: `.github/workflows/ci.yml`
- Triggers: push to `main`, and pull requests targeting `main`
- Python version: `3.13`
- CI also builds `bbmb-server` and sets `CHATTING_BBMB_SERVER_BIN` before running the test suite.
