# Debug And Test Guide

Run `uv sync` once before using the commands below.

## Fast test commands

- Full suite:
```bash
uv run python -m unittest discover -s tests
```
- CI-equivalent command (same as GitHub Actions workflow):
```bash
uv sync --locked
uv run python -m unittest discover -s tests
```
- Main flow tests:
```bash
uv run python -m unittest tests.test_main
```
- Connectors:
```bash
uv run python -m unittest tests.test_connectors
```
- Executor parser checks:
```bash
uv run python -m unittest tests.test_executor.ParseExecutionResultTests
```
- State store checks:
```bash
uv run python -m unittest tests.test_sqlite_store tests.test_state_contract
```
- Split-mode runtime coverage:
```bash
uv run python -m unittest tests.test_worker_runtime tests.test_message_handler_runtime tests.test_main_reply
```
- Split-mode smoke e2e:
```bash
uv run python -m unittest tests.test_split_mode_e2e -v
```
This skips locally unless `CHATTING_BBMB_SERVER_BIN` points to a built `bbmb-server`.

## Useful runtime inspection commands

- Message-handler runtime help:
```bash
uv run python -m app.main_message_handler --help
```
- Worker runtime help:
```bash
uv run python -m app.main_worker --help
```
- Immediate reply CLI help:
```bash
uv run python -m app.main_reply --help
```
- Inspect recent runs:
```bash
sqlite3 /tmp/chatting-state.db "select run_id, result_status, created_at from run_records order by created_at desc limit 50;"
```
- Inspect recent audit events:
```bash
sqlite3 /tmp/chatting-state.db "select run_id, result_status, created_at from audit_events order by created_at desc limit 50;"
```
- Inspect pending dead letters:
```bash
sqlite3 /tmp/chatting-state.db "select dead_letter_id, status, created_at from dead_letters where status = 'pending' order by created_at desc;"
```
- Inspect persisted metrics summary:
```bash
sqlite3 /tmp/chatting-state.db "select result_status, count(*), avg(latency_ms) from run_records group by result_status order by result_status;"
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
3. Confirm the replay result by re-querying `run_records`, `audit_events`, and `dead_letters`.

## CI notes

- Workflow file: `.github/workflows/ci.yml`
- Triggers: push to `main`, and pull requests targeting `main`
- Python version: `3.13`
- CI installs `uv`, locks/syncs the project environment, downloads the latest BBMB release binary, verifies its published SHA256, and sets `CHATTING_BBMB_SERVER_BIN` before running the test suite.
