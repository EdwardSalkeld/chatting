# Quick Start

This system is designed for private, single-user operation on one machine.

## Prerequisites

- Python 3.13+
- Access to a shell
- Optional: Codex CLI if you want real executor mode (`codex exec --json`)

## 1) Clone and enter repo

```bash
git clone <your-repo-url>
cd chatting
```

## 2) Run tests once

```bash
python3 -m unittest discover -s tests
```

## 3) Bootstrap run (no external services)

```bash
python3 -m app.main --db-path /tmp/chatting-state.db
```

What this does:
- runs fake cron + fake email envelopes
- applies routing/execution/policy/applier pipeline
- persists run + audit history in SQLite

## 4) Inspect persisted history

```bash
python3 -m app.main --db-path /tmp/chatting-state.db --list-runs --limit 20
python3 -m app.main --db-path /tmp/chatting-state.db --list-audit-events --limit 20
python3 -m app.main --db-path /tmp/chatting-state.db --list-metrics
```

## 5) Live mode (real connectors)

Use runtime config:

```bash
cp configs/live-runtime.example.json /tmp/chatting-live.json
# edit /tmp/chatting-live.json to enable only connectors you need
python3 -m app.main --run-live --config /tmp/chatting-live.json
```

## Required environment variables by connector

- IMAP connector: `CHATTING_IMAP_PASSWORD` (or custom `imap_password_env`)
- SMTP sender: `CHATTING_SMTP_PASSWORD` (or custom `smtp_password_env`)
- Telegram connector/sender: `CHATTING_TELEGRAM_BOT_TOKEN` (or custom `telegram_bot_token_env`)

## Minimal Telegram smoke run

1. Set token:
```bash
export CHATTING_TELEGRAM_BOT_TOKEN='...'
```
2. In config set:
- `"telegram_enabled": true`
- `"use_stub_executor": true`
- `"max_loops": 1`
3. Run:
```bash
python3 -m app.main --run-live --config /tmp/chatting-live.json
```

## Notes

- `--config` values are used first; explicit CLI flags override config.
- This project intentionally targets single-user reliability and auditability, not distributed scale.
- For split-process operation with BBMB (`message-handler` + `worker`), see [Run Split Mode (BBMB)](/home/edward/chatting/docs/run-split-bbmb.md).
