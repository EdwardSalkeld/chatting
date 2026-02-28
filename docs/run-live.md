# Running Live Email + Scheduler Mode

This app now supports a long-running mode that polls scheduler jobs and IMAP email, executes tasks, and dispatches responses.

## 1) Set secrets

```bash
export CHATTING_IMAP_PASSWORD='your-imap-password'
export CHATTING_SMTP_PASSWORD='your-smtp-password'
```

## 2) Configure schedule jobs

Use [configs/live-schedule.example.json](/home/edward/chatting/configs/live-schedule.example.json) as a template.

## 3) Smoke run (no Codex dependency)

Runs one loop with the stub executor while still using real scheduler/IMAP/SMTP connectors.

```bash
python3 -m app.main \
  --run-live \
  --db-path /tmp/chatting-live.db \
  --schedule-file configs/live-schedule.example.json \
  --imap-host imap.example.com \
  --imap-username bot@example.com \
  --smtp-host smtp.example.com \
  --smtp-username bot@example.com \
  --smtp-from bot@example.com \
  --use-stub-executor \
  --max-loops 1
```

## 4) Live run with Codex executor

```bash
python3 -m app.main \
  --run-live \
  --db-path /tmp/chatting-live.db \
  --schedule-file configs/live-schedule.example.json \
  --imap-host imap.example.com \
  --imap-username bot@example.com \
  --smtp-host smtp.example.com \
  --smtp-username bot@example.com \
  --smtp-from bot@example.com \
  --codex-command "codex exec --json"
```

## Notes

- `--smtp-host` is required when `--imap-host` is set, so inbound email can be answered.
- Add one or more `--context-ref` values to enrich connector-generated envelopes.
- Use `--poll-interval-seconds` to control loop frequency.
