# Running Live Email + Scheduler Mode

This app now supports a long-running mode that polls scheduler jobs and IMAP email, executes tasks, and dispatches responses.

## 1) Set secrets

```bash
export CHATTING_IMAP_PASSWORD='your-imap-password'
export CHATTING_SMTP_PASSWORD='your-smtp-password'
```

## 2) Configure schedule jobs and runtime settings

Use [configs/live-schedule.example.json](/home/edward/chatting/configs/live-schedule.example.json) as a template.
Use [configs/live-runtime.example.json](/home/edward/chatting/configs/live-runtime.example.json) as the main runtime config template.

## 3) Smoke run (short command, no Codex dependency)

Runs one loop with the stub executor while still using real scheduler/IMAP/SMTP connectors.

```bash
python3 -m app.main --run-live --config configs/live-runtime.example.json
```

## 4) Live run with Codex executor

Set `"use_stub_executor": false` in config, then run:

```bash
python3 -m app.main --run-live --config configs/live-runtime.example.json
```

## Notes

- `--smtp-host` is required when `--imap-host` is set, so inbound email can be answered.
- CLI flags override config file values.
- Add one or more `--context-ref` flags to append extra context refs beyond config.
