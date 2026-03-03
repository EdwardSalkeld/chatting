# Running Live Mode

This app supports a long-running mode that polls configured connectors, executes tasks, and dispatches responses for private single-user operation.

See also:
- [Quick Start](/home/edward/chatting/docs/quick-start.md)
- [Debug And Test Guide](/home/edward/chatting/docs/debug-and-test.md)
- [Connector Docs](/home/edward/chatting/docs/connectors/README.md)

## 1) Set secrets

```bash
export CHATTING_IMAP_PASSWORD='your-imap-password'
export CHATTING_SMTP_PASSWORD='your-smtp-password'
export CHATTING_TELEGRAM_BOT_TOKEN='your-telegram-bot-token'
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

## 5) Telegram-only smoke run

Set the following config fields in `configs/live-runtime.example.json` (or a copy):
- `"telegram_enabled": true`
- `"use_stub_executor": true`
- `"max_loops": 1`
- Remove or leave unset IMAP/SMTP fields if you only want Telegram for this smoke pass.

Then run:

```bash
python3 -m app.main --run-live --config configs/live-runtime.example.json
```

## Notes

- `--smtp-host` is required when `--imap-host` is set, so inbound email can be answered.
- Set `"telegram_enabled": true` to turn on Telegram long polling + outbound Telegram replies.
- CLI flags override config file values.
- Add one or more `--context-ref` flags to append extra context refs beyond config.
- Run `python3 -m app.main --db-path /tmp/chatting-live.db --list-metrics` to output run metrics JSON.
- Run `python3 -m app.main --db-path /tmp/chatting-live.db --serve-metrics --metrics-port 8080` to expose `/metrics` and `/dashboard`.

## 6) Run as a `systemd` service

Use the bundled unit template [chatting-live.service](/home/edward/chatting/deploy/systemd/chatting-live.service)
and environment template [chatting-live.env.example](/home/edward/chatting/configs/chatting-live.env.example).

1. Create the runtime environment file (no `export` prefixes):

```bash
cp /home/edward/chatting/configs/chatting-live.env.example /home/edward/chatting/configs/chatting-live.env
chmod 600 /home/edward/chatting/configs/chatting-live.env
```

2. Edit `/home/edward/chatting/configs/chatting-live.env` and set real secret values.

3. Install and start the service:

```bash
sudo cp /home/edward/chatting/deploy/systemd/chatting-live.service /etc/systemd/system/chatting-live.service
sudo systemctl daemon-reload
sudo systemctl enable --now chatting-live.service
```

4. Check service status and logs:

```bash
systemctl status chatting-live.service --no-pager
journalctl -u chatting-live.service -f
```

5. Restart after config/code updates:

```bash
sudo systemctl restart chatting-live.service
```
