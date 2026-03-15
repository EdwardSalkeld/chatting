# Quick Start

This system now runs in split mode only:
- `message-handler`
- `worker`
- `bbmb-server`

## Prerequisites

- Python 3.13+
- Access to a shell
- `bbmb-server` available
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

## 3) Configure split mode

```bash
cp configs/message-handler-runtime.example.json /tmp/message-handler.json
cp configs/worker-runtime.example.json /tmp/worker.json
# edit bbmb_address and connector/executor settings as needed
```

## 4) Start BBMB

```bash
bbmb-server
```

By default `chatting` expects BBMB on `127.0.0.1:9876`.

## 5) Start chatting services

```bash
python3 -m app.main_message_handler --config /tmp/message-handler.json
python3 -m app.main_worker --config /tmp/worker.json
```

Or use the provided systemd unit templates in `deploy/systemd/`.

The message handler also exposes Prometheus-style metrics at `http://127.0.0.1:9464/metrics` by default. You can override the bind host and port with `metrics_host` and `metrics_port` in the message-handler config or the matching CLI flags.

## 6) Query state and metrics

`app.cli` is the preferred admin/query entrypoint. `app.main` remains as a compatibility alias.

```bash
python3 -m app.cli --db-path /tmp/chatting-message-handler.db --list-runs --limit 20
python3 -m app.cli --db-path /tmp/chatting-message-handler.db --list-audit-events --limit 20
python3 -m app.cli --db-path /tmp/chatting-message-handler.db --list-metrics
```

## Notes

- For the queue-by-queue runtime conversation, payload examples, and config levers, see
  [BBMB Message Flow](bbmb-message-flow.md).
- For full split-mode setup and operational details, see [Run Split Mode (BBMB)](run-split-bbmb.md).
- `app.cli`/`app.main` no longer run bootstrap/live runtime execution.
- `app.cli` reads one SQLite database at a time. In split mode, point it at either the
  message-handler DB or the worker DB depending on what you want to inspect.
