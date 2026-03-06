# Running Split Mode With BBMB

This mode runs `chatting` as two processes:
- `message-handler` on `UserOne` (connectors + outbound dispatch)
- `worker` on `UserTwo` (routing + executor + policy)

BBMB sits in the middle over TCP.

Queues are hardcoded:
- `chatting.tasks.v1`
- `chatting.egress.v1`

## Topology

- Host A (`UserOne`): `python3 -m app.main_message_handler`
- Host B (`UserTwo`): `python3 -m app.main_worker`
- Host C (or A/B): `bbmb-server` on `:9876`

All hosts must have network reachability to the BBMB TCP endpoint.

## 1) Configure message-handler (`UserOne`)

```bash
cp configs/message-handler-runtime.example.json /tmp/message-handler.json
# edit bbmb_address and connector settings
python3 -m app.main_message_handler --config /tmp/message-handler.json
```

Optional env-based config path:

```bash
export CHATTING_MESSAGE_HANDLER_CONFIG_PATH=/tmp/message-handler.json
python3 -m app.main_message_handler
```

## 2) Configure worker (`UserTwo`)

```bash
cp configs/worker-runtime.example.json /tmp/worker.json
# edit bbmb_address and executor settings
python3 -m app.main_worker --config /tmp/worker.json
```

If the service/user shell working directory is not where you want Codex to run, set
`codex_working_dir` in worker config (or pass `--codex-working-dir`) to control only
the Codex subprocess cwd without changing the worker service `WorkingDirectory`.

Optional env-based config path:

```bash
export CHATTING_WORKER_CONFIG_PATH=/tmp/worker.json
python3 -m app.main_worker
```

## 3) Security boundary expectations

- `message-handler` owns integration secrets (`IMAP`, `SMTP`, `Telegram`).
- `worker` does not read integration secrets and does not dispatch directly.
- Egress is strict: if a task is unknown to the ingress ledger, it is logged and dropped.
- Egress channel dispatch is allowlist-gated by `allowed_egress_channels`.

## 4) Run as `systemd` services

Use:
- `deploy/systemd/chatting-message-handler.service`
- `deploy/systemd/chatting-worker.service`

Env templates:
- `configs/chatting-message-handler.env.example`
- `configs/chatting-worker.env.example`

Install pattern mirrors existing live service setup:

```bash
sudo cp deploy/systemd/chatting-message-handler.service /etc/systemd/system/
sudo cp deploy/systemd/chatting-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now chatting-message-handler.service
sudo systemctl enable --now chatting-worker.service
```
