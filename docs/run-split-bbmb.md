# Running Split Mode With BBMB

This mode runs `chatting` as a 3-part application:
- `message-handler` on the integration host (connectors + outbound dispatch)
- `worker` on the execution host (routing + executor + policy)
- `bbmb-server` in the middle (message bus)

GitHub assignment polling is part of `message-handler` when configured.

BBMB sits in the middle over TCP.

Queues are hardcoded:
- `chatting.tasks.v1`
- `chatting.egress.v1`

Egress payload contract is v2-only:
- Queue name `chatting.egress.v1` is transport-level only.
- Worker publishes `message_type: "chatting.egress.v2"`.
- `message-handler` rejects legacy `chatting.egress.v1` payload types.

For a worked message example and the full message-handler <-> worker conversation, see
[BBMB Message Flow](bbmb-message-flow.md).

## Topology

- Host A (integration host): `python3 -m app.main_message_handler`
- Host B (execution host): `python3 -m app.main_worker`
- Host C (or A/B): `bbmb-server` on `:9876`

All hosts must have network reachability to the BBMB TCP endpoint.

## 1) Start BBMB

```bash
bbmb-server
```

If BBMB is listening somewhere else, set `bbmb_address` in both runtime configs.

## 2) Configure message-handler

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

## 3) Configure worker

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

## 4) Security boundary expectations

- `message-handler` owns integration secrets (`IMAP`, `SMTP`, `Telegram`).
- `worker` does not read integration secrets and does not dispatch directly.
- Egress is strict: if a task is unknown to the ingress ledger, it is logged and dropped.
- Worker always emits a terminal final egress event; when there is no user-visible reply it emits a
  `drop/task` completion marker so `message-handler` can close the task and reject future egress.
- Egress channel dispatch is allowlist-gated by `allowed_egress_channels`.

## 5) Configure GitHub assignment polling (in message-handler)

```bash
# edit message-handler config: github_repositories (owner/repo or owner/*)
# optional: github_assignee_login (defaults to authenticated gh user)
# required for assignment-generated tasks: github_reply_channel_type and github_reply_channel_target
python3 -m app.main_message_handler --config /tmp/message-handler.json
```

`gh` CLI must already be authenticated on the message-handler host.

## 6) Run as `systemd` services

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

## 7) Publish an immediate incremental reply from worker side

Use the worker-side CLI to push an egress event directly to BBMB without waiting for worker loop completion:

```bash
python3 -m app.main_reply task:email:53 \
  --message "working on it" \
  --channel email \
  --target alice@example.com \
  --config /tmp/worker.json
```

Notes:
- `message_type` is `chatting.egress.v2` with `event_kind=incremental`.
- These ad-hoc events are intentionally unsequenced and dispatch immediately at message-handler.
- `--event-id` can be supplied for stable idempotency across retries.
