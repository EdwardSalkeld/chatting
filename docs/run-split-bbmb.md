# Running Split Mode With BBMB

`chatting` runs as three services:
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

- Host A (integration host): `uv run python -m app.main_message_handler`
- Host B (execution host): `uv run python -m app.main_worker`
- Host C (or A/B): `bbmb-server` on `:9876`

All hosts must have network reachability to the BBMB TCP endpoint.

## 1) Start BBMB

```bash
bbmb-server
```

If BBMB is listening somewhere else, set `bbmb_address` in both runtime configs.

## 2) Configure message-handler

```bash
uv sync
cp configs/message-handler-runtime.example.json /tmp/message-handler.json
# edit bbmb_address and connector settings
uv run python -m app.main_message_handler --config /tmp/message-handler.json
```

Optional env-based config path:

```bash
export CHATTING_MESSAGE_HANDLER_CONFIG_PATH=/tmp/message-handler.json
uv run python -m app.main_message_handler
```

## 3) Configure worker

```bash
cp configs/worker-runtime.example.json /tmp/worker.json
# edit bbmb_address and executor settings
uv run python -m app.main_worker --config /tmp/worker.json
```

If the service/user shell working directory is not where you want Codex to run, set
`codex_working_dir` in worker config (or pass `--codex-working-dir`) to control only
the Codex subprocess cwd without changing the worker service `WorkingDirectory`.

The worker also serves a local read-only activity page by default at `http://127.0.0.1:9465/`
with JSON at `/activity.json`. Use `activity_host`, `activity_port`, and
`activity_history_limit` to change the bind or retention window.

Optional env-based config path:

```bash
export CHATTING_WORKER_CONFIG_PATH=/tmp/worker.json
uv run python -m app.main_worker
```

## 4) Security boundary expectations

- `message-handler` owns integration secrets (`IMAP`, `SMTP`, `Telegram`).
- `worker` does not read integration secrets and does not dispatch directly.
- Egress is strict: if a task is unknown to the ingress ledger, it is logged and dropped.
- Worker emits zero or more task-scoped visible `message` egress events and exactly one terminal
  internal `completion` event so `message-handler` can close the task and reject future egress.
- Egress channel dispatch is allowlist-gated by `allowed_egress_channels`.

## 5) Configure GitHub assignment polling (in message-handler)

```bash
# edit message-handler config: github_repositories (owner/repo or owner/*)
# optional: github_assignee_login (defaults to authenticated gh user)
uv run python -m app.main_message_handler --config /tmp/message-handler.json
```

`gh` CLI must already be authenticated on the message-handler host for both polling and issue-comment egress.

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

## 7) Publish a visible reply from worker side

Use the worker-side CLI to push a visible egress event directly to BBMB. Executors should use this
path for both quick acknowledgements and final user-visible answers instead of returning replies in
their stdout JSON:

```bash
uv run python -m app.main_reply task:email:53 \
  --message "working on it" \
  --channel email \
  --target alice@example.com \
  --config /tmp/worker.json
```

Notes:
- `message_type` is `chatting.egress.v2` with `event_kind=incremental`.
- These events are intentionally unsequenced and dispatch immediately at `message-handler`.
- Executor stdout is completion-only; visible replies belong here.
- `--event-id` can be supplied for stable idempotency across retries.
- Telegram reactions use the same CLI, but publish `telegram_reaction` egress under the hood:

```bash
uv run python -m app.main_reply task:telegram:53 \
  --channel telegram \
  --target 8605042448 \
  --telegram-reaction "👍" \
  --config /tmp/worker.json
```

- If `--telegram-message-id` is omitted, `app.main_reply` looks up the inbound Telegram `message_id` from the task ledger in `db_path`.

## 8) Docker worker CLI auth bootstrap

When running with `docker-compose.yml`, the worker image includes both `codex` and `claude` CLIs.
Auth is still external to the app and must be completed once interactively, then persisted in Docker
volumes.

The compose file mounts:
- `codex-auth` -> `/root/.codex`
- `claude-auth` -> `/root/.claude`

One-time bootstrap:

```bash
docker compose run --rm worker codex login
docker compose run --rm worker claude login
```

Then run the stack normally:

```bash
docker compose up -d
```

The compose stack publishes the worker activity UI on `9465`.
