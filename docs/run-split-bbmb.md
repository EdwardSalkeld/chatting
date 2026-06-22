# Running Split Mode With BBMB

`chatting` runs under Docker Compose as three services by default, or four when auxiliary webhook
ingress is enabled:
- `message-handler` on the integration host (connectors + outbound dispatch)
- `worker` on the execution host (routing + executor + policy)
- `bbmb-server` in the middle (message bus)
- optional `auxiliary-ingress` on a web-facing host (secret-path JSON POST listener)

GitHub assignment polling is part of the Go `message-handler` when configured.

BBMB sits in the middle over TCP.

Queues:
- auxiliary ingress uses one configured queue per route
- `chatting.tasks.v1`
- `chatting.egress.v1`

Egress payload contract is v2-only:
- Queue name `chatting.egress.v1` is transport-level only.
- Worker publishes `message_type: "chatting.egress.v2"`.
- `message-handler` rejects legacy `chatting.egress.v1` payload types.

For a worked message example and the full message-handler <-> worker conversation, see
[BBMB Message Flow](bbmb-message-flow.md).

## Topology

- `handler`: `chatting-handler --config /config/handler.json`
- `worker`: `python -m app.main_worker`
- `bbmb`: `bbmb-server` on `:9876`
- optional `auxiliary-ingress`: `python -m app.main_auxiliary_ingress`

All app services must have network reachability to the BBMB TCP endpoint. In the default compose
stack, that address is `bbmb:9876`.

## 1) Prepare config

```bash
mkdir -p configs/handler configs/worker
cp configs/handler.json.example configs/handler/handler.json
cp configs/worker.json.example configs/worker/worker.json
cp configs/handler.env.example configs/handler/handler.env
cp configs/worker.env.example configs/worker/worker.env
```

Edit the copied configs and env files for the target integrations and executor provider secrets.

## 2) Set the worker workspace mount

```bash
export LOCAL_WORKSPACE=/absolute/path/to/the/workspace/codex-should-use
```

## 3) Choose the runtime image

The default compose stack pulls the published runtime image from GHCR:

```bash
export CHATTING_RUNTIME_IMAGE=ghcr.io/edwardsalkeld/chatting:latest
```

You can pin that to a published `sha-<commit>` tag when you want a fixed deploy.

If the host has not already authenticated to GHCR, log in once with a token that
has package read access:

```bash
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin
```

## 4) Start the stack

```bash
docker compose pull
docker compose up -d
```

Optional auxiliary ingress connector settings in message-handler config:
- `auxiliary_ingress_enabled`
- `auxiliary_ingress_queues`
- `auxiliary_ingress_context_refs`

If the container working directory is not where you want Codex to run, set `codex_working_dir` in
worker config to control only the Codex subprocess cwd without changing the worker service working
directory.

The worker also serves a local read-only activity page by default at `http://127.0.0.1:9465/`
with JSON at `/activity.json`. The bind stays fixed at `9465`; use
`activity_history_limit` to change the retention window.

## 4.5) Optional auxiliary webhook ingress

```bash
uv run python -m app.main_auxiliary_ingress \
  --bbmb-address 127.0.0.1:9876 \
  --ingress-route generic-post:very-secret-path
```

This service accepts JSON `POST` requests on any configured secret path and publishes only the
parsed JSON body into the configured queue for that route.

Example config-driven setup:

```json
{
  "bbmb_address": "127.0.0.1:9876",
  "ingress_routes": ["generic-post:12334", "new-service:secret-two"]
}
```

With that config, auxiliary ingress listens on `/12334` and `/secret-two`, and publishes those
JSON bodies to `generic-post` and `new-service` respectively. To make the handler poll those same
queues, add `auxiliary_ingress_queues: ["generic-post", "new-service"]` to the message-handler
config.

## 5) Security boundary expectations

- `message-handler` owns integration secrets (`IMAP`, `SMTP`, `Telegram`).
- `worker` does not read integration secrets and does not dispatch directly.
- Egress is strict: if a task is unknown to the ingress ledger, it is logged and dropped.
- Worker emits zero or more task-scoped visible `message` egress events and exactly one terminal
  internal `completion` event so the Go `message-handler` can close the task and reject future egress.
- Egress channel dispatch is allowlist-gated by `allowed_egress_channels`.

## 6) Configure GitHub assignment polling (in message-handler)

Edit message-handler config:
- `github_repositories` (`owner/repo` or `owner/*`)
- optional `github_assignee_login` (defaults to authenticated `gh` user)

`gh` CLI must already be authenticated on the message-handler host for both polling and issue-comment egress.

## 7) Publish a visible reply from worker side

Use the worker-side CLI to push a visible egress event directly to BBMB. Executors should use this
path for both quick acknowledgements and final user-visible answers instead of returning replies in
their stdout/stderr transcript:

```bash
docker compose exec worker python -m app.main_reply task:email:53 \
  --message "working on it" \
  --channel email \
  --target alice@example.com \
  --config /config/worker.json
```

Notes:
- `message_type` is `chatting.egress.v2` with `event_kind=incremental`.
- These events are intentionally unsequenced and dispatch immediately at `message-handler`.
- Executor stdout/stderr are treated as operator transcript and audit detail, not user-visible reply transport.
- `--event-id` can be supplied for stable idempotency across retries.
- Telegram reactions use the same CLI, but publish `telegram_reaction` egress under the hood:

```bash
docker compose exec worker python -m app.main_reply task:telegram:53 \
  --channel telegram \
  --target 8605042448 \
  --telegram-reaction "👍" \
  --config /config/worker.json
```

- If `--telegram-message-id` is omitted, `app.main_reply` looks up the inbound Telegram `message_id` from the task ledger in `db_path`.

## 8) Docker worker CLI auth bootstrap

When running with `docker-compose.yml`, the worker image includes both `codex` and `claude` CLIs.
Auth is still external to the app and must be completed once interactively, then persisted in Docker
volumes.

The compose file mounts:
- `codex-auth` -> `/home/chatting/.codex`
- `claude-auth` -> `/home/chatting/.claude`
- `gh-auth` -> `/home/chatting/.config/gh`

One-time bootstrap:

```bash
docker compose run --rm worker gh auth login
docker compose run --rm worker codex login
docker compose run --rm worker claude login
```

The compose stack publishes the worker activity UI on `9465`.

The runtime image already sets Git's credential helper to `gh auth git-credential` at the system
level, so plain `git push` keeps working after container replacement as long as the `gh-auth`
volume still contains a valid `gh` login.
