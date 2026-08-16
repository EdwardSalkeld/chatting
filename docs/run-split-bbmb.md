# Running Split Mode With BBMB

`chatting` runs under Docker Compose as three services by default, or four when auxiliary webhook
ingress is enabled:
- `message-handler` on the integration host (connectors + outbound dispatch)
- `worker` on the execution host (routing + executor + policy)
- `bbmb-server` in the middle (message bus)
- optional `auxiliary-ingress` on a web-facing host (secret-path JSON POST listener)

GitHub assignment polling is part of the Go `message-handler` when configured.

BBMB sits in the middle over TCP.

Queues (ingress only — egress does not use BBMB):
- auxiliary ingress uses one configured queue per route
- `chatting.tasks.v1`

Egress is a synchronous HTTP call, not a queue:
- The worker (and `app.main_reply`) POST `message_type: "chatting.egress.v2"` payloads to the
  handler's egress endpoint (`handler_egress_url`, default `http://127.0.0.1:9467/egress`) and get
  the delivery outcome back.
- The `chatting.egress.v1` queue is retired from the live path.

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

## 2) Choose the runtime image

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

## 3) Start the stack

The worker still needs a host workspace bind so the executor can operate on real repos:

```bash
export LOCAL_WORKSPACE=/absolute/path/to/the/workspace/codex-should-use
```

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

The default compose stack also runs a simple static preview service on `http://127.0.0.1:9466/`.
It serves a Docker-managed `html-output` volume that is mounted read-write into the worker at
`/workspace/html`. The worker still uses the existing `${LOCAL_WORKSPACE}:/workspace` bind for its
main working tree, and the extra volume just provides a stable place for worker-generated HTML
reports without tying that output to a checkout-specific host path.

## 3.5) Optional auxiliary webhook ingress

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

## 4) Security boundary expectations

- `message-handler` owns integration secrets (`IMAP`, `SMTP`, `Telegram`).
- `worker` does not read integration secrets and does not dispatch directly.
- Egress is strict: if a task is unknown to the ingress ledger, it is logged and dropped.
- Worker emits zero or more task-scoped visible `message` egress events and exactly one terminal
  internal `completion` event so the Go `message-handler` can close the task and reject future egress.
- Egress channel dispatch is allowlist-gated by `allowed_egress_channels`.

## 5) Configure GitHub assignment polling (in message-handler)

Edit message-handler config:
- `github_repositories` (`owner/repo` or `owner/*`)
- optional `github_assignee_login` (defaults to authenticated `gh` user)

`gh` CLI must already be authenticated on the message-handler host for both polling and issue-comment egress.

## 6) Publish a visible reply from worker side

Use the worker-side CLI to submit a visible egress event to the handler's synchronous egress
endpoint (it POSTs to `handler_egress_url`, not BBMB, and exits non-zero if the handler drops or
can't reach the send). Executors should use this path for both quick acknowledgements and final
user-visible answers instead of returning replies in their stdout/stderr transcript.

The reply is described by a JSON spec file (written with the executor's editor, never via the
shell) and passed with `--spec-file`, so the body is never a shell argument. Run it with `-P` so a
workspace checkout's stale `app/` can't shadow the deployed module:

```bash
cat > /tmp/reply.json <<'JSON'
{"task_id": "task:email:53", "channel": "email", "target": "alice@example.com",
 "message": "working on it"}
JSON
docker compose exec worker python -P -m app.main_reply --spec-file /tmp/reply.json \
  --config /config/worker.json
```

Notes:
- `message_type` is `chatting.egress.v2` with `event_kind=incremental`.
- These events are intentionally unsequenced and dispatch immediately at `message-handler`.
- The inline `--message` flag is retired: the body only ever comes from `--spec-file`, so shell
  metacharacters (backticks, `$`, quotes, newlines) can't mangle or split the reply.
- Executor stdout/stderr are treated as operator transcript and audit detail, not user-visible reply transport.
- `event_id` in the spec can be supplied for stable idempotency across retries.
- Telegram reactions go in the spec too (`"telegram_reaction": "👍"`). When a reaction is
  supplied with a message or attachment, `app.main_reply` sends both. If `telegram_message_id`
  is omitted, `app.main_reply` looks up the inbound Telegram `message_id` from the task ledger in `db_path`.
- For Telegram, the worker keeps draining BBMB into a durable inbox while Codex runs. At reply
  time, `app.main_reply` peeks by opaque conversation ID. If newer same-chat/topic turns exist,
  exit code `4` returns them in `follow_ups`, withholds the drafted text or attachment, and tells
  the executor to incorporate them and call `main_reply` again. A combined reaction may still be
  sent as acknowledgement. Other chats are never claimed.
- Incorporated tasks receive their own completion and run record without launching another
  executor. Their run page links to the parent run that answered the bundle.
- Telegram's native reply metadata is preserved. For a reply-quoted task, the executor receives a
  `history_contract` with the supported `app.main_history` command for retrieving nearby turns
  around the quoted `(chat_id, message_id)` anchor. The worker ledger starts at deployment time,
  so older anchors may initially be absent; no handler-history migration or cutover is required.

- If `--telegram-message-id` is omitted, `app.main_reply` looks up the inbound Telegram `message_id` from the task ledger in `db_path`.

## 7) Docker worker CLI auth bootstrap

When running with `docker-compose.yml`, the worker image includes `codex`, `claude`, and `hugo`.
Auth is still external to the app and must be completed once interactively, then persisted in
Docker volumes.

If the mounted workspace contains a Hugo site, you can verify the tool is present with:

```bash
docker compose run --rm worker hugo version
```

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
