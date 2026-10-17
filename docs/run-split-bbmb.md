# Running Split Mode With BBMB

`chatting` runs under Docker Compose as three services:
- `message-handler` on the integration host (connectors + outbound dispatch)
- `worker` on the execution host (routing + executor + policy)
- `bbmb-server` in the middle (message bus)

GitHub assignment polling is part of the Go `message-handler` when configured.

BBMB sits in the middle over TCP.

Queues (ingress only — egress does not use BBMB):
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

If the container working directory is not where you want Codex to run, set `codex_working_dir` in
worker config to control only the Codex subprocess cwd without changing the worker service working
directory.

Set `workspace_root` to the persistent directory for work item workspaces. The
worker runs up to two Codex tasks at once by default; `executor_pool_size` sets
that limit. Tasks assigned to the same work item run in arrival order, one at
a time. Run only one worker process against a worker database.

The worker is now the manager for separate supervisor processes, one per slot.
Each supervisor owns its executor and task bookkeeping. The manager reconnects
to their Unix sockets on startup, preserving running tasks and their follow-ups.
Supervisors retain completion status until the manager acknowledges it. Executor
exit and reply checks still determine completion; there is no new executor finish
command in this change.

`supervisor_dir` (also `--supervisor-dir`) defaults to `<absolute db_path>.supervisors`.
It must be a private directory owned by the worker user, writable by the service,
and short enough for Unix sockets. Slot N uses `supervisor-N.sock`, a slot lock,
a config snapshot, and a log in that directory. Use the same database and directory
on every manager restart. The manager discovers existing slots before recovering
abandoned inbox claims, and never reclaims work owned by a live supervisor.

Changing the pool size drains surplus slots. Changes to configuration, environment,
or the installed runtime take effect by replacing idle supervisors; busy supervisors
finish their current task first. A dead or unresponsive supervisor is a failure:
the manager stops rather than replacing it or repeating uncertain work. Its slot
lock remains as evidence. Inspect its log, task state, and any surviving executor
before manually resolving the failed slot. Supervisor crash recovery is deferred.

For a full shutdown, stop the manager first, then drain and stop supervisors:

```sh
python -m app.worker.supervisor --directory /path/to/worker.db.supervisors --shutdown
```

See [host service configuration](../deploy/host_runtime/README.md) for the service
settings required to preserve supervisors across manager restarts. Restarting a
whole container or host also stops supervisors and remains a failure in this phase.

The worker also serves a read-only work item and run UI by default at `http://127.0.0.1:9465/`.
Active sessions stream output through `/api/tasks/<task_id>/events`; completed runs use the same paged feed. Raw activity JSON remains at `/activity.json`. The bind stays fixed at `9465`; use `activity_history_limit` to set the recent run list size per work item.

The default compose stack also runs a simple static preview service on `http://127.0.0.1:9466/`.
It serves a Docker-managed `html-output` volume that is mounted read-write into the worker at
`/workspace/html`. The worker still uses the existing `${LOCAL_WORKSPACE}:/workspace` bind for its
main working tree, and the extra volume just provides a stable place for worker-generated HTML
reports without tying that output to a checkout-specific host path.

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
- Telegram's native reply metadata is preserved. Every Telegram executor run receives recent
  worker-owned turns and a `history_contract` with commands to search older turns by words,
  sender, or date and retrieve nearby turns around a message ID. Search and retrieval stay within
  the chat and topic. The worker ledger starts at deployment time, so older handler-only turns
  and quoted anchors may be absent. Handler history remains stored but is no longer injected.

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
