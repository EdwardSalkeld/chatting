# Platform interface

The platform is Chatting’s private, single-operator interface. It combines
handler and worker views without replacing their existing UIs. It has no
application authentication: the installation must gate access at the network
level. The listener defaults to loopback.

## Runtime and build

`chatting-platform` is a separate Go process. Its React/TypeScript frontend is
built with Vite, Tailwind CSS and shadcn/ui, then embedded in the binary. Node is
only needed at build time. The platform has no database, integration credentials,
or executor of its own. It connects to the handler’s metrics/admin listener and
the worker’s activity listener; it does not use the egress or webhook listeners.

Build from the repository root with Node 24 and the Go version in
`go/handler/go.mod`:

```sh
npm --prefix platform ci
npm --prefix platform run build
(cd go/handler && go build -o /tmp/chatting-platform ./cmd/chatting-platform)
/tmp/chatting-platform --config /path/to/platform.json
```

Build the frontend **before** compiling Go; its output is embedded at compile
time. Generated assets are ignored by Git. Go tests can run without a frontend
build; starting an unbuilt platform binary gives an explicit build instruction.
The compiled binary needs no source checkout, static directory or Node runtime.

The Nix flake also exports `chatting-platform`, including its frontend. See
[packaging and releases](packaging-and-releases.md).

## Configuration

All fields are optional. An omitted `--config` uses these defaults:

```json
{
  "host": "127.0.0.1",
  "port": 9466,
  "handler_url": "http://127.0.0.1:9464",
  "worker_url": "http://127.0.0.1:9465"
}
```

Upstream URLs are HTTP(S) origins with no credentials, paths or query strings.
Unknown config keys are rejected. Each installation owns its bind address,
network gate, service configuration and rollout procedure in its deployment
repository. The app does not assume that either upstream is on the same host.

## Views and actions

The interface is a dense operator console with light and dark themes (the top
bar toggles system, light and dark). Lists are 32px-row tables with sticky
headers; the window is the only vertical scroller. Every filter, tab, search,
sort, open sheet and transcript option is a URL query parameter, so reloads,
back/forward and shared links restore the view.

- Overview (`/`): stat tiles (running, queued, errors in 24h, dead letters,
  waiting inbox mail, handler loop age), a one-line system strip, a "needs
  attention" list (dead letters, failed runs, overdue reminders, stuck inbox
  mail, old outbox rows, a stale handler loop), running and queued work, upcoming
  schedule and reminder fires, recent runs and inbox categories.
- Work items (`/items`, `/items/:id`): status filters, search, last result,
  failures and 7-day tokens per item. Item detail shows reply facts, a run
  summary (failure rate, p50/p95 latency, tokens) and run history. Read-only.
- Runs (`/runs`): every non-internal run with status, source, time-range and
  item filters. Running executors are pinned first. Dead-letter rows expand to
  show the error, reason codes, attempts and replay.
- Sessions (`/tasks/:id`, `/runs/:id`): run facts (status, attempts, backend,
  model and reroutes, tokens, context use), errors, and the transcript of
  messages, paired commands with exit codes, system events and deliveries. Kind
  filters (`?show=`), find (`?find=`), error navigation, pause/resume, follow
  and copy. Codex exec and app-server output formats are both supported.
- Schedules (`/schedules`): next and last run, cron in plain English, filters
  and a side sheet (`?edit=`, `?new=1`) to create, edit or delete with live
  cron/timezone validation, the next five fires, recent fires and revisions.
- Reminders (`/reminders`): Upcoming and History tabs; the sheet creates,
  reschedules or cancels. Times use the browser's timezone and are sent as UTC.
  Reply metadata and context are preserved, and retries of an unchanged
  submission reuse an idempotency key.
- Personal inbox (`/inbox`): mode, threshold, waiting/stuck/dead-letter facts,
  category filters and a detail sheet (`?row=`). Read-only. A missing inbox
  endpoint is shown as "not configured", separately from a failed service.
- System (`/system`): handler and worker `/metrics` as grouped key/values with
  per-minute rates, queue and outbox state, live executors and the raw text
  (`?show=raw`).

The sidebar counts running executors, failed runs in the last 24h, overdue
reminders and waiting inbox mail. The tab title is prefixed with `● ` while an
executor is live and `(N) ` for failed runs, overdue reminders and stuck mail.

Keyboard shortcuts (`?` lists them; ignored while typing):

| Key | Action |
| --- | --- |
| `⌘K` / `Ctrl+K` | Command palette |
| `/` | Focus the page search |
| `j` / `k`, `↓` / `↑` | Move between table rows |
| `Enter` / `o` | Open the row (`⌘`/`Ctrl` for a new tab) |
| `→` / `←` / `Space` | Expand or collapse a dead-letter row |
| `g` then `o i r s m n y` | Overview, Items, Runs, Schedules, Reminders, Inbox, System |
| `[` | Collapse the sidebar |
| `r` | Refetch the page |
| `f` / `p` / `e` / `c` | Session: follow, pause, next error, copy |
| `Esc` | Clear search, close a sheet or stop following |

Work items and activity update every four seconds, runs every five, handler
lists and metrics every ten. Session activity polls every second while active,
draining full pages without delaying. Pause stops transcript polling; it does
not stop execution. Copy uses the browser clipboard API; browsers that restrict
it display a message so text can be copied manually. Service failures show a
strip with the time of the last good data and keep that data visible; counts
show "—", never a healthy zero.

## HTTP interfaces

The browser uses same-origin APIs. The platform forwards only these routes:

| Platform route | Upstream route | Methods |
| --- | --- | --- |
| `/api/worker/items` | Worker `/api/items` | GET |
| `/api/worker/items/{id}/runs` | Worker `/api/items/{id}/runs` | GET |
| `/api/worker/runs?limit=&status=&source=&work_item_id=&since=` | Worker `/api/runs` | GET |
| `/api/worker/runs/{id}` | Worker `/api/runs/{id}` | GET |
| `/api/worker/dead-letters?limit=` | Worker `/api/dead-letters` | GET |
| `/api/worker/tasks/{id}/events?after=N` | Worker `/api/tasks/{id}/events?after=N` | GET |
| `/api/worker/activity` | Worker `/activity.json` | GET |
| `/api/worker/metrics` | Worker `/metrics` (Prometheus text) | GET |
| `/api/handler/schedules` | Handler `/api/schedules` | GET, POST |
| `/api/handler/schedules/{id}` | Handler `/api/schedules/{id}` | GET, PUT, DELETE |
| `/api/handler/reminders` | Handler `/api/reminders` | GET, POST |
| `/api/handler/reminders/{id}` | Handler `/api/reminders/{id}` | GET, PUT, DELETE |
| `/api/handler/inbox` | Handler `/api/personal-inbox/decisions` | GET |
| `/api/handler/metrics` | Handler `/metrics` (Prometheus text) | GET |

Query strings, JSON bodies and upstream status codes are preserved. Browser
cookies and authorization headers are not forwarded. Cross-origin browser
writes are rejected; this is a request safeguard, not application authentication.
Writes are limited to 1 MiB. Upstream header waits are limited to five seconds
and requests to ten seconds. Connection failures return JSON with status 502.
No-store headers cover the private interface and its APIs.

Both metrics routes come from listeners the platform already uses: the handler
serves `/metrics` on its metrics/admin listener and the worker on its activity
listener. The frontend parses the Prometheus text itself.

Worker `GET /api/runs` lists runs newest first as `runs`, with `limit` (default
200, at most 1000) and a `truncated` flag. Filters are exact matches on
`status` (`result_status`), `source` and `work_item_id`; `since` is an RFC 3339
timestamp compared with the run's finish time. Internal runs are left out unless
`source=internal`. Each row has `run_id`, `created_at` (finish time),
`result_status`, `latency_ms`, `source`, `task_id`, `work_item_id`, `preview`,
`executed_model_tier`, and `model` and `tokens` (input, cached input, output,
reasoning and total) when the executor reported usage, otherwise `null`.

Worker `GET /api/dead-letters` returns `dead_letters`, newest first, without the
envelope body: `dead_letter_id`, `run_id`, `task_id`, `work_item_id`,
`envelope_id`, `source`, `received_at`, `reply_channel_type`,
`reply_channel_target`, `reason_codes`, `last_error`, `attempt_count`, `status`,
`replayed_run_id` and `created_at`. It takes the same `limit` as runs.

`GET /api/runs/{id}` reports `started_at` (finish time minus duration) and
`finished_at`, plus the executor detail recorded in the run's audit event:
`executor_backend`, `model_tier` (configured), `executed_model_tier`,
`auto_model_decision`, `escalation_reason`, `last_error`, `last_error_stage`,
`execution_errors`, `egress_message_count`, `executor_launch_count`,
`max_attempts` and `supervised_recovery_used`. Older runs return `null` for
fields their audit event lacks. `/activity.json` adds `outbox`: the number of
egress events still `pending_publish` and the oldest one's `created_at`.

The handler now supports `GET /api/reminders?status=all&latest=1`. `latest=1`
selects the highest revision **before** applying the status filter. Without it,
the existing API continues to return every matching revision. Detail
`?history=1` retains its complete history.

The handler’s new `GET /api/personal-inbox/decisions` lives on the same private
metrics/admin listener as the existing inbox page. It returns that page’s last
200 messages as `rows`, with `sender`, `subject`, `received_at` (RFC3339),
`decision`, `score`, `why`, `link` and `unclassified`. Summary fields are `dry_run`,
`threshold`, `counts`, `awaiting`, `stuck`, `oldest_wait`, `dead_letters` and
`last_error`. The old HTML route is unchanged.

## Development and verification

With the platform gateway running on port 9466:

```sh
npm --prefix platform run dev
```

Vite provides hot reload on its printed local URL and proxies `/api` to the
platform. Use `CHATTING_PLATFORM_URL` to override the development proxy target.
Production uses the embedded build and serves deep links directly.

```sh
npm --prefix platform run format:check
npm --prefix platform run test
npm --prefix platform run build
(cd go/handler && go test ./... && go vet ./...)
```

For browser QA without real integrations, see [testing](testing.md). Fixtures use
real handler routes and the real worker activity monitor with disposable SQLite
state. They start no connector, broker, executor or outbound delivery.
