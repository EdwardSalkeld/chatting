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

- Overview: active executors, queued tasks, recurring schedule count, waiting
  inbox classifications, recent work items and upcoming reminders.
- Work items: search, running/queued filters, run history, model tier and explicit
  finish settings displayed from the worker. These views are read-only.
- Sessions: original request, executor transcript, commands and output, optional
  sent messages, pause/resume, follow and copy. Both Codex exec and app-server
  output formats are supported, including lines split across activity events.
- Schedules: create, edit, delete and inspect revision history. Five-field cron
  and IANA timezones are validated by the existing handler API.
- Reminders: create, reschedule, cancel and inspect history. The platform lists
  the latest revision of each reminder. Older revisions remain in detail history.
  Reminder times use the browser’s timezone and are sent as UTC timestamps.
  Reply metadata and context are preserved when editing. Retries of an unchanged
  reminder submission reuse an idempotency key.
- Personal inbox: recent decisions, category/search filters, confidence, reasons,
  waiting/stuck/rejected counts and message links when the handler supplies them.
  This is read-only. A missing inbox endpoint is shown separately from a failed
  service; enable personal inbox and use a compatible handler to make it available.

Work item lists update every four seconds. Handler views update every ten
seconds. Session activity polls every second while active, draining full pages
without delaying. Pause stops transcript polling; it does not stop execution.
Copy uses the browser clipboard API; browsers that restrict it display a message
so text can be copied manually. Service failures show errors and retain any
already loaded data. They are never presented as healthy zero counts.

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
