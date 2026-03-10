# BBMB Message Flow

This document describes the runtime conversation between `message-handler`, `worker`, and
`bbmb-server` in split mode.

Use it as the source of truth for:
- which BBMB queues exist
- what payloads go over those queues
- which config keys change the flow
- what to keep simple when adding new ingress or egress paths

## Topology

- `message-handler` owns ingress polling, ingress idempotency, strict egress validation, and
  real-world dispatch (`email`, `telegram`, `log`)
- `worker` owns routing, executor calls, policy evaluation, retries, dead-lettering, and egress
  publication
- `bbmb-server` is only the transport layer between them

## Queues

There are currently two BBMB queues:

| Queue | Producer | Consumer | Purpose |
| --- | --- | --- | --- |
| `chatting.tasks.v1` | `message-handler` | `worker` | Carries normalized ingress tasks as `chatting.task.v1` payloads |
| `chatting.egress.v1` | `worker` | `message-handler` | Carries `chatting.egress.v2` payloads for ordered dispatch and immediate ad-hoc incrementals |

Important details:
- Queue names are hardcoded today.
- There is no separate BBMB retry queue, dead-letter queue, or approval queue.
- Retries, dead letters, ingress dedupe state, the task ledger, and the worker egress outbox all
  live in SQLite, not in BBMB.
- The current worker emits `chatting.egress.v2` payloads on `chatting.egress.v1`. The queue name is
  transport-level; the `message_type` inside the JSON is the payload contract version.
- `message-handler` enforces this contract and rejects legacy `chatting.egress.v1` payload types.

## End-To-End Flow

### 1. Ingress enters message-handler

A connector returns a normalized `TaskEnvelope`.

Examples:
- IMAP email
- Telegram update
- interval schedule trigger
- GitHub issue assignment
- internal heartbeat ping

`message-handler` then:
- skips the envelope if `(source, dedupe_key)` has already been seen
- enriches Telegram ingress with recent conversation memory when applicable
- wraps the envelope in a `TaskQueueMessage`
- publishes that task message to `chatting.tasks.v1`
- records the task in the message-handler ledger so future egress can be matched back to a known
  ingress task

### 2. Worker consumes the task

`worker` picks one payload from `chatting.tasks.v1`, parses `chatting.task.v1`, routes it, executes
it, runs policy, persists run and audit records, and always builds at least one terminal
`event_kind="final"` egress message.

Current worker behavior:
- normal replies are emitted as `chatting.egress.v2`
- incremental replies use `event_kind="incremental"` and increasing `sequence`
- final replies use `event_kind="final"`
- worker-side `app.main_reply` can also publish unsequenced `event_kind="incremental"` messages for
  immediate operator-visible updates
- if execution/policy yields no final reply content, the worker emits a terminal
  `channel="drop", target="task"` final message as a completion marker
- heartbeat tasks skip the normal executor path and emit a single log pong

Before publishing each egress message, the worker writes it to the SQLite egress outbox. That lets
the worker replay unpublished or unacked egress after restart.

The task queue message is only acked after processing completes and egress has been published to the
outbox/BBMB path.

### 3. Message-handler consumes egress

`message-handler` picks one payload from `chatting.egress.v1` and then:
- validates `message_type == "chatting.egress.v2"`
- validates that the task exists in the ingress ledger
- drops unknown-task egress
- drops egress for tasks that are already completed
- drops disallowed egress channels, except for the internal heartbeat log pong and terminal
  `drop/task` completion markers
- dispatches unsequenced incrementals immediately
- stages sequenced events by `event_id` and `sequence`
- dispatches staged events in order
- records dispatched event ids so duplicate egress is ignored safely
- marks the task complete after dispatching a final event, then rejects future egress for that task

This is the strict boundary in split mode: the worker can suggest output, but only the
message-handler can dispatch it to external systems.

## Worked Example

### Example task on `chatting.tasks.v1`

An IMAP message from `alice@example.com` turns into a `chatting.task.v1` payload like this:

```json
{
  "schema_version": "1.0",
  "message_type": "chatting.task.v1",
  "trace_id": "trace:email:101",
  "task_id": "task:email:101",
  "emitted_at": "2026-03-09T10:00:02Z",
  "envelope": {
    "schema_version": "1.0",
    "id": "email:101",
    "source": "email",
    "received_at": "2026-03-09T10:00:00Z",
    "actor": "alice@example.com",
    "content": "Subject: Please summarize\n\nSummarize the overnight logs.",
    "attachments": [],
    "context_refs": ["repo:/opt/chatting"],
    "policy_profile": "default",
    "reply_channel": {
      "type": "email",
      "target": "alice@example.com"
    },
    "dedupe_key": "email:101"
  }
}
```

### Example reply on `chatting.egress.v1`

If the worker decides to send one final email reply, it publishes a `chatting.egress.v2` payload
like this:

```json
{
  "schema_version": "1.0",
  "message_type": "chatting.egress.v2",
  "task_id": "task:email:101",
  "envelope_id": "email:101",
  "trace_id": "trace:email:101",
  "event_id": "evt:task:email:101:0:final",
  "sequence": 0,
  "event_kind": "final",
  "emitted_at": "2026-03-09T10:00:04Z",
  "message": {
    "channel": "email",
    "target": "alice@example.com",
    "body": "Overnight summary: no failures, 2 warnings, deploy finished cleanly."
  }
}
```

Then `message-handler`:
- verifies `task:email:101` is in the ledger
- verifies `email` is allowed by `allowed_egress_channels`
- dispatches through the configured SMTP sender
- records the event id as dispatched so replay or duplicate delivery is harmless

## Internal Heartbeat

Each message-handler loop also emits one internal heartbeat task by default.

Flow:
- `message-handler` creates an internal `TaskEnvelope`
- it publishes that envelope to `chatting.tasks.v1`
- `worker` turns it into a single log-based pong on `chatting.egress.v1`
- `message-handler` accepts and logs that pong even if `log` is not otherwise allowlisted

This gives a built-in round-trip signal for the handler -> BBMB -> worker -> BBMB -> handler path.

## Config That Changes The Flow

### Shared transport config

- `bbmb_address`
  Must match on `message-handler` and `worker`.

### Message-handler ingress config

- `poll_interval_seconds`
  Controls how often connectors are polled.
- `max_loops`
  Useful for smoke runs; not a normal production setting.
- `schedule_file`
  Enables interval schedule ingress.
- `imap_host`, `imap_port`, `imap_username`, `imap_password_env`, `imap_mailbox`, `imap_search`
  Enable IMAP ingress.
- `telegram_enabled`, `telegram_bot_token_env`, `telegram_api_base_url`,
  `telegram_poll_timeout_seconds`, `telegram_allowed_chat_ids`, `telegram_allowed_channel_ids`,
  `telegram_context_refs`
  Enable and scope Telegram ingress.
- `context_ref` / `context_refs`
  Default context refs attached to some ingress sources.
- `github_repositories`, `github_assignee_login`, `github_context_refs`,
  `github_policy_profile`, `github_max_issues`, `github_max_timeline_events`
  Enable and shape GitHub assignment ingress.

### Message-handler egress and observability config

- `poll_timeout_seconds`
  How long the message-handler waits on `chatting.egress.v1` for one egress payload each loop.
- `allowed_egress_channels`
  Main control for what the worker is allowed to cause externally. Internal heartbeat pong on
  `log/heartbeat` is the only built-in exception.
- `smtp_host`, `smtp_port`, `smtp_username`, `smtp_password_env`, `smtp_from`, `smtp_starttls`
  Required for real email dispatch.
- `telegram_bot_token_env`, `telegram_api_base_url`
  Also affect real Telegram dispatch.
- `metrics_host`, `metrics_port`
  Control the Prometheus-style `/metrics` endpoint on the message-handler.

### Worker execution config

- `poll_timeout_seconds`
  How long the worker waits on `chatting.tasks.v1` before treating the queue as empty.
- `sleep_seconds`
  Delay between empty polls.
- `max_loops`
  Useful for smoke runs; not a normal production setting.
- `max_attempts`
  Retry budget before the task becomes a dead letter.
- `use_stub_executor`
  Replaces the real executor path for testing/smoke runs.
- `codex_command`, `codex_working_dir`
  Shape the real executor subprocess.

## Recommendations

- Keep BBMB queue count small. Add a new queue only when delivery semantics are materially
  different, not just because a new connector exists.
- Keep all integration secrets and real-world dispatch in `message-handler`; keep `worker` focused
  on execution and policy.
- Prefer one canonical `TaskEnvelope` shape per source and normalize aggressively at the edge.
- Treat `allowed_egress_channels` as the policy boundary for external side effects.
- Prefer `chatting.egress.v2` for any new worker output. It supports ordering and dedupe explicitly.
- Use SQLite state, not more BBMB queues, for retries, outbox replay, checkpoints, and dedupe.
- Keep internal operational traffic, like heartbeat pong, on `log` or another non-user-facing
  channel unless there is a strong reason to expose it externally.
