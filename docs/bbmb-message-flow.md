# BBMB Message Flow

This document describes the runtime conversation between `message-handler`, `worker`, and
`bbmb-server`.

Use it as the source of truth for:
- which BBMB queues exist
- what payloads go over those queues
- which config keys change the flow
- what to keep simple when adding new ingress or egress paths

## Topology

- `message-handler` owns ingress polling, ingress idempotency, work item routing, strict egress
  validation, and real-world dispatch (`email`, `telegram`, `telegram_reaction`, `github`, `log`)
- `worker` owns executor calls, retries, dead-lettering, and egress publication
- `bbmb-server` is only the transport layer between them

## Queues

Egress does not use BBMB — see [Egress transport](#egress-transport).

| Queue | Producer | Consumer | Purpose |
| --- | --- | --- | --- |
| `chatting.tasks.v1` | `message-handler` | `worker` | Carries normalized ingress tasks as `chatting.task.v1` payloads |
| `chatting.personal-inbox.tasks.v1` | `message-handler` | `worker` | Personal mail classification requests |
| `chatting.personal-inbox.results.v1` | `worker` | `message-handler` | Classification results |
| `chatting.personal-inbox.invalid.v1` | both | nobody (inspect by hand) | Invalid personal inbox tasks and results |

The personal inbox queues are described in [Personal inbox triage](connectors/personal-inbox.md).
Personal mail never enters `chatting.tasks.v1`.

Important details:
- Queue names are hardcoded.
- There is no separate BBMB retry queue, dead-letter queue, or approval queue.
- Retries, dead letters, ingress dedupe state, the task ledger, and the worker egress outbox all
  live in SQLite, not in BBMB.

### Egress transport

Egress does not flow over BBMB. Every egress message — the executor's visible replies via
`app.main_reply` and the worker's own `completion`/sequenced events — is POSTed to the handler's
synchronous egress endpoint (`handler_egress_url`, default `http://127.0.0.1:9467/egress`), and the
delivery outcome is returned in the HTTP status. The `chatting.egress.v2` payload contract is
unchanged; only the transport moved from a queue to a request/response call so the sender learns
whether the send landed. The endpoint always binds loopback, independent of the metrics bind,
since it can send messages to the outside world.

## End-To-End Flow

### 1. Ingress enters message-handler

A connector returns a normalized `TaskEnvelope`.

Examples:
- IMAP email
- Telegram update
- schedule or reminder trigger
- GitHub issue assignment or pull request review
- internal heartbeat ping

`message-handler` then:
- skips the envelope if `(source, dedupe_key)` has already been seen
- assigns the task to a work item (one per Telegram chat and topic, a registered pull request's
  work item for GitHub events, otherwise a general work item)
- wraps the envelope in a `TaskQueueMessage`
- publishes that task message to `chatting.tasks.v1`
- records the task in the message-handler ledger so future egress can be matched back to a known
  ingress task

### 2. Worker consumes the task

`worker` picks one payload from `chatting.tasks.v1`, parses `chatting.task.v1`, executes it,
persists run and audit records, and always emits exactly one terminal
`event_kind="completion"` egress message.

Worker behavior:
- the executor returns only completion metadata, actions, config updates, and errors
- the executor must publish any visible reply itself with
  `python3 -P -m app.main_reply --spec-file <path>`, writing the reply as a JSON file so the body
  never passes through the shell
- terminal task closure uses `event_kind="completion"` and is internal-only
- worker-side `app.main_reply` submits unsequenced `event_kind="incremental"` messages for both
  intermediate acknowledgements and final user-visible replies
- the worker emits only the completion event for normal task processing
- heartbeat tasks skip the normal executor path and emit a log pong followed by completion

Egress does not use BBMB at all. Every egress message — the executor's visible replies via
`app.main_reply`, and the worker's own `completion`/sequenced events — is POSTed to the handler's
synchronous egress endpoint (`handler_egress_url`, default `http://127.0.0.1:9467/egress`) and the
delivery outcome comes straight back: HTTP 200 for a dispatched/completed message, 202 for a
sequenced event staged pending an earlier one, 422 when the handler drops it (bad payload, unknown
task, disallowed channel, dispatch failure such as a missing attachment), 503 when the handler is
unreachable. `app.main_reply` maps this to an exit code (0 delivered, 1 dropped, 3 transient, 4
same-conversation follow-ups claimed and the drafted content withheld) so the
executor learns immediately whether a reply landed instead of publishing fire-and-forget.

The worker still writes each of its own egress events to the SQLite egress outbox first, as a
write-ahead log: on a 2xx the row is acked; a 422 is logged loudly and acked (a permanent drop is an
error, not something to retry forever); anything else leaves the row pending and it is replayed on a
later loop or after restart. Crucially the task-queue message is acked once processing completes
regardless of egress delivery — a transient egress failure never re-runs the executor, it just
defers the reply to the outbox replay.

### 3. Message-handler dispatches egress

The handler receives each egress POST on its egress endpoint and then:
- validates `message_type == "chatting.egress.v2"`
- validates that the task exists in the ingress ledger
- drops unknown-task egress
- drops egress for tasks that are already completed
- drops disallowed egress channels, except for the internal heartbeat log pong and internal
  completion events
- dispatches unsequenced incrementals immediately
- stages sequenced events by `event_id` and `sequence`
- dispatches staged events in order
- records dispatched event ids so duplicate egress is ignored safely
- applies completion after all earlier sequenced visible messages and then rejects future egress for
  that task

This is the strict boundary: the worker can suggest output, but only the
message-handler can dispatch it to external systems or close the task internally.

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
    "prompt_context": {
      "global_instructions": ["Keep replies brief and practical."],
      "source_instructions": [],
      "reply_channel_instructions": ["Use concise professional email tone and include a useful subject line."],
      "task_instructions": [],
      "assembled_instructions": [
        "Keep replies brief and practical.",
        "Use concise professional email tone and include a useful subject line."
      ]
    },
    "reply_channel": {
      "type": "email",
      "target": "alice@example.com"
    },
    "dedupe_key": "email:101"
  }
}
```

### Example reply

If the worker decides to send one email reply, it POSTs a `chatting.egress.v2` payload
to the egress endpoint like this:

```json
{
  "schema_version": "1.0",
  "message_type": "chatting.egress.v2",
  "task_id": "task:email:101",
  "envelope_id": "email:101",
  "trace_id": "trace:email:101",
  "event_id": "evt:task:email:101:0:message",
  "sequence": 0,
  "event_kind": "message",
  "emitted_at": "2026-03-09T10:00:04Z",
  "message": {
    "channel": "email",
    "target": "alice@example.com",
    "body": "Overnight summary: no failures, 2 warnings, deploy finished cleanly."
  }
}
```

The worker then emits a second sequenced payload to close the task:

```json
{
  "schema_version": "1.0",
  "message_type": "chatting.egress.v2",
  "task_id": "task:email:101",
  "envelope_id": "email:101",
  "trace_id": "trace:email:101",
  "event_id": "evt:task:email:101:1:completion",
  "sequence": 1,
  "event_kind": "completion",
  "emitted_at": "2026-03-09T10:00:04Z",
  "message": {
    "channel": "internal",
    "target": "task",
    "body": "Worker completed; task closure is internal-only."
  }
}
```

Then `message-handler`:
- verifies `task:email:101` is in the ledger
- verifies `email` is allowed by `allowed_egress_channels`
- dispatches through the configured SMTP sender
- records both event ids as dispatched so replay or duplicate delivery is harmless
- marks the task complete only when the completion event is applied

## Internal Heartbeat

Each message-handler loop also emits one internal heartbeat task.

Flow:
- `message-handler` creates an internal `TaskEnvelope`
- it publishes that envelope to `chatting.tasks.v1`
- `worker` turns it into a log-based pong POSTed to the egress endpoint, then emits completion
- `message-handler` accepts and logs that pong even if `log` is not otherwise allowlisted
- `message-handler` closes the heartbeat task when the completion event arrives

This gives a built-in round-trip signal for the handler -> BBMB -> worker -> egress endpoint ->
handler path.

## Config That Changes The Flow

The deployed values are in lab's `nixos/hosts/magpie/chatting-config.nix`.

### Shared

- `bbmb_address`: must match on `message-handler` and `worker`.

### Message-handler

- `poll_interval_seconds`: how often connectors are polled.
- `allowed_egress_channels`: the main control over what the worker can cause externally. The
  heartbeat pong on `log` and internal completion events are the only exceptions.
- `egress_http_port`: the loopback egress endpoint's port (default `9467`).
- `egress_attachment_allowed_dirs`: the only directories an outbound attachment may come from.
- `metrics_host`, `metrics_port`: the metrics/admin listener.
- `context_refs`: default context refs for ingress tasks.
- Connector settings are listed on each [connector page](connectors/README.md).

Prompt guidance is inspectable in the task payload under `envelope.prompt_context`. Schedules and
reminders carry their own `prompt_context`; reminders also add a fixed source instruction.

### Worker

- `poll_timeout_seconds`: how long the worker waits on `chatting.tasks.v1` before treating the
  queue as empty.
- `sleep_seconds`: delay between empty polls.
- `max_attempts`: retry budget before the task becomes a dead letter.
- `codex_command`, `codex_working_dir`: shape the executor subprocess.
- `workspace_root`, `executor_pool_size`: see [Operations](operations.md#worker).
- `handler_egress_url`: the handler's egress endpoint.

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
