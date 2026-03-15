# 0004: Separate Visible Egress From Task Completion

## Status
Accepted

## Context

In split mode, the old egress contract used terminal `final` events for two different jobs:

- deliver the user-visible reply
- mark the task complete so later egress is rejected

That coupling made it awkward to send the real answer before run end without also emitting a second
synthetic closing reply. It also made out-of-band worker/operator replies feel secondary even when
they were the messages the user actually saw.

The deployment is private and single-owner. We control both `worker` and `message-handler`, so we
can change this protocol coherently without carrying a long mixed-version compatibility burden.

## Decision

Split the semantics explicitly:

- task-scoped visible replies are published by the executor itself via `app.main_reply`
- terminal task closure uses `event_kind="completion"`
- visible executor-published replies use unsequenced `event_kind="incremental"`

Completion is internal-only:

- it is ordered only as an internal terminal event
- it is not routed to real-world transports
- `message-handler` applies it by closing the task ledger entry

## Rules

1. A successful task emits exactly one `completion` event.
2. The executor must not return visible replies in its structured JSON output.
3. Any visible reply must be sent by the executor itself, typically with `python3 -m app.main_reply`.
4. If a task emits no visible replies, completion alone closes it.
5. `incremental` is the visible reply path for both intermediate and final executor-published text.
6. Conversation memory stores only externally visible assistant text on the real reply channel.
   It never stores completion events or reactions.

## Consequences

Benefits:

- user-visible replies are emitted directly by the executor, not inferred from its return payload
- silent-success tasks stay possible without synthetic drop/final replies
- task closure semantics are clearer in logs, docs, and telemetry

Tradeoffs:

- `worker` and `message-handler` must both understand `completion`
- executor prompts must be explicit that visible replies belong in `app.main_reply`, not stdout JSON
- older terminology around `final`/`incremental` needs cleanup
