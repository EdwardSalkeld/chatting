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

- task-scoped visible replies use `event_kind="message"`
- terminal task closure uses `event_kind="completion"`
- operator/ad-hoc side-channel egress keeps using unsequenced `event_kind="incremental"`

Completion is internal-only:

- it is ordered with other sequenced task-scoped events
- it is not routed to real-world transports
- `message-handler` applies it by closing the task ledger entry

## Rules

1. A successful task emits exactly one `completion` event.
2. A task may emit zero, one, or many visible `message` events before completion.
3. If a task emits no visible messages, completion alone closes it.
4. `completion` is the last sequenced event for the task.
5. `incremental` remains the out-of-band path for manual/operator nudges and similar immediate
   updates.
6. Conversation memory stores only externally visible assistant text on the real reply channel.
   It never stores completion events or reactions.

## Consequences

Benefits:

- user-visible replies are the primary runtime output, not a side effect of terminal closure
- silent-success tasks stay possible without synthetic drop/final replies
- task closure semantics are clearer in logs, docs, and telemetry

Tradeoffs:

- `worker` and `message-handler` must both understand `completion`
- older terminology around `final`/`incremental` needs cleanup
- `app.main_reply` remains mechanically important, but it is no longer the conceptual model for
  task-scoped reply semantics
