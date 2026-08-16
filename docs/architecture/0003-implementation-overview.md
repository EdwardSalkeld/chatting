# 0003: Current Implementation Overview (Split Mode)

## Status
Accepted

## Scope

This document describes the current implementation as of 2026-03-01.
The deployment model is private, single-user, split mode.

## Runtime flow

1. The Go `message-handler` polls connectors and emits canonical `TaskEnvelope` objects.
2. Message-handler publishes `TaskQueueMessage` payloads to `chatting.tasks.v1` and records them in the ingress ledger.
3. `app.main_worker` persists tasks into its durable inbox before acknowledging BBMB. While an
   executor runs, a collector continues draining ingress so same-conversation follow-ups are
   available without another executor launch.
4. Before Telegram egress, `app.main_reply` atomically claims newer pending turns with the same
   opaque conversation ID. It returns them to the executor and withholds stale text/attachments;
   a subsequent current reply completes the incorporated tasks together.
5. Message-handler validates egress against the ingress ledger, dispatches allowed visible messages, and marks tasks complete on internal completion events.
6. `SQLiteStateStore` persists idempotency, run history, audit, dead letters, conversation memory, worker inbox, conversation routes, and worker egress outbox state.

## Entrypoints

- `go/handler/cmd/chatting-handler`: ingress + egress dispatch in split mode
- `app.main_worker`: task execution in split mode
- `app.main_reply`: submit visible worker-side incremental egress (POST to the handler egress endpoint) for acknowledgements and final replies

## Persistence tables (SQLite)

- `idempotency_keys`
- `run_records`
- `audit_events`
- `dead_letters`
- `conversation_turns`
- `dispatched_events`
- `dispatched_event_ids`
- `egress_outbox`
- `conversation_routes`
- `worker_inbox`

## Safety controls implemented

- strict schema validation and required-field enforcement
- source-scoped idempotency (`source + dedupe_key`)
- bounded retries with dead-letter terminal state
- deny-by-default action policy
- full run/audit event persistence with trace metadata

## Non-goals

- multi-tenant operation
- replacing BBMB with multiple transport layers

The implementation remains intentionally private and single-user; the split deployment can run on one
host or a small number of cooperating hosts.
