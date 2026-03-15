# 0003: Current Implementation Overview (Split Mode)

## Status
Accepted

## Scope

This document describes the current implementation as of 2026-03-01.
The deployment model is private, single-user, split mode.

## Runtime flow

1. `app.main_message_handler` polls connectors and emits canonical `TaskEnvelope` objects.
2. Message-handler publishes `TaskQueueMessage` payloads to `chatting.tasks.v1` and records them in the ingress ledger.
3. `app.main_worker` consumes tasks, routes them, runs the executor, evaluates policy, and emits `EgressQueueMessage` payloads.
4. Message-handler validates egress against the ingress ledger, dispatches allowed visible messages, and marks tasks complete on internal completion events.
5. `SQLiteStateStore` persists idempotency, run history, audit, dead letters, conversation memory, and worker egress outbox state.

## Entrypoints

- `app.main_message_handler`: ingress + egress dispatch in split mode
- `app.main_worker`: task execution in split mode
- `app.main_reply`: publish visible worker-side incremental egress for acknowledgements and final replies
- `app.cli`: preferred read/query + admin CLI (`--list-*`, replay dead letters, metrics)
- `app.main`: compatibility alias for `app.cli`

## Persistence tables (SQLite)

- `idempotency_keys`
- `run_records`
- `audit_events`
- `dead_letters`
- `conversation_turns`
- `dispatched_events`
- `dispatched_event_ids`
- `egress_outbox`

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
