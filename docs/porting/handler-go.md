# Go Handler Port Plan

## Goal

Port `message-handler` to Go while keeping the Python handler in this repo as the reference
implementation during migration.

The immediate target is:
- Go handler
- Python worker
- same BBMB payload contracts
- same handler-side SQLite semantics where the rest of the system depends on them

## Non-Goals

- Running Python and Go handlers simultaneously against the same live integrations or BBMB queues
- Building a cross-language shared runtime library
- Porting the worker in the same phase as the initial handler port
- Preserving Python module structure in Go

## Operating Model

Python and Go handlers should coexist in the repo, but only one handler implementation should run
for any given environment at a time.

That means compatibility pressure is on:
- BBMB payloads
- SQLite state semantics
- ingress/egress behavior
- externally visible dispatch behavior

It is not on:
- internal implementation details
- concurrency model
- exact package/module boundaries

## Compatibility Surface

The following should be treated as the stable contract that the Go handler must satisfy.

### Message Contracts

- `TaskEnvelope` in [app/models.py](/Users/edward/develop/chatting/app/models.py:191)
- `OutboundMessage` in [app/models.py](/Users/edward/develop/chatting/app/models.py:248)
- `TaskQueueMessage` in [app/broker/messages.py](/Users/edward/develop/chatting/app/broker/messages.py:121)
- `EgressQueueMessage` in [app/broker/messages.py](/Users/edward/develop/chatting/app/broker/messages.py:269)
- `AuxiliaryIngressQueueMessage` in [app/broker/messages.py](/Users/edward/develop/chatting/app/broker/messages.py:408)
- `SCHEMA_VERSION` in [app/models.py](/Users/edward/develop/chatting/app/models.py:10)

### Transport Semantics

Source of truth:
- [docs/bbmb-message-flow.md](/Users/edward/develop/chatting/docs/bbmb-message-flow.md:1)

Important rules:
- `chatting.tasks.v1` carries `chatting.task.v1`
- `chatting.egress.v1` carries `chatting.egress.v2`
- auxiliary ingress queues carry `chatting.auxiliary_ingress.v1`
- task and egress queue names are fixed
- auxiliary ingress queue names are configurable

### Handler State Semantics

Handler-owned state is split across:
- `SQLiteStateStore` in [app/state/sqlite_store.py](/Users/edward/develop/chatting/app/state/sqlite_store.py:24)
- `TaskLedgerStore` in [app/handler/runtime.py](/Users/edward/develop/chatting/app/handler/runtime.py:67)
- `TelegramAttachmentStore` in [app/handler/runtime.py](/Users/edward/develop/chatting/app/handler/runtime.py:350)

Normative behavior:
- ingress dedupe is keyed on `(source, dedupe_key)`
- handler records ingress tasks before accepting worker egress
- dispatched event IDs are persisted to make duplicate egress safe
- completion closes the task and future egress is rejected
- Telegram attachment cleanup eligibility and deletion behavior remain consistent

### Egress Rules

Source of truth in Python:
- [app/handler/main.py](/Users/edward/develop/chatting/app/handler/main.py:1652)
- [app/handler/main.py](/Users/edward/develop/chatting/app/handler/main.py:1808)
- [app/handler/main.py](/Users/edward/develop/chatting/app/handler/main.py:1854)
- [app/handler/main.py](/Users/edward/develop/chatting/app/handler/main.py:2004)

Normative behavior:
- reject egress that is not `chatting.egress.v2`
- reject unknown-task egress
- reject egress for already completed tasks
- reject disallowed channels except explicit internal exceptions
- dispatch unsequenced incrementals immediately
- stage and flush sequenced events in order
- apply completion only after earlier sequenced visible events

### Worker Assumptions The Go Handler Must Preserve

Relevant Python behavior:
- worker task processing: [app/worker/runtime.py](/Users/edward/develop/chatting/app/worker/runtime.py:33)
- worker egress outbox replay: [app/worker/main.py](/Users/edward/develop/chatting/app/worker/main.py:447)

The Go handler must continue to accept:
- Python worker `chatting.egress.v2` messages
- heartbeat message/completion flow
- outbox replayed egress without causing duplicate visible dispatch

## Current Python Handler Boundaries

The main Python concentrations are:
- orchestrator/process wiring: [app/handler/main.py](/Users/edward/develop/chatting/app/handler/main.py:2383)
- connector construction: [app/handler/main.py](/Users/edward/develop/chatting/app/handler/main.py:1050)
- fail-open ingress startup: [app/handler/main.py](/Users/edward/develop/chatting/app/handler/main.py:1427)
- egress engine: [app/handler/main.py](/Users/edward/develop/chatting/app/handler/main.py:1652)
- ingress loop: [app/handler/main.py](/Users/edward/develop/chatting/app/handler/main.py:2158)
- egress loop: [app/handler/main.py](/Users/edward/develop/chatting/app/handler/main.py:2343)

This should not be copied directly into Go. It should be decomposed.

## Proposed Repo Layout

```text
app/
  handler/                      # Python reference implementation

go/
  handler/
    cmd/
      chatting-handler/
        main.go
    internal/
      bbmb/
      config/
      contracts/
      connectors/
        auxiliary/
        github/
        heartbeat/
        imap/
        schedule/
        telegram/
      dispatch/
        github/
        smtp/
        telegram/
      egress/
      ingress/
      state/
        sqlite/
      telemetry/

docs/
  porting/
    handler-go.md

tests/
  e2e/
  fixtures/
    contracts/
```

## Proposed Go Package Responsibilities

### `cmd/chatting-handler`

Owns only:
- process startup
- config load
- dependency construction
- signal handling

### `internal/contracts`

Owns:
- Go structs for canonical payloads
- JSON encode/decode
- validation rules matching Python contracts

This package should be treated as the first-class compatibility layer.

### `internal/bbmb`

Owns:
- queue pickup
- publish
- ack
- transport-specific client concerns

### `internal/config`

Owns:
- handler JSON config parsing
- defaults
- strict key validation
- connector-specific config shaping

### `internal/ingress`

Owns:
- connector interface
- polling runner
- dedupe/apply-to-task-publication flow

### `internal/connectors/...`

Owns source-specific normalization only.

Expected order of implementation:
- `heartbeat`
- `auxiliary`
- `schedule`
- later `imap`
- later `telegram`
- later `github`

### `internal/egress`

Owns:
- egress message parsing
- task existence validation
- channel allowlist checks
- immediate incremental dispatch
- ordered stage/flush behavior
- completion application

This is the most important early Go subsystem after contracts/state.

### `internal/dispatch/...`

Owns outbound side effects only:
- SMTP
- Telegram Bot API
- GitHub issue/pull request comments

Keep this handler-specific. Do not design it as a future shared worker package.

### `internal/state/sqlite`

Owns:
- dedupe keys
- dispatched event IDs
- task ledger
- staged egress
- completion ledger
- Telegram attachment cleanup state

This package should expose Go interfaces that are small and handler-oriented.

### `internal/telemetry`

Owns:
- in-memory rollups
- metrics rendering
- optional `/metrics` HTTP serving

## Shared Code Policy

The safest rule is:

- share specs
- share fixtures
- share black-box tests
- do not share runtime implementation logic across languages

### Good Things To Share

- canonical JSON fixtures
- message flow docs
- SQLite behavior docs
- E2E scenarios
- config examples where compatible

### Bad Things To Share

- SMTP client logic
- Telegram API logic
- GitHub polling logic
- SQLite query code
- concurrency primitives
- retry or staging implementation

If the worker is ported later, shared meaning should still be:
- shared protocol docs
- shared golden fixtures
- shared E2E assertions

not shared runtime code.

## E2E Test Strategy

Use one black-box harness with implementation selection, not separate bespoke suites.

### Implementation Matrix

Initial:
- Python handler + Python worker
- Go handler + Python worker

Later:
- Python handler + Go worker
- Go handler + Go worker

### Isolation Requirements

Each test run should have:
- its own BBMB instance
- its own handler DB
- its own worker DB
- its own ports
- fake or sandboxed external integrations where possible

Do not try to run Python and Go handlers against the same live queues or same integration inputs.

### Core Scenarios

- schedule ingress -> worker -> completion
- auxiliary ingress -> worker -> visible reply path
- unknown-task egress is dropped
- disallowed channel is dropped
- sequenced egress is dispatched in order
- completion closes task and blocks later egress
- duplicate event IDs do not duplicate visible dispatch
- heartbeat roundtrip succeeds
- Telegram attachment cleanup semantics
- IMAP happy path
- GitHub polling happy path

### Assertions

Each scenario should prefer black-box assertions:
- BBMB payload shapes
- final SQLite state
- externally visible side effects
- key metrics or logs when needed

Avoid asserting internal implementation details of the Go handler.

## Migration Plan

### Milestone 0: Freeze Contracts

Before meaningful Go work:
- treat [docs/bbmb-message-flow.md](/Users/edward/develop/chatting/docs/bbmb-message-flow.md:1) as normative
- add golden JSON fixtures for task and egress messages
- list normative SQLite tables/semantics used by handler compatibility

### Milestone 1: Minimal Useful Go Handler

Implement:
- contracts
- BBMB adapter
- SQLite handler state
- strict egress engine
- internal heartbeat connector
- auxiliary ingress connector
- interval schedule connector

Target runtime:
- Go handler + Python worker

This gives a real end-to-end replacement path without blocking on IMAP/Telegram/GitHub.

### Milestone 2: SMTP + IMAP

Implement:
- SMTP dispatch
- IMAP ingress

This covers the lowest-complexity real-world integration pair.

### Milestone 3: Telegram

Implement:
- Telegram ingress
- Telegram dispatch
- attachment tracking and cleanup

Telegram is more stateful and should come after the egress engine is already proven.

### Milestone 4: GitHub

Implement:
- GraphQL polling
- checkpointing
- issue assignment ingress
- pull request review ingress
- GitHub reply dispatch

This is the most logic-dense integration and should come last.

## Rollback Plan

Rollback should stay operationally simple:
- keep Python handler entrypoint intact
- use environment/service config to choose Python or Go handler
- keep test harness able to run either implementation explicitly

Swapping back should not require DB migration reversal or transport changes.

## Initial Open Questions

- Which handler config keys must remain byte-for-byte identical between Python and Go?
- Should Go read the exact same SQLite tables from day one, or can some non-observed internals differ?
- Do we want contract fixtures generated from Python objects, or hand-maintained golden JSON?
- Do we want the Go handler introduced behind a separate service name first, or as a drop-in replacement command later?

## Immediate Next Steps

1. Add contract fixtures for `chatting.task.v1`, `chatting.egress.v2`, and `chatting.auxiliary_ingress.v1`.
2. Add implementation selection to the E2E harness.
3. Scaffold the Go handler tree and define the contract structs first.
4. Build the Go egress engine before tackling the more complex connectors.
