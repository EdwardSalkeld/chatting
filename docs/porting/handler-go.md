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

## PR Breakdown

Use this section as the task queue for individual Codex iterations. Each item should be small
enough to land as one PR and should leave the repo in a working state.

### 1. Contract Golden Fixtures

Status: complete. Shared fixtures now live under `tests/fixtures/contracts/`, and
both the Python and Go contract tests load and round-trip them.

Add golden JSON fixtures under `tests/fixtures/contracts/` for:
- one `chatting.task.v1`
- one sequenced `chatting.egress.v2` message
- one unsequenced incremental `chatting.egress.v2` message
- one `chatting.auxiliary_ingress.v1`

Validation:
- Python tests load each fixture and parse it with the current Python contract code
- Go tests load each fixture and parse it with `internal/contracts`
- re-serializing should preserve the contract-relevant shape

### 1a. Go Handler CI Test Wiring

Status: complete. The main CI workflow runs `go test ./...` under `go/handler`
using the module Go version from `go/handler/go.mod`.

Validation:
- CI installs Go for pull requests and pushes to `main`
- Go handler unit tests run alongside the existing Python unit suite
- local verification remains `cd go/handler && go test ./...`

### 2. E2E Implementation Selector

Status: complete. E2E tests select the handler implementation with
`CHATTING_E2E_HANDLER_IMPLEMENTATION`; `python` remains the default, and `go`
fails explicitly until the Go runtime is implemented.

Extend the E2E harness so tests can choose a handler implementation:
- `python`
- `go`

The default should remain `python`.

Validation:
- existing Python-handler E2E tests still pass unchanged
- selecting `go` should fail clearly until the Go runtime exists, not silently fall back to Python
- the selector should be documented in the test helpers or E2E docs

### 3. Go BBMB Client

Status: complete. `internal/bbmb` now provides the Go BBMB JSON client for queue
ensure, publish, pickup, and ack, with protocol-level fake-server tests covering
normal operation, empty pickups, malformed payloads, and ack behavior.

Implement `internal/bbmb` with:
- publish JSON
- pickup JSON
- ack
- ensure queue if the BBMB protocol requires it

Validation:
- unit tests use a fake BBMB server or protocol-level test double
- publish/pickup/ack behavior matches the Python `BBMBQueueAdapter` expectations
- malformed payloads and empty pickups are handled explicitly

### 4. Go Config Loader

Status: complete. `internal/config` now loads the minimal handler runtime config
with Python-compatible defaults for supported keys, strict unknown-key rejection,
validation for blank strings and invalid numeric/list values, and `--config`/env
path wiring in the Go handler binary.

Implement `internal/config` for the minimal handler runtime config:
- `bbmb_address`
- `db_path`
- `max_loops`
- `poll_interval_seconds`
- `poll_timeout_seconds`
- `metrics_host`
- `metrics_port`
- `allowed_egress_channels`

Validation:
- unknown keys are rejected
- defaults match Python for supported keys
- invalid types and blank strings fail with useful errors
- the binary can parse `--config`

### 5. SQLite Dedupe And Task Ledger

Status: complete. `internal/state/sqlite` now creates Python-compatible
`idempotency_keys`, `task_ledger`, and `completed_task_ledger` tables, supports
source-scoped dedupe checks, task record round trips, and completion checks that
close the matching task/envelope pair. Tests cover Go round trips and verify
Python `TaskLedgerStore` can read task rows written by Go.

Implement handler state for:
- `idempotency_keys`
- `task_ledger`
- `completed_task_ledger`

Validation:
- Go tests create a temp SQLite DB and exercise dedupe round trips
- task records written by Go can be read by Python tests, or are byte-compatible at the table level
- completion state blocks future egress for the same task/envelope pair

### 6. SQLite Egress State

Status: complete. `internal/state/sqlite` now creates Python-compatible
`dispatched_event_ids`, `staged_egress_events`, and `egress_sequence_state`
tables, supports event-id dispatch dedupe, stages sequenced egress payloads,
fetches events by expected sequence, advances sequence state only when staged
events are marked dispatched, and clears staged/sequence state on completion.

Implement handler state for:
- `dispatched_event_ids`
- `staged_egress_events`
- `egress_sequence_state`

Validation:
- duplicate event IDs are recognized
- staged events can be fetched by expected sequence
- expected sequence advances only when an event is marked dispatched
- completion events can be staged and applied in order

### 7. Go Egress Engine

Status: complete. `internal/egress` now decodes and validates
`chatting.egress.v2` payloads, rejects unknown/completed/disallowed egress,
dedupes dispatched event IDs, dispatches unsequenced incremental events, stages
sequenced events, flushes them in order, and applies sequenced completion events.
Tests cover the decision branches with fake state/dispatchers and exercise
ordered staging plus replay dedupe against SQLite-backed state.

Implement `internal/egress` with a fake dispatcher:
- parse `chatting.egress.v2`
- reject invalid payloads
- reject unknown tasks
- reject completed tasks
- enforce allowed channels
- dispatch unsequenced incrementals immediately
- stage and flush sequenced events in order
- apply completion

Validation:
- unit tests cover each decision branch above
- tests use fake state and fake dispatchers where possible
- SQLite-backed integration tests cover staging and dedupe persistence

### 8. Handler Runtime Skeleton

Wire:
- config loader
- BBMB client
- SQLite state
- egress loop
- signal/shutdown handling

No ingress connectors are required yet except optional heartbeat in the next step.

Validation:
- `chatting-handler --version` works
- `chatting-handler --config <file>` starts and exits cleanly with `max_loops`
- egress queue draining can be exercised with a fake or local BBMB instance

### 9. Internal Heartbeat Connector

Implement the heartbeat task source and heartbeat egress recognition.

Validation:
- Go handler publishes an internal heartbeat task
- Python worker responds to that task
- Go handler accepts the heartbeat log pong even if `log` is not allowlisted
- completion closes the heartbeat task

### 10. Auxiliary Ingress Queue Connector

Implement the handler-side consumer for `chatting.auxiliary_ingress.v1`.

Validation:
- Go handler consumes configured auxiliary ingress queue names
- JSON body is rendered into worker-visible task content like Python
- dedupe and ack behavior match Python
- a Go-handler/Python-worker E2E auxiliary ingress test passes

### 11. Schedule Connector

Implement interval schedule jobs.

Validation:
- schedule file parsing matches Python-supported keys
- unknown schedule keys are rejected
- a scheduled task is published with matching prompt context and context refs
- a Go-handler/Python-worker schedule smoke test passes

### 12. Metrics Endpoint

Implement basic Prometheus-style handler metrics.

Validation:
- `/metrics` serves successfully
- ingress loop counters and egress counters update
- metric names remain stable enough for current docs/tests

### 13. SMTP Dispatch

Implement email dispatch.

Validation:
- fake SMTP sender/unit tests cover subject/body formatting
- reply subject behavior matches Python for email envelopes
- dispatch failure records the same high-level failure outcome as Python

### 14. IMAP Ingress

Implement IMAP polling and email normalization.

Validation:
- fake IMAP tests cover message normalization
- invalid/missing email dates fall back consistently
- Go-handler/Python-worker email E2E passes against the existing fake email setup

### 15. Telegram Dispatch

Implement Telegram text, reaction, and attachment dispatch.

Validation:
- fake HTTP tests cover sendMessage, setMessageReaction, sendPhoto/sendDocument
- parse-mode fallback behavior matches Python
- attachment URI validation matches Python behavior

### 16. Telegram Ingress And Attachments

Implement Telegram getUpdates polling, update normalization, attachment download, conversation memory,
and attachment cleanup state.

Validation:
- fake Telegram update tests cover text, channel posts, reactions, photos, and location payloads
- allowlist behavior matches Python
- conversation memory is included for Telegram tasks
- attachment cleanup tests match Python semantics

### 17. GitHub Checkpoint Store

Implement GitHub checkpoint persistence.

Validation:
- scope keys match Python
- checkpoints round trip through SQLite
- events before or equal to checkpoint are not re-emitted

### 18. GitHub Polling

Implement GitHub issue assignment and pull request review polling.

Validation:
- GraphQL request variables match Python behavior
- repository wildcard expansion works
- event normalization fixtures match Python output
- checkpointed polling emits only new events

### 19. GitHub Dispatch

Implement GitHub comment dispatch.

Validation:
- target parsing accepts the same issue/PR URL and `owner/repo#number` forms as Python
- fake command/client tests cover successful and failed dispatch
- GitHub egress failure handling matches Python at the handler level

### 20. Go Handler Drop-In E2E

Run the full handler-oriented E2E suite with:
- Go handler
- Python worker

Validation:
- all handler scenarios pass through the implementation selector
- Python handler remains the default
- rollback is still just changing the selected handler command

## Initial Open Questions

- Which handler config keys must remain byte-for-byte identical between Python and Go?
- Should Go read the exact same SQLite tables from day one, or can some non-observed internals differ?
- Do we want contract fixtures generated from Python objects, or hand-maintained golden JSON?
- Do we want the Go handler introduced behind a separate service name first, or as a drop-in replacement command later?

## Immediate Next Steps

1. Wire the handler runtime skeleton around config, BBMB, SQLite state, and the egress loop.
2. Add the internal heartbeat connector and heartbeat egress recognition.
3. Add auxiliary ingress queue consumption for the first Go-handler/Python-worker E2E path.
