# 0005: Synchronous Egress Transport (No BBMB)

## Status
Accepted

## Context

Egress used to be published to a BBMB queue (`chatting.egress.v1`) and drained later by the
`message-handler` in its run loop. That made egress fire-and-forget from the sender's point of view:
`app.main_reply` (and the worker's own completion events) got a queue GUID back and reported success
as soon as the message was enqueued — the actual send, and any failure, happened later in a different
process with no path back to the caller.

This hid a real failure: a Telegram reply with an attachment was dropped by the handler
(`telegram_attachment_missing`) while the executor reported success, so the user silently got nothing.
The earlier supervised-reply-recovery heuristic only checked whether a reply was *published*, not
whether it was *delivered*, so it could not catch this.

Ingress is stream-shaped and benefits from a queue. Egress is request/response-shaped: the sender
wants the delivery outcome. The handler must remain the sender (it holds the channel secrets; the
worker runs untrusted executor code), so only the transport worker→handler needs to change.

## Decision

Egress does not use BBMB. Both `app.main_reply` and the worker POST each `chatting.egress.v2`
payload to a synchronous handler endpoint and act on the result:

- Endpoint: `POST {handler_egress_url}` (default `http://127.0.0.1:9467/egress`), served by a
  dedicated loopback HTTP server (`egress_http_host`/`egress_http_port`). It is deliberately NOT on
  the metrics mux, which may bind `0.0.0.0` for scraping — the ability to send messages externally
  must stay reachable only from the local host, matching BBMB's old posture.
- The endpoint runs the same `egress.Engine` path the BBMB drain used (`HandleRaw → Result`), so
  validation, dedup, channel allowlisting, staging/flush, and loud drops are unchanged.
- HTTP status maps the engine `Result`: `200` dispatched/completed/deduped, `202` staged (a
  sequenced event waiting on an earlier one), `422` dropped (with reason code), `503` on an
  infrastructure/state error.
- `app.main_reply` maps this to an exit code so the executor can react: **0 delivered, 1 dropped
  (permanent — adjust and resend, e.g. without the attachment), 3 transient (retry)**.
- The worker's egress outbox is the durability layer, a write-ahead log: persist → POST →
  `2xx` acks the row; `422` is logged loudly (`worker_egress_dropped_by_handler`) and acked so a
  permanent drop cannot replay forever; unreachable/`5xx` leaves the row pending and it is retried on
  a later loop or after restart.
- The task-queue message is acked once processing completes **regardless of egress delivery**, so a
  transient egress failure defers the reply to the outbox replay rather than re-running the executor.

## Rules

1. No egress path publishes to BBMB. `handler_egress_url` is the single egress transport.
2. A dropped send (`422`) is an error: it is logged loudly and, for worker-originated events, acked in
   the outbox (not retried); `app.main_reply` exits non-zero so the executor falls back.
3. Only a transient failure (handler unreachable / `5xx`) is retried, via outbox replay.
4. Outbound attachment paths are confined to `egress_attachment_allowed_dirs` (fail-closed): the path
   is caller-controlled, so an unlisted path is refused before any file read.
5. The endpoint binds loopback by default; do not move it onto a `0.0.0.0` mux.

## Consequences

Benefits:

- The sender learns the real delivery outcome; a drop is surfaced immediately instead of silently.
- No egress queue to buffer, drain, or reason about; the outbox is the only durability layer.
- Ordering of a task's egress is naturally preserved by sequential synchronous calls.

Tradeoffs / notes:

- Egress now requires the handler to be up. In production all services run under one host so this
  holds; the outbox covers transient handler downtime for worker-originated events.
- The handler now writes SQLite from two concurrent places (the ingress loop and the egress
  endpoint goroutine) where the drain used to run inside the single loop. The handler DB is opened
  with `busy_timeout=5000` + WAL so a second writer waits for the lock instead of failing with
  `SQLITE_BUSY`.
- `DrainEgress`, the `chatting.egress.v1` queue constant, and the `NewRunner` egress-handler
  parameter are retained (dead in the live path, still unit-tested) pending a follow-up removal.

## Relationship to 0004

0004 (completion vs incremental) is unchanged: the payload semantics are the same; only the transport
that carries them moved from a queue to a synchronous call.
