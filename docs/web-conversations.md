# Web conversations

The platform's Conversations view is a second transport for existing Telegram
conversations. Each chat/topic has its own view. A web message resumes the
handler-assigned work item and its workspace/session; it does not create a new
Telegram message or depend on the Telegram API.

## Runtime configuration

Web ingress is disabled until both processes have a private Unix socket configured:

- Handler: `platform_api_socket`, an absolute socket path.
- Platform: `handler_socket`, the same absolute path.

For example, both may use `/run/chatting-web/handler.sock`. This is an interface
example, not an installed deployment value. The deployment repository must
provision the parent directory, service users/groups, sandbox write paths and
socket access. The handler creates the socket with mode 0660. A protected parent
directory with an appropriate shared group and setgid bit can give access to
just the handler and platform without granting access to either service's
database. The worker does not need access to this socket. The app does not add
users to groups or change host permissions.

The socket exposes no TCP listener. It preserves active listeners and existing
non-socket files; a stale socket refusing connections can be replaced after a
crash. Normal shutdown removes the listener's socket.

The platform's existing network access gate is still required: anyone who can
use this operator UI can submit work to the executor. Same-origin write checks
apply to conversation sends, and browser cookies/Authorization headers are not
forwarded to the private handler API. A missing socket configuration produces
an explicit unavailable response instead of falling back to a network endpoint.

Deploy compatible handler, worker and platform builds together. The worker
owns history and needs its normal database write access; no new worker
configuration or integration secret is needed. Handler ingress uses
`telegram_context_refs`, falling back to `context_refs`.

## Message flow and delivery

1. The handler lists known Telegram routes, including topic identity and their
   existing work item. The platform sends only a route ID, text and a client
   message ID; clients cannot submit instructions, context paths, an actor or
   arbitrary work-item assignments.
2. In a transaction, the handler reserves that assignment and stores the web
   envelope in `web_ingress`. HTTP 202 means **queued**, not answered.
3. The normal ingress loop publishes pending web envelopes through BBMB.
   Publication/acknowledgement and worker staging reuse existing deduplication
   and restart behavior. Telegram polling can delay a loop; its failure does
   not discard web submissions.
4. The worker records the inbound web turn, executes in the existing work item,
   and supplies recent recorded Telegram/web turns as context.
5. The reply contract directs `app.main_reply` to use `channel: "web"` and the
   event's target. The handler restricts this channel to that task's own web
   conversation. After acceptance, the worker durably records the reply and
   marks delivery. Worker-generated visible error replies use the same history.
   Completion events remain internal and are not shown as chat messages.
6. The platform polls the worker history. Web replies appear without a Telegram
   send. Cross-posting requires an explicit user request.

Client message IDs make an unchanged retry idempotent; reusing one with a
different body returns 409. Drafts and an uncertain send's ID are retained in
the browser tab's session storage, including across reloads. Sending is disabled
while a submission is in flight.

The worker keeps web history in `worker_web_messages`, separately from
Telegram's message-ID-based history. The UI combines these histories only for
the selected chat/topic, labels the transport, and supports cursor pagination.
History starts when Chatting recorded it; it is not a Telegram backfill. Existing
attachment names appear as labels, with no local file-download endpoint. Web
input and replies support text only in this version.

Queued/working status comes from durable submissions and worker inbox state.
Answered means a web answer is recorded. A finished task without a web answer
shows failed with a task link. The original Telegram history and search CLI
continue to use actual Telegram message IDs.

These history rows are retained with conversation history. Handler submissions
are durable records, including the request body and client ID, rather than a
transient browser cache.

## HTTP interfaces

The platform gateway explicitly permits:

- `GET /api/handler/conversations`: known route IDs, labels, chat/topic and work item.
- `GET /api/handler/conversations/:id/messages`: up to 100 outstanding durable web
  submissions, including ones not yet staged by the worker. Completed tasks
  leave this feed and remain in worker history.
- `POST /api/handler/conversations/:id/messages`: queue text on an existing route.
- `GET /api/worker/conversations/:id/messages?before=<cursor>&limit=100`:
  combined history, statuses and an optional `next_cursor`.

The handler's private socket uses the same paths without the `/handler` gateway
segment. The worker uses paths without `/worker`. Route IDs are opaque to the
browser; they identify chat plus topic, not a worker-controlled destination.

Example send body:

```json
{
  "message": "Continue our discussion here.",
  "client_message_id": "e12445101e9c497db0b9e3df6f37ece7"
}
```

The message must be nonblank and at most 32000 UTF-8 bytes. Client IDs contain
16–80 ASCII letters, digits, underscores or hyphens. Unknown request fields and
trailing JSON values are rejected. The composer uses an 8000-character limit
within that byte bound.

See [testing](testing.md) for a disposable browser preview and the real
handler/broker/worker roundtrip test.
