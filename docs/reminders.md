# One-off reminders

The handler owns durable one-off reminder timing and lifecycle state. The worker
creates and manages reminders through the handler JSON API; there is no separate
scheduling CLI.

## API

- `POST /api/reminders` creates a reminder.
- `GET /api/reminders?status=scheduled` lists upcoming reminders. `status` may
  also be `fired`, `cancelled`, or `all`.
- `GET /api/reminders/{reminder_id}?history=1` returns the latest revision and
  optional revision history.
- `PUT /api/reminders/{reminder_id}` reschedules a scheduled reminder while
  retaining its id and incrementing its revision.
- `DELETE /api/reminders/{reminder_id}` cancels a scheduled reminder without
  deleting its history.

Example request body:

```json
{
  "run_at": "2026-08-15T14:30:00+01:00",
  "prompt": "Send the reminder response now",
  "reply_channel": {
    "type": "telegram",
    "target": "-1001234567890",
    "metadata": {}
  },
  "context_refs": [],
  "prompt_context": [],
  "created_from_task_id": "task:telegram:example",
  "created_by": "worker",
  "idempotency_key": "task:telegram:example:reminder:1"
}
```

`run_at` must be RFC3339 with `Z` or an explicit offset and is returned in UTC.
POST and PUT require an idempotency key. Retrying the same request with the same
key returns the existing revision; reusing the key with different data returns
409. Every 4xx JSON response includes a `usage` object containing endpoints, an
example body, and correction notes so an agent can repair its request.

Workers should write request JSON to a temporary file and use `curl
--data-binary @<path>` against the handler API, as they do for schedules. They
should copy the current task's reply channel unless another destination was
explicitly requested.

## Delivery semantics

The reminder connector emits overdue reminders after handler downtime. Event ids
are stable as `reminder:{reminder_id}:{revision}`. The row remains `scheduled`
until the normal ingress path publishes the task or finds its event in the
handler dedupe ledger, then the connector acknowledgement marks it `fired`.
Broker failures therefore leave the reminder available for retry.

`fired` means accepted into the worker queue, not that worker execution or final
egress delivery succeeded.

## UI and observability

`/reminders` shows upcoming reminders and retained fired/cancelled history, with
forms to create, reschedule, and cancel reminders. Prometheus metrics cover
created, cancelled, due, late, retried, published, and fired lifecycle events.

The API is served on the handler metrics/admin listener (normally port 9464), so
it has the same trusted-network boundary as the schedules API.
