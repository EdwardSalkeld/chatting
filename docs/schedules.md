# Schedules

Recurring jobs live in the handler database. The worker manages them through
the handler JSON API, and an operator can use the `/schedules` page. Both are
served on the handler's metrics/admin listener (port 9464), alongside
[one-off reminders](reminders.md).

## API

- `POST /api/schedules` creates a schedule.
- `GET /api/schedules` lists them.
- `GET /api/schedules/{schedule_id}?history=1` returns one, optionally with its
  revision history.
- `PUT /api/schedules/{schedule_id}` replaces one.
- `DELETE /api/schedules/{schedule_id}` deletes one.

Example request body:

```json
{
  "job_name": "london-morning",
  "content": "Prepare the 8am London update",
  "cron": "0 8 * * *",
  "timezone": "Europe/London",
  "context_refs": ["repo:/workspace/memory"],
  "prompt_context": [],
  "reply_channel_type": "telegram",
  "reply_channel_target": "-1001234567890",
  "created_by": "worker"
}
```

- `cron` is a five-field expression, evaluated in `timezone` (an IANA name,
  default `UTC`).
- `reply_channel_type` and `reply_channel_target` must be supplied together.
  Without them the task replies to `log:<job_name>`.
- `context_refs` may be empty. Each entry must use `repo:/absolute/path`, for
  example `repo:/workspace/memory`. Bare paths, relative paths and unsupported
  schemes are rejected with HTTP 400 on both creation and replacement, before
  any schedule revision is saved. This matches the worker's context parser.
- The connector also skips and logs invalid stored schedules when polling;
  inspect existing schedules when upgrading and correct invalid references
  through `PUT /api/schedules/{schedule_id}`.
- Every 4xx response includes a `usage` object with the endpoints, an example
  body and correction notes, so an agent can repair its request.

## Output mapping

- `source`: `cron`
- `id` / `dedupe_key`: `cron:<job_name>:<scheduled UTC time>`
- The job's `prompt_context` becomes the task's instructions.
