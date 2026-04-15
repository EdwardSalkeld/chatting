# Connector: Scheduled Jobs

Module: `app.connectors.interval_schedule_connector`

## Purpose

Live scheduled-job connector that emits `cron`-source events from cron expressions.

## Runtime config keys

Configured via `schedule_file` JSON loaded by `app.main_message_handler`.
Global schedule prompt guidance comes from message-handler config:
- `prompt_context` (optional global prompt instructions)
- `cron_prompt_context` (optional schedule-source prompt instructions)

Each job supports:
- `job_name` (required)
- `content` (required)
- `cron` (required 5-field cron expression)
- `timezone` (optional, defaults to `UTC`; uses an IANA name such as `Europe/London`)
- `context_refs` (optional list of non-empty strings)
- `prompt_context` (optional list of per-job prompt instructions)
- `reply_channel_type` (optional non-empty string, requires `reply_channel_target`)
- `reply_channel_target` (optional non-empty string, requires `reply_channel_type`)

Each job must define a cron schedule.

## Examples

Cron:

```json
{
  "job_name": "london-morning",
  "content": "Prepare the 8am London update",
  "cron": "0 8 * * *",
  "timezone": "Europe/London"
}
```

## Output mapping

- `source`: `cron`
- `id` / `dedupe_key`: `cron:<job_name>:<scheduled_iso>`
- `reply_channel`:
  - default: `log:<job_name>`
  - override: `<reply_channel_type>:<reply_channel_target>` when both override fields are set

## Notes

- Startup validation is strict: unknown keys, missing keys, invalid types, blank fields, invalid cron expressions, and invalid timezone names are rejected.
- Prompt guidance is assembled in this order before task content:
  global `prompt_context`, then `cron_prompt_context`, then the job's own `prompt_context`.
- `reply_channel_type` and `reply_channel_target` must be provided together.
- Cron schedules are evaluated in the configured local timezone and emitted as UTC timestamps.
- If `timezone` is omitted for a cron job, `UTC` is used.
- DST behavior follows real local wall-clock times:
  - nonexistent local times during the spring-forward transition are skipped
  - repeated local times during the fall-back transition can fire twice, once for each real UTC instant
