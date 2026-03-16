# Connector: Scheduled Jobs

Module: `app.connectors.interval_schedule_connector`

## Purpose

Live scheduled-job connector that emits `cron`-source events from either fixed intervals or cron expressions.

## Runtime config keys

Configured via `schedule_file` JSON loaded by `app.main_message_handler`.
Each job supports:
- `job_name` (required)
- `content` (required)
- `interval_seconds` (optional positive int)
- `cron` (optional 5-field cron expression)
- `timezone` (required when `cron` is set, IANA name such as `Europe/London`)
- `context_refs` (optional list of non-empty strings)
- `policy_profile` (optional, default `default`)
- `start_at` (optional RFC3339 datetime)
- `reply_channel_type` (optional non-empty string, requires `reply_channel_target`)
- `reply_channel_target` (optional non-empty string, requires `reply_channel_type`)

Each job must define at least one schedule:
- interval mode: `interval_seconds` with optional `start_at`
- cron mode: `cron` plus `timezone`

If both `cron` and `interval_seconds` are present, cron scheduling takes precedence.

## Examples

Interval:

```json
{
  "job_name": "heartbeat",
  "content": "Run scheduled heartbeat",
  "interval_seconds": 300,
  "start_at": "2026-03-07T00:00:00Z"
}
```

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

- Startup validation is strict: unknown keys, missing keys, invalid types, blank fields, invalid cron expressions, invalid timezone names, and invalid `start_at` values are rejected.
- `reply_channel_type` and `reply_channel_target` must be provided together.
- Cron schedules are evaluated in the configured local timezone and emitted as UTC timestamps.
- DST behavior follows real local wall-clock times:
  - nonexistent local times during the spring-forward transition are skipped
  - repeated local times during the fall-back transition can fire twice, once for each real UTC instant
