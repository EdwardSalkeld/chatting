# Connector: Interval Schedule

Module: `app.connectors.interval_schedule_connector`

## Purpose

Live cron-style connector that emits events on fixed intervals.

## Runtime config keys

Configured via `schedule_file` JSON loaded by `app.main`.
Each job supports:
- `job_name` (required)
- `content` (required)
- `interval_seconds` (required, positive int)
- `context_refs` (optional list of non-empty strings)
- `policy_profile` (optional, default `default`)
- `start_at` (optional RFC3339 datetime)

## Output mapping

- `source`: `cron`
- `id` / `dedupe_key`: `cron:<job_name>:<scheduled_iso>`
- `reply_channel`: `log:<job_name>`

## Notes

- Startup validation is strict: unknown keys, missing keys, invalid types, blank fields, and invalid `start_at` values are rejected.
