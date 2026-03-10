# Connector: Fake Cron

Module: `app.connectors.fake_cron_connector`

## Purpose

In-memory cron connector used in tests and fixture-style local runs.

## Input model

`CronTrigger` fields:
- `job_name`
- `content`
- `scheduled_for` (timezone-aware datetime)
- `context_refs`
- `policy_profile` (default `default`)

## Output mapping

- `source`: `cron`
- `id` / `dedupe_key`: `cron:<job_name>:<scheduled_for_iso>`
- `reply_channel`: `log:<job_name>`
- `actor`: `null`

## Configuration

No CLI/runtime config path. It is created directly in code/tests.
