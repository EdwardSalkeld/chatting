# GitHub Issue Assignment Polling

`chatting` can ingest GitHub issue assignments without exposing any webhook endpoint.

GitHub assignment polling is implemented as a first-class connector inside the existing
message-handler loop:

```bash
uv run python -m app.main_message_handler --config /path/to/message-handler-runtime.json
```

When `github_repositories` is configured, the GitHub connector polls `gh api graphql` for
`AssignedEvent` timeline items, filters by assignee login, and emits normalized `TaskEnvelope`
objects. Message-handler then handles publication to `chatting.tasks.v1` the same way it does for
other connectors.

## Required config

- `github_repositories`: list of `owner/repo` and/or `owner/*` selectors to scan.

## Optional config

- `github_assignee_login`: only assignments to this GitHub login are emitted.
  If omitted, message-handler uses the authenticated `gh` user login (`viewer.login`).
- Generated tasks always reply via GitHub issue comment egress to the assigned issue URL.

## Idempotency and checkpointing

- Event dedupe key: `github:{repo_id}:{issue_id}:{assigned_event_id}`.
- Processed keys are persisted via SQLite idempotency table.
- Poll progress checkpoint (`created_at` + `event_id`) is persisted in SQLite table
  `github_assignment_checkpoints`.

This keeps retries/restarts safe while avoiding replaying historical assignment events each loop.
