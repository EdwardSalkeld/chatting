# Connector: GitHub

Package: `go/handler/internal/connectors/github`

When `github_repositories` is set, the handler polls `gh api graphql` for two
kinds of event, without exposing a webhook endpoint. `gh` must be
authenticated as the handler user.

- **Issue assignments**: `AssignedEvent` timeline items assigning an issue to
  the configured login. Dedupe key
  `github:{repo_id}:{issue_id}:{assigned_event_id}`.
- **Pull request reviews**: reviews on open pull requests authored by that
  login. Dedupe key `github-review:{repo_id}:{pr_id}:{review_id}`.

Both reply by GitHub comment on the issue or pull request.

When an executor opens a pull request with `gh pr create`, the worker sees the
command complete in the executor's event stream and registers the PR against
the task's work item on the handler. PRs opened another way, or taken over, are
registered with `python3 -P -m app.main_work_items register-pr <task_id>
<pr_url>`, which reads its settings from the worker config in the environment.

GitHub events and GitHub notification emails that link to a registered PR then
route to the same work item. When that work item started in Telegram, the task
replies to the original chat and joins its conversation, so an app-server
executor resumes the same Codex thread.

CI failure emails link only the workflow run, so the handler asks GitHub which
pull request the run, or failing that its head commit, belongs to.

## Config keys

- `github_repositories`: `owner/repo` and/or `owner/*` selectors to scan.
- `github_assignee_login` (required with `github_repositories`): the login to
  watch.

Each poll reads at most 25 issues or pull requests per repository, and 10
timeline events or reviews per item.

## Checkpointing

Processed dedupe keys go in the handler's idempotency table. Poll progress
(`created_at` and event id) goes in `github_assignment_checkpoints`, one row
per stream, so restarts do not replay old events.
