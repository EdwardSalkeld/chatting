# Connector: GitHub

GitHub events reach the handler by webhook; see
[webhook ingress](../webhook-ingress.md) for the endpoint and its
`github_webhook_*` config keys. The handler no longer polls GitHub.
`go/handler/internal/connectors/github` now holds only the workflow-run lookup
described below.

Replies go out as a GitHub comment on the issue or pull request. `gh` must be
authenticated as the handler user for those comments and for the run lookup.

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

## Retired config keys

`github_repositories` and `github_assignee_login` configured the polling
connector. They are still accepted, with any value, so an existing config keeps
loading, but they do nothing: the handler logs `handler_config_retired_keys` at
startup when either is present. Remove them from the config.
