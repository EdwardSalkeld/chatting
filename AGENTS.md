# Local Run Notes

- If starting a new feature, check that you have current code before doing any implementation work.
  Update from current `main` / current branch tip first so new work does not start from stale code.

- Sync local env: `uv sync`
- Run all tests: `uv run python -m unittest discover -s tests`
- Run message-handler tests: `uv run python -m unittest tests.test_message_handler_runtime tests.test_main_github_ingress`
- Run worker tests: `uv run python -m unittest tests.test_worker_runtime`
- Run GitHub ingress runtime tests: `uv run python -m unittest tests.test_github_ingress_runtime`
- Run app-main admin/query CLI tests: `uv run python -m unittest tests.test_main`

## Runtime Entry Points

- Split mode only:
  - `uv run python -m app.main_message_handler --config <message-handler.json>`
  - `uv run python -m app.main_worker --config <worker.json>`
- `app.cli` is the preferred admin/query CLI (`--list-*`, dead-letter replay, metrics).
- `app.main` remains available as a compatibility alias for the same admin/query CLI.

## Query Examples

- List runs: `uv run python -m app.cli --db-path /tmp/chatting-message-handler.db --list-runs --limit 20`
- List audit events: `uv run python -m app.cli --db-path /tmp/chatting-message-handler.db --list-audit-events --limit 20`
- List metrics: `uv run python -m app.cli --db-path /tmp/chatting-message-handler.db --list-metrics`

## GitHub Workflow

- If asked to complete a GitHub issue in this repo, opening or updating a PR is part of the expected deliverable unless the user explicitly says not to create one.

## Tooling Note

- If `rg` is unavailable locally, use `find . -type f` for file discovery and `grep -n` for content search.
