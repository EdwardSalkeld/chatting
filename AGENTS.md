# Agent Notes

- Before starting a feature, update from current `main` / the branch tip so new work does not start from stale code.
- This repo defines the Chatting app. Document runtime requirements, configuration and interfaces here; keep host configuration, rollout procedures and installed values in each deployment's repository. Do not infer app usage from one deployment.
- Docs are indexed in [docs/README.md](docs/README.md).

## Commands

- Sync env: `uv sync`
- Python tests: `uv run pytest -n auto`
- Go tests: `cd go/handler && go test ./...`
- Lint: `uv run ruff check app tests` and `uv run pyright app/`
- E2E setup: [docs/testing.md](docs/testing.md)

## Entry points

- `cd go/handler && go run ./cmd/chatting-handler --config <handler.json>`
- `uv run python -m app.worker.main --config <worker.json>`

## GitHub workflow

- If asked to complete a GitHub issue in this repo, opening or updating a PR is part of the expected deliverable unless the user explicitly says not to create one.

## Tooling note

- If `rg` is unavailable locally, use `find . -type f` for file discovery and `grep -n` for content search.
