# Testing

Run `uv sync` once first.

```bash
uv run pytest -n auto
cd go/handler && go test ./...
uv run ruff check app tests
uv run ruff format --check app tests
uv run pyright app/
```

Apply Python formatting with `uv run ruff format app tests`. Ruff targets Python
3.14 and removes optional parentheses from multi-exception handlers. CI checks
this style; parentheses remain where required, including handlers with `as`.

Run one module with, for example, `uv run pytest tests/test_worker_runtime.py`.

## End-to-end tests

The split-mode e2e tests start a real `bbmb-server` and Go handler. They skip
unless `CHATTING_BBMB_SERVER_BIN` points at a `bbmb-server` binary. Point
`CHATTING_E2E_HANDLER_BINARY` at a prebuilt handler to avoid `go run`
start-up on every test:

```bash
(cd go/handler && go build -o /tmp/chatting-handler ./cmd/chatting-handler)
CHATTING_E2E_HANDLER_BINARY=/tmp/chatting-handler \
CHATTING_BBMB_SERVER_BIN=/path/to/bbmb-server \
uv run pytest tests/test_split_mode_e2e.py
```

On macOS, set a short `TMPDIR` (for example `/tmp/chatting`) for these: the
default is too long for the worker's supervisor socket paths.

`tests/e2e/test_email_e2e.py` also needs a GreenMail server; the `E2E` workflow
in `.github/workflows/e2e.yml` shows how CI provides it.

The JMAP connector has an opt-in read-only check against a real account: put a
read-only token in `~/.config/chatting/fastmail-jmap-token` and run
`JMAP_LIVE=1 go test ./internal/connectors/jmap/` from `go/handler`.
