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
uv run pytest tests/e2e/test_split_mode_e2e.py
```

On macOS, set a short `TMPDIR` (for example `/tmp/chatting`) for these: the
default is too long for the worker's supervisor socket paths.

`tests/e2e/test_email_e2e.py` also needs a GreenMail server; the `E2E` workflow
in `.github/workflows/e2e.yml` shows how CI provides it.

The JMAP connector has an opt-in read-only check against a real account: put a
read-only token in `~/.config/chatting/fastmail-jmap-token` and run
`JMAP_LIVE=1 go test ./internal/connectors/jmap/` from `go/handler`.

## Platform UI

The platform frontend has its own locked Node dependencies and checks:

```sh
npm --prefix platform ci
npm --prefix platform run format:check
npm --prefix platform run test
npm --prefix platform run build
```

Gateway tests run with the regular Go suite. They exercise real schedule and
reminder APIs with disposable SQLite databases, upstream errors, encoded IDs,
method/path restrictions and frontend deep links. Transcript tests cover both
executor output formats, split chunks, malformed output and delivery visibility.

### Disposable browser preview

Build the frontend first, then the two local binaries:

```sh
(cd go/handler && go build -o /tmp/chatting-platform ./cmd/chatting-platform)
(cd go/handler && go build -o /tmp/chatting-platform-handler-fixture ./internal/platform/testserver)
```

Choose a fresh state directory for each preview. In separate terminals from the
repository root:

```sh
/tmp/chatting-platform-handler-fixture --state-dir /tmp/chatting-platform-qa
PYTHONPATH=. uv run python tests/platform_preview.py --state-dir /tmp/chatting-platform-qa
```

The fixture handler listens on loopback port 19464, and the worker on 19465. Both
support `--port` and `--empty`. The handler also accepts `--no-inbox` to
check the optional-integration state. They use real APIs and stores, but seed example
work items, schedules, reminders and inbox decisions. No integrations or
executors are started. Pass `--rich` to both to judge density against realistic
volume: about 40 work items and 200 runs over 14 days with failures, dead
letters, token usage, three live executors and one task with over 300 events on
the worker; 15 versioned schedules, 30 reminders (two overdue), 120 inbox
decisions and synthetic handler metrics on the handler. Pass `--telegram-outage` to the handler fixture to simulate Telegram HTTP 502
failures alongside a dropped reply, without contacting Telegram. The warning
banner should appear on every route, and System should show the failing poll
and its error code. The handler fixture
always serves `/metrics` like the real handler. Start the platform with a
temporary config:

```json
{
  "host": "127.0.0.1",
  "port": 19466,
  "handler_url": "http://127.0.0.1:19464",
  "worker_url": "http://127.0.0.1:19465"
}
```

```sh
/tmp/chatting-platform --config /tmp/platform-qa.json
```

Open `http://127.0.0.1:19466`. The live fixture emits an event every five seconds.
Creating a `finish-live` file inside its state directory ends the simulated run
and lets the browser verify the transition to a completed transcript. Stop an
upstream to inspect unavailable-service and recovery states. For frontend hot
reload against this gateway, run:

```sh
CHATTING_PLATFORM_URL=http://127.0.0.1:19466 npm --prefix platform run dev
```

Browser checks should cover desktop and narrow layouts, each navigation route,
search and filters, schedule and reminder validation/create/edit/delete or
cancel, revision history, inbox decision details, live/completed transcripts,
pause/resume/follow/copy and delivery visibility, empty and unavailable states.
The fixture state is disposable and must never point at a deployment database.


### Web conversation preview

Build the handler fixture and platform as above, then choose fresh disposable
directories and a short private socket path. For example:

```sh
mkdir -m 700 /tmp/chatting-web-preview-socket
/tmp/chatting-platform-handler-fixture --state-dir /tmp/chatting-web-handler \
  --port 30464 --web-socket /tmp/chatting-web-preview-socket/handler.sock --telegram-outage
PYTHONPATH=. uv run python tests/web_chat_preview.py \
  --state-dir /tmp/chatting-web-worker \
  --handler-fixture-db /tmp/chatting-web-handler/handler.db --port 30465
```

The dedicated worker fixture seeds two separate topics and simulates web replies
after a short delay. It reads the explicitly supplied **fixture** handler DB to
simulate queue handoff, without a broker, executor, Telegram API or live state.
Never point either fixture at deployment databases.

Configure the platform with handler/worker URLs on 30464/30465, its own free
loopback port, and `handler_socket` pointing to the fixture socket. Open
`/conversations`. Verify topic isolation, web-only sending, queued/answered
transitions, older history, retained drafts after navigation/reload, error
recovery when the fixture handler stops, and desktop/mobile layouts.

`tests/e2e/test_web_chat_e2e.py` separately exercises the actual private API,
durable ingress loop, real BBMB transport, worker/executor helper, web reply
history and completion path with Telegram disabled. It uses the standard E2E
binary environment variables above and a fake executor; it does not need model
credentials or Telegram.
