# Quick Start

`chatting` runs as three services:
- `message-handler` (Go)
- `worker`
- `bbmb-server`

## Prerequisites

- Docker with Compose support
- Access to a shell
- GHCR login that can pull `ghcr.io/edwardsalkeld/chatting`
- Optional: Codex or Claude credentials if you want real executor mode

## 1) Clone and enter repo

```bash
git clone <your-repo-url>
cd chatting
```

## 2) Create runtime config

```bash
mkdir -p configs/handler configs/worker
cp configs/handler.json.example configs/handler/handler.json
cp configs/worker.json.example configs/worker/worker.json
cp configs/handler.env.example configs/handler/handler.env
cp configs/worker.env.example configs/worker/worker.env
```

Edit the copied files before starting the stack:
- `configs/handler/handler.json`: connector settings, egress channels, metrics, and integration paths
- `configs/handler/handler.env`: IMAP, SMTP, Telegram, and other integration secrets
- `configs/worker/worker.json`: executor settings and mounted workspace path
- `configs/worker/worker.env`: executor provider secrets

The Docker examples use container paths and Docker DNS:
- handler DB: `/data/handler.db`
- worker DB: `/data/worker.db`
- BBMB: `bbmb:9876`

## 3) Set the workspace mount

```bash
export LOCAL_WORKSPACE=/absolute/path/to/the/workspace/codex-should-use
```

## 4) Choose the runtime image

The default compose file pulls the published runtime image:

```bash
export CHATTING_RUNTIME_IMAGE=ghcr.io/edwardsalkeld/chatting:latest
```

You can pin a specific published tag instead, for example `sha-<commit>` from the
GitHub Container Registry package page.

If this host has not already authenticated to GHCR, log in once with a token that
has package read access:

```bash
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin
```

## 5) Start chatting

```bash
docker compose pull
docker compose up -d
```

The compose stack starts:
- `bbmb`
- `handler`
- `worker`
- `site` on port `3000`, serving files from `$LOCAL_WORKSPACE/site`

The Go message handler exposes Prometheus-style metrics at `http://127.0.0.1:9464/metrics`.
The worker exposes a read-only activity page at `http://127.0.0.1:9465/`, with matching JSON at
`http://127.0.0.1:9465/activity.json`.

If you want the static preview path to have content immediately, create a simple page in the
mounted workspace before or after startup:

```bash
mkdir -p "$LOCAL_WORKSPACE/site"
cat > "$LOCAL_WORKSPACE/site/index.html" <<'EOF'
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Site Preview</title>
  </head>
  <body>
    <h1>Site preview is live.</h1>
    <p>Edit files in the workspace-mounted site directory and reload port 3000.</p>
  </body>
</html>
EOF
```

## 6) Bootstrap CLI auth

When using real executor mode, authenticate the CLIs once inside the worker container. Auth state is
persisted in Docker volumes.

```bash
docker compose run --rm worker codex login
docker compose run --rm worker claude login
```

## 7) Run tests

Local tests are separate from the Docker runtime path and require Python 3.13+ plus `uv`.

```bash
uv sync
uv run python -m unittest discover -s tests
```

## 8) Query state and metrics

Use the worker page for a quick operator view, or query SQLite directly when you need deeper history:

```bash
docker compose exec handler sqlite3 /data/handler.db "select run_id, result_status, created_at from run_records order by created_at desc limit 20;"
docker compose exec handler sqlite3 /data/handler.db "select run_id, result_status, created_at from audit_events order by created_at desc limit 20;"
docker compose exec handler sqlite3 /data/handler.db "select result_status, count(*) from run_records group by result_status order by result_status;"
docker compose exec worker sqlite3 /data/worker.db "select run_id, result_status, created_at from run_records order by created_at desc limit 20;"
```

## Notes

- For the queue-by-queue runtime conversation, payload examples, and config levers, see
  [BBMB Message Flow](bbmb-message-flow.md).
- For full split-mode setup and operational details, see [Run Split Mode (BBMB)](run-split-bbmb.md).
- In split mode, inspect either the message-handler DB or the worker DB depending on what you
  want to inspect.
