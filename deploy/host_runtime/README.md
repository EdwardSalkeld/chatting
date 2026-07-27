# Host Runtime Deployment

This directory contains generic helpers for running the split `chatting`
runtime directly on a host instead of through Docker Compose.

## What this covers

- build the host runtime tree (`deploy/host_runtime/build-runtime.sh`)
- run BBMB, the handler, and the worker under an external service manager
- rewrite Docker-oriented config files into host paths

The helpers are intentionally path-configurable so an infra repo can decide
where the checkout, config, state, and workspace should live.

## Build the runtime

Run in a checked-out `chatting` repo on the target host:

```sh
./deploy/host_runtime/build-runtime.sh
```

That does three things:

1. creates `.venv` with the worker dependencies
2. builds `.runtime/bin/chatting-handler`
3. downloads `.runtime/bin/bbmb-server`

Override `CHATTING_RUNTIME_DIR` or `CHATTING_BBMB_VERSION` if needed.

## Render host config files

If your source config was written for Docker Compose, render host-specific
copies with:

```sh
python3 deploy/host_runtime/render_runtime_config.py \
  --source-root /path/to/source-configs \
  --output-root /path/to/rendered-configs \
  --workspace-dir /absolute/host/workspace
```

Useful overrides:

- `--handler-state-dir`
- `--worker-state-dir`
- `--config-dir`
- `--bbmb-address`
- `--metrics-host` (defaults to `0.0.0.0` so an external Prometheus can scrape
  the handler; firewall-gated. Pass `127.0.0.1` for local-only metrics.)

This rewrites:

- handler and worker SQLite paths
- handler schedule-file paths
- handler context refs that point at `repo:/workspace`
- the worker `codex_working_dir`

## Service entrypoints

An infra repo can point systemd or another service manager at:

- `deploy/host_runtime/run-bbmb.sh`
- `deploy/host_runtime/run-handler.sh`
- `deploy/host_runtime/run-worker.sh`

The scripts accept overrides through environment variables such as:

- `CHATTING_RUNTIME_DIR`
- `CHATTING_CONFIG_DIR`
- `CHATTING_HANDLER_CONFIG_PATH`
- `CHATTING_WORKER_CONFIG_PATH`
- `CHATTING_BBMB_BIN`
- `CHATTING_HANDLER_BIN`
- `CHATTING_WORKER_PYTHON`
