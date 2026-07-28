# Host Runtime Deployment

This directory contains generic helpers for running the split `chatting`
runtime directly on a host instead of through Docker Compose.

## What this covers

- build the host runtime tree (`deploy/host_runtime/build-runtime.sh`)
- run BBMB, the handler, and the worker under an external service manager

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

## Host config files

Provide `handler.json` and `worker.json` (and any env/schedule files) with
host-appropriate paths at the config dir the entrypoints below read. An infra
repo that manages the host is expected to own these — for example the NixOS
`lab` repo generates them declaratively for the `magpie` host.

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
