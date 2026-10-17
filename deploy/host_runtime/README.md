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

## Manager restarts and supervisors

The worker entrypoint now manages detached supervisor processes. Under systemd,
the worker unit must use `KillMode=process`, so stopping or restarting the manager
does not kill supervisors and executors in its cgroup. Unix sockets and slot locks
live in the configured `supervisor_dir`, by default beside the worker database;
keep that directory persistent and writable by the worker user. Do not place it
in a service-private temporary directory that changes across restarts.

The worker unit must also disable `PrivateTmp`. systemd removes that unit's
private temporary directories when the manager stops, even though
`KillMode=process` leaves its supervisors and executors alive. Their inherited
mount then points to a deleted directory: existing finish sockets disappear,
and creating new temporary files fails. A successful manager reconnection does
not check this resource lifetime.

Set `TMPDIR` to a short, persistent, worker-owned directory with mode `0700`
(for example `/var/lib/worker/chatting-tmp`), and create it through tmpfiles
without an age-based cleanup rule. The directory must be writable under the
unit's filesystem restrictions and survive manager restarts. This also keeps
Python's finish and escalation sockets/files away from temporary-directory
cleanup. Disabling `PrivateTmp` preserves ordinary `/tmp` use by tools that do
not honor `TMPDIR`.
With `ProtectSystem=strict`, explicitly include `/tmp` and `/var/tmp` in
`ReadWritePaths` too: they no longer get writable private mounts implicitly.

These settings protect newly started supervisors. A busy supervisor retains
its old mount namespace and environment until it finishes; changing the unit
does not restore files already deleted from that namespace. Do not kill or
requeue an affected executor automatically, since it may have completed side
effects. Resolve an affected task explicitly with the operator before retiring
its supervisor.

On first rollout, drain the old worker before upgrading: its executors do not yet
have separate supervisors. After rollout, a manager restart preserves existing
work. Busy supervisors continue with the runtime and environment they started
with, then are replaced when idle if the manager's settings or runtime changed.

For a full application shutdown, stop the manager and then run:

```sh
python -m app.worker.supervisor --directory /path/to/worker.db.supervisors --shutdown
```

This waits for running tasks, acknowledges retained completions, and stops the
slots. Stopping the manager alone intentionally leaves them alive. Supervisor,
container, and host crashes are failures; automatic recovery is not implemented.
