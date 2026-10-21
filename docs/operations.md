# Operations

## Deployment

The only deployment is the `magpie` host, defined in the `lab` repo:

- `flake.nix` builds `chatting-handler` from `go/handler`, wraps
  `python -m app.worker.main` as `chatting-worker`, and builds `bbmb-server`.
- `nixos/hosts/magpie/chatting.nix` defines the systemd units.
- `nixos/hosts/magpie/chatting-config.nix` renders `/etc/chatting/handler.json`
  and `/etc/chatting/worker.json`. A config option that file does not set is
  not in use anywhere.
- `nixos/hosts/magpie/chatting-secrets.nix` renders the secret env files with
  sops-nix. Config refers to secrets by env-var name (the `*_env` keys).

After CI passes on `main`, `.github/workflows/open-lab-deploy-pr.yml` opens a
lab PR that bumps the `chatting` flake input. Merging and switching that PR is
the deploy.

## Services

| Unit | Process | Listens on |
| --- | --- | --- |
| `chatting-bbmb` | `bbmb-server` | `:9876` (broker), `:9877` (metrics) |
| `chatting-handler` | `chatting-handler --config /etc/chatting/handler.json` | `9464` metrics/admin, `127.0.0.1:9467` egress |
| `chatting-worker` | `chatting-worker --config /etc/chatting/worker.json` | `9465` activity UI |

The handler's metrics/admin listener serves `/metrics`, the `/schedules`,
`/reminders` and `/personal-inbox` pages, and the `/api/schedules` and
`/api/reminders` APIs. The egress listener serves `/egress`,
`/work-items/register-pr` and `/api/personal-inbox`. It binds loopback because
it can send messages to the outside world; keep it off the metrics listener.

The worker's activity UI lists work items and their runs. Active runs stream
through `/api/tasks/<task_id>/events`; raw activity JSON is at
`/activity.json`.

The handler owns every integration secret and is the only process that sends
anything externally. The worker reads no integration secrets. `gh` must be
authenticated as the handler user for GitHub polling and issue comments, and
Codex must be logged in as the worker user (`scripts/magpie-codex-reauth.sh`
in lab).

## Worker

`codex_working_dir` sets the Codex subprocess cwd. `workspace_root` holds the
persistent per-work-item workspaces. The worker runs up to
`executor_pool_size` tasks at once (default two); tasks for one work item run
one at a time in arrival order. Run only one worker against a worker database.

### Supervisors

The worker process is a manager for one supervisor process per slot. Each
supervisor owns its executor and task bookkeeping. On startup the manager
reconnects to their Unix sockets, so running tasks and their follow-ups survive
a manager restart. Supervisors retain completion status until the manager
acknowledges it.

Supervisors live in `<absolute db_path>.supervisors`, which must be a
private, persistent directory owned by the worker user, and short enough for
Unix socket paths. Slot N uses `supervisor-N.sock`, a slot lock, a config
snapshot and a log there. The manager discovers existing slots before
recovering abandoned inbox claims, and never reclaims work owned by a live
supervisor.

Changing the pool size drains surplus slots. Config, environment or runtime
changes take effect by replacing idle supervisors; busy ones finish their
current task first. A dead or unresponsive supervisor is a failure: the manager
stops rather than replacing it or repeating uncertain work, and the slot lock
remains as evidence. Inspect its log, task state and any surviving executor
before resolving the slot by hand. Supervisor, host and whole-unit crashes are
not recovered automatically.

### systemd requirements

- `KillMode=process`, so restarting the manager does not kill supervisors and
  executors in its cgroup.
- `PrivateTmp` disabled. systemd removes a unit's private temporary directory
  when the manager stops, even though supervisors survive; their mount then
  points at a deleted directory, finish sockets disappear and new temporary
  files fail.
- `TMPDIR` set to a short, persistent, worker-owned `0700` directory (for
  example `/var/lib/worker/chatting-tmp`), created through tmpfiles without an
  age-based cleanup rule.
- With `ProtectSystem=strict`, `/tmp` and `/var/tmp` listed in
  `ReadWritePaths`, since they no longer get writable private mounts.

A busy supervisor keeps the mount namespace and environment it started with, so
changing the unit does not fix a supervisor that has already lost files. Do not
kill or requeue its executor automatically, since it may have completed side
effects; resolve the task with the operator first.

For a full shutdown, stop the manager, then drain and stop the supervisors:

```sh
python -m app.worker.supervisor --directory /path/to/worker.db.supervisors --shutdown
```

This waits for running tasks, acknowledges retained completions and stops the
slots.

## Inspecting state

The handler and worker databases have no built-in retention. Lab's
`chatting-prune.nix` deletes rows older than 30 days from both each week; the
personal inbox database and cache are not pruned.

Run records, audit events and dead letters live in the worker database:

```sh
sqlite3 /var/lib/worker/chatting-worker.db "select run_id, result_status, created_at from run_records order by created_at desc limit 20;"
sqlite3 /var/lib/worker/chatting-worker.db "select dead_letter_id, status, created_at from dead_letters where status = 'pending' order by created_at desc;"
```

Useful worker log lines: `worker_processed` (per-task outcome, including
`execution_error` and `dead_letter`), `worker_egress_dropped_by_handler`,
`worker_egress_delivery_deferred` and `supervisor_task_failed`.
