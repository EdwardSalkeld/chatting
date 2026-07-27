# 0005: Dedicated VM Plan For Chatting

Date: 2026-07-27

## Status

Draft

## Goal

Move `chatting` off Blink onto a dedicated lab-managed VM while preserving the
current split runtime model:

- `handler`: integrations and outbound dispatch
- `worker`: executor runtime and workspace access
- `bbmb`: local message bus

The VM is dedicated to `chatting`, so the host-level user names should be:

- `handler`
- `worker`
- `bbmb`
- `edward` (sudo)
- `billy` (sudo)

The security boundary is separate Unix users and private state, not separate
containers.

## Non-Goals

- Keep the Docker Compose deployment model.
- Keep the `site` preview service.
- Collapse handler and worker into one Unix user.
- Depend on Blink beyond one-off migration sync and rollback.

## Current Runtime Facts

From the current repo/runtime shape:

- `handler` owns IMAP, SMTP, Telegram, GitHub polling, outbound dispatch, and
  handler metrics on port `9464`.
- `worker` owns executor auth and the live Codex workspace, and serves the
  local activity UI on port `9465`.
- `bbmb` listens on port `9876` and exposes metrics on `9877`.
- Handler and worker each keep their own SQLite DB.
- Telegram attachments are handler-owned.
- The worker image currently bundles:
  Python 3.13, `uv`, `git`, `gh`, `bubblewrap`, `node`, `npm`, `ripgrep`,
  `sqlite3`, `curl`, CA certs, Go, `codex`, and `claude`.

The dedicated VM should preserve those ownership boundaries even though all
three services will run on one host.

## Service Layout

### `bbmb`

- User: `bbmb`
- Listens on `127.0.0.1:9876`
- Metrics on `127.0.0.1:9877`
- No access to handler or worker secrets
- Persistent state: none beyond logs unless BBMB grows local state later

### `handler`

- User: `handler`
- Connects to `127.0.0.1:9876`
- Metrics on `127.0.0.1:9464`
- Owns:
  - handler SQLite DB
  - IMAP/SMTP/Telegram environment secrets
  - Telegram attachment storage
  - GitHub CLI auth if GitHub polling/egress remains enabled
- Must not read:
  - worker Codex auth
  - worker Claude auth
  - worker workspace contents except via explicit future sharing

### `worker`

- User: `worker`
- Connects to `127.0.0.1:9876`
- Activity UI on `127.0.0.1:9465`
- Owns:
  - worker SQLite DB
  - Codex auth
  - Claude auth, if retained
  - executor provider secrets
  - the checked-out `/workspace`
- Must not read:
  - handler mail/Telegram secrets
  - handler attachment storage except through explicit future handoff

## Host Filesystem Layout

Use simple dedicated paths with ownership matching the service user:

- `/srv/chatting/repo`
  - checked-out repo or assembled runtime tree
  - owned by `root`, readable by service users
- `/var/lib/handler`
  - `handler.db`
  - `telegram-attachments/`
  - any other handler-owned durable state
- `/var/lib/worker`
  - `worker.db`
  - `.codex/`
  - `.claude/` if retained
- `/var/lib/bbmb`
  - reserved for any future BBMB state
- `/srv/chatting/workspace`
  - worker-only writable live workspace
- `/etc/chatting/handler.env`
- `/etc/chatting/handler.json`
- `/etc/chatting/worker.env`
- `/etc/chatting/worker.json`

Recommended ownership:

- `/var/lib/handler`: `handler:handler`, mode `0700`
- `/var/lib/worker`: `worker:worker`, mode `0700`
- `/var/lib/bbmb`: `bbmb:bbmb`, mode `0750`
- `/srv/chatting/workspace`: `worker:worker`, mode `0750` or stricter
- `/etc/chatting/*.env`: root-owned, mode `0640`, group-readable only by the
  matching service account
- `/etc/chatting/*.json`: root-owned, mode `0644` unless they contain secrets;
  use `0640` if secrets end up embedded

## Networking

All runtime ports should stay local-only initially:

- `127.0.0.1:9876` BBMB
- `127.0.0.1:9877` BBMB metrics
- `127.0.0.1:9464` handler metrics
- `127.0.0.1:9465` worker activity UI

No public ingress is needed for the first cut. SSH access is enough for
operations.

## Software Needed On The VM

Base host packages:

- `git`
- `curl`
- `sqlite`
- `ripgrep`
- `rsync`
- `tmux`
- `vim`
- `htop`

Runtime/build packages:

- `python3.13`
- `uv`
- `go`
- `bubblewrap`
- `nodejs`
- `gh`
- CA certificates

Runtime CLIs:

- `codex`
- optionally `claude`

Service artifacts:

- `bbmb-server`
- `chatting-handler`

The first non-Docker cut can either:

1. build from a checked-out repo on-host, or
2. ship prebuilt binaries plus a prepared Python environment

For the first migration, a checked-out repo plus systemd units is the simplest
shape to debug.

## VM Sizing

The main variable is the worker workspace plus auth/state, not the host OS.

Target with headroom, not a tight fit:

- root disk: `40 GiB`
- workspace disk: `80 GiB`

This aligns with the first lab VM shape and should leave room for:

- the base NixOS install and toolchain
- one checked-out `chatting` repo copy
- worker auth and SQLite state
- the synced workspace
- temporary rollback copies during migration

Known inputs today:

- the current `/workspace/chatting` checkout in this worker is about `70 MiB`
- Blink stores persistent `chatting` Docker data under
  `/mnt/ssd4tb/docker-volumes/chatting_*`
- the live workspace and auth volumes on Blink are still expected to dominate
  total usage

Before the final Terraform apply for the production VM shape, measure Blink and
keep at least 2x the observed in-use bytes:

```sh
du -sh /home/edward/develop/chatting
du -sh /mnt/ssd4tb/docker-volumes/chatting_handler-data
du -sh /mnt/ssd4tb/docker-volumes/chatting_worker-data
du -sh /mnt/ssd4tb/docker-volumes/chatting_codex-auth
du -sh /mnt/ssd4tb/docker-volumes/chatting_claude-auth
du -sh /mnt/ssd4tb/docker-volumes/chatting_gh-auth
du -sh /mnt/ssd4tb/docker-volumes/chatting_shared-temp
du -sh /mnt/ssd4tb/docker-volumes/chatting_html-output
```

If the worker workspace currently lives outside the `chatting` checkout, measure
that path directly and use it as the main sizing input.

## Base Host Bootstrap Checklist

This is the first VM bring-up target before installing `chatting` itself.

1. Create the VM in `lab`.
2. Install NixOS from the `lab` flake.
3. Create the host users:
   - `edward` in `wheel`
   - `billy` in `wheel`
   - service users `handler`, `worker`, `bbmb` with no interactive login path
4. Install the base packages and runtime/build tools listed above.
5. Create directories and permissions:
   - `/srv/chatting/repo`
   - `/srv/chatting/workspace`
   - `/etc/chatting`
   - `/var/lib/handler`
   - `/var/lib/worker`
   - `/var/lib/bbmb`
6. Prove the boundary:
   - `sudo -u worker test ! -r /etc/chatting/handler.env`
   - `sudo -u handler test ! -r /var/lib/worker`
   - `sudo -u worker test ! -r /var/lib/handler`
7. Confirm `edward` and `billy` can SSH in and use passwordless sudo.
8. Only after that, add `chatting` systemd units and runtime config.

## Data Migration Plan

Initial migration should be copy-first, then start services:

1. Build the VM and lock down users/directories.
2. Copy the repo/runtime tree.
3. Sync handler state:
   - handler SQLite DB
   - Telegram attachment storage
   - any GitHub auth needed by handler
4. Sync worker state:
   - worker SQLite DB
   - worker `.codex/`
   - worker `.claude/` if retained
   - worker workspace tree
5. Start `bbmb`, then `handler`, then `worker`.
6. Do a short final re-sync during a quiet window if needed.
7. Cut traffic over.
8. Stop `chatting` on Blink, but leave Blink otherwise untouched as rollback
