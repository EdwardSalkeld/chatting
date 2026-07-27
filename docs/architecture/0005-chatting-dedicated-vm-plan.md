# 0005: Magpie Repurpose Plan For Chatting

Date: 2026-07-27

## Status

Draft

## Goal

Move `chatting` off Blink onto `magpie`, repurposing that existing lab-managed
host for a single workload while preserving the current split runtime model:

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

The repurposed `magpie` host should preserve those ownership boundaries even
though all three services will run on one host.

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

## Host Sizing

The main variable is the worker workspace plus auth/state, not the host OS.

Measured on Blink on 2026-07-27:

- `/home/edward/develop/chatting`: `699 MiB`
- `/home/edward/develop/chatting/.git`: `3.6 MiB`
- worker workspace root `/mnt/ext2tb/4/billy`: `1.7 GiB`
- backing partition `/mnt/ext2tb/4`: `392 GiB` total, `69 GiB` used, `303 GiB`
  available
- Blink root disk `/`: `467 GiB` total, `94 GiB` used, `350 GiB` available
- Blink Docker local volumes total: `17.27 GiB`
- Blink Docker build cache total: `21.48 GiB`

Repurposing `magpie` changes the storage decision:

- start with `magpie` root on its existing VM root disk
- add or reuse a small dedicated workspace disk for
  `/srv/chatting/workspace`
- prefer the suspected spare `5 GiB` local-lvm disk first, because current
  observed use is only `1.7 GiB`
- keep at least 2x observed workspace usage free at cutover time

That makes the first practical target:

- root disk: keep the current `magpie` root allocation for the base host work
- dedicated worker workspace disk: `5 GiB`, assuming the unused local-lvm
  volume is really available

If that `5 GiB` volume turns out not to exist, or if the initial sync lands
materially above the current `1.7 GiB`, fall back to one of:

- grow the `magpie` workspace disk in Proxmox before cutover
- physically move the current Blink worker disk that backs `/mnt/ext2tb/4`
  onto the lab host and attach or pass it through to `magpie`

Concrete host anchor for the physical-disk fallback on Blink:

- disk model: `WDC WD20EURS-63S48Y0`
- device: `/dev/sdc`
- worker partition in use: `/dev/sdc4` mounted at `/mnt/ext2tb/4`

The measured `1.7 GiB` worker tree is mostly repo checkouts and scratch space,
not one huge state directory. Largest visible entries on 2026-07-27 were:

- `untitled-music-project`: `739 MiB`
- `rumandpopcorn`: `188 MiB`
- `site-infra`: `160 MiB`
- `chatting` checkout: `70 MiB`
- several extra `chatting` worktrees/checkouts around `67 MiB` each
- hidden scratch directories `.tmp` and `.tmp-bin`: about `213 MiB` combined

That means a first migration does not need a huge fresh workspace disk if we
prune or selectively re-sync, but it still benefits from a dedicated writable
disk or mount rather than mixing worker churn into the host root filesystem.

## Base Host Bootstrap Checklist

This is the first VM bring-up target before installing `chatting` itself.

1. Rebuild `magpie` in `lab` as the dedicated `chatting` host.
2. Install or reinstall NixOS from the `lab` flake if the current guest is not
   already in the intended baseline state.
3. Create the host users:
   - `edward` in `wheel`
   - `billy` in `wheel`
   - service users `handler`, `worker`, `bbmb` with no interactive login path
4. Install the base packages and runtime/build tools listed above.
5. Attach and format the workspace disk for `/srv/chatting/workspace`.
6. Create directories and permissions:
   - `/srv/chatting/repo`
   - `/srv/chatting/workspace`
   - `/etc/chatting`
   - `/var/lib/handler`
   - `/var/lib/worker`
   - `/var/lib/bbmb`
7. Prove the boundary:
   - `sudo -u worker test ! -r /etc/chatting/handler.env`
   - `sudo -u handler test ! -r /var/lib/worker`
   - `sudo -u worker test ! -r /var/lib/handler`
8. Confirm `edward` and `billy` can SSH in and use passwordless sudo.
9. Only after that, add `chatting` systemd units and runtime config.

## Initial Migration Sequence

1. Repurpose `magpie` and apply the base host config.
2. Create and mount the workspace disk.
3. One-off sync the current Blink state:
   - `/home/edward/develop/chatting`
   - worker workspace content from `/mnt/ext2tb/4/billy`
   - Docker-backed handler and worker state exported into the new host layout
4. Start `bbmb`, then `handler`, then `worker`.
5. Run a short quiet-period re-sync if needed to reduce drift.
6. Cut traffic over and stop the `chatting` services on Blink.

## Data Migration Plan

Initial migration should be copy-first, then start services:

1. Build the VM and lock down users/directories.
2. Attach or provision the worker workspace storage before copying any data.
3. Copy the repo/runtime tree.
4. Sync handler state:
   - handler SQLite DB
   - Telegram attachment storage
   - any GitHub auth needed by handler
5. Sync worker state:
   - worker SQLite DB
   - worker `.codex/`
   - worker `.claude/` if retained
   - worker workspace tree
6. Start `bbmb`, then `handler`, then `worker`.
7. Do a short final re-sync during a quiet window if needed.
8. Cut traffic over.
9. Stop `chatting` on Blink, but leave Blink otherwise untouched as rollback
