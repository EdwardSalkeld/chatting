# Magpie Runtime Deployment

This directory contains the first non-Docker deployment helpers for moving the
split `chatting` runtime from Blink onto `magpie`.

## What this covers

- build the on-host runtime tree (`deploy/magpie/build-runtime.sh`)
- run the three long-lived services under systemd
- transform Blink's config files into Magpie host paths
- one-off sync the current Blink state into Magpie

The expected host layout matches the architecture note:

- repo checkout: `/srv/chatting/repo`
- worker workspace: `/srv/chatting/workspace`
- handler state: `/var/lib/handler`
- worker state: `/var/lib/worker`
- config: `/etc/chatting`

## Build the runtime on Magpie

Run on `magpie` after the repo checkout is in place:

```sh
cd /srv/chatting/repo
sudo ./deploy/magpie/build-runtime.sh
```

That does three things:

1. creates `.venv` with the worker dependencies
2. builds `.runtime/bin/chatting-handler`
3. downloads `.runtime/bin/bbmb-server`

Override `CHATTING_BBMB_VERSION` if you want a pinned BBMB release instead of
the default `latest`.

## One-off Blink -> Magpie sync

Run from any machine that can SSH to both hosts:

```sh
SSH_KEY=/path/to/lab-billy-rsa ./deploy/magpie/sync-blink-state.sh
```

The script:

1. copies the current Blink `chatting` checkout to `/srv/chatting/repo`
2. rewrites Blink config files into Magpie paths under `/etc/chatting`
3. copies handler and worker Docker volume state into `/var/lib/handler` and
   `/var/lib/worker`
4. copies the worker workspace tree into `/srv/chatting/workspace`
5. builds the runtime on Magpie

Useful overrides:

- `BLINK_HOST`
- `MAGPIE_HOST`
- `BLINK_REPO`
- `BLINK_WORKSPACE`
- `MAGPIE_REPO`
- `MAGPIE_WORKSPACE`
- `SSH_KEY`

## Service entrypoints

The lab host config points systemd at these scripts:

- `deploy/magpie/run-bbmb.sh`
- `deploy/magpie/run-handler.sh`
- `deploy/magpie/run-worker.sh`

They expect the repo to live at `/srv/chatting/repo` and the rendered config
files at `/etc/chatting/handler.json` and `/etc/chatting/worker.json`.
