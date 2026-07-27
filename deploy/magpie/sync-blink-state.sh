#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
BLINK_HOST=${BLINK_HOST:-billy@blink.ts.alcachofa.faith}
MAGPIE_HOST=${MAGPIE_HOST:-billy@magpie.int.alcachofa.faith}
BLINK_REPO=${BLINK_REPO:-/home/edward/develop/chatting}
BLINK_WORKSPACE=${BLINK_WORKSPACE:-/mnt/ext2tb/4/billy}
MAGPIE_REPO=${MAGPIE_REPO:-/srv/chatting/repo}
MAGPIE_WORKSPACE=${MAGPIE_WORKSPACE:-/srv/chatting/workspace}
SSH_KEY=${SSH_KEY:-}
SSH_OPTS="-o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"

if [ -n "$SSH_KEY" ]; then
    SSH_OPTS="$SSH_OPTS -i $SSH_KEY"
fi

ssh_cmd() {
    # shellcheck disable=SC2086
    ssh $SSH_OPTS "$@"
}

copy_tree() {
    source_host=$1
    source_path=$2
    target_host=$3
    target_path=$4
    target_owner=$5

    ssh_cmd "$source_host" "sudo tar -C '$source_path' -cf - ." \
        | ssh_cmd "$target_host" "sudo mkdir -p '$target_path' && sudo tar -C '$target_path' -xf - && sudo chown -R '$target_owner' '$target_path'"
}

TEMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TEMP_ROOT"' EXIT INT TERM HUP

mkdir -p "$TEMP_ROOT/configs/handler" "$TEMP_ROOT/configs/worker" "$TEMP_ROOT/rendered"

# Pull the checked-out repo first so Magpie has the deployment scripts this same run relies on.
ssh_cmd "$BLINK_HOST" "sudo tar -C '$BLINK_REPO' -cf - ." \
    | ssh_cmd "$MAGPIE_HOST" "sudo rm -rf '$MAGPIE_REPO' && sudo mkdir -p '$MAGPIE_REPO' && sudo tar -C '$MAGPIE_REPO' -xf - && sudo chown -R root:root '$MAGPIE_REPO' && sudo find '$MAGPIE_REPO/deploy/magpie' -type f -name '*.sh' -exec chmod 0755 {} +"

# Copy the current Blink configs locally, rewrite them, then install into /etc/chatting.
rsync -a -e "ssh $SSH_OPTS" "$BLINK_HOST:$BLINK_REPO/configs/handler/" "$TEMP_ROOT/configs/handler/"
rsync -a -e "ssh $SSH_OPTS" "$BLINK_HOST:$BLINK_REPO/configs/worker/" "$TEMP_ROOT/configs/worker/"
python3 "$REPO_ROOT/deploy/magpie/render_runtime_config.py" \
    --source-root "$TEMP_ROOT/configs" \
    --output-root "$TEMP_ROOT/rendered"
rsync -a -e "ssh $SSH_OPTS" "$TEMP_ROOT/rendered/" "$MAGPIE_HOST:/tmp/chatting-rendered/"
ssh_cmd "$MAGPIE_HOST" "sudo mkdir -p /etc/chatting && sudo install -m 0644 /tmp/chatting-rendered/handler.json /etc/chatting/handler.json && sudo install -m 0644 /tmp/chatting-rendered/worker.json /etc/chatting/worker.json && if [ -f /tmp/chatting-rendered/live-schedule.local.json ]; then sudo install -m 0644 /tmp/chatting-rendered/live-schedule.local.json /etc/chatting/live-schedule.local.json; fi && if [ -f /tmp/chatting-rendered/handler.env ]; then sudo install -m 0400 /tmp/chatting-rendered/handler.env /etc/chatting/handler.env; fi && if [ -f /tmp/chatting-rendered/worker.env ]; then sudo install -m 0400 /tmp/chatting-rendered/worker.env /etc/chatting/worker.env; fi && rm -rf /tmp/chatting-rendered"

copy_tree "$BLINK_HOST" "/var/lib/docker/volumes/chatting_handler-data/_data" "$MAGPIE_HOST" "/var/lib/handler" "handler:handler"
copy_tree "$BLINK_HOST" "/var/lib/docker/volumes/chatting_worker-data/_data" "$MAGPIE_HOST" "/var/lib/worker" "worker:worker"
copy_tree "$BLINK_HOST" "/var/lib/docker/volumes/chatting_codex-auth/_data" "$MAGPIE_HOST" "/var/lib/worker/.codex" "worker:worker"
copy_tree "$BLINK_HOST" "/var/lib/docker/volumes/chatting_claude-auth/_data" "$MAGPIE_HOST" "/var/lib/worker/.claude" "worker:worker"
copy_tree "$BLINK_HOST" "/var/lib/docker/volumes/chatting_gh-auth/_data" "$MAGPIE_HOST" "/var/lib/handler/.config/gh" "handler:handler"
copy_tree "$BLINK_HOST" "/var/lib/docker/volumes/chatting_gh-auth/_data" "$MAGPIE_HOST" "/var/lib/worker/.config/gh" "worker:worker"
copy_tree "$BLINK_HOST" "$BLINK_WORKSPACE" "$MAGPIE_HOST" "$MAGPIE_WORKSPACE" "worker:worker"

ssh_cmd "$MAGPIE_HOST" "cd '$MAGPIE_REPO' && sudo ./deploy/magpie/build-runtime.sh"
