#!/usr/bin/env bash
set -euo pipefail

KEY="${ONWARD_SSH_KEY:?dispatcher must set ONWARD_SSH_KEY}"
TARGET_SHA="${1:-}"

if [[ -z "$TARGET_SHA" ]]; then
  REPO_ROOT="$(git rev-parse --show-toplevel)"
  TARGET_SHA="$(git -C "$REPO_ROOT" rev-parse HEAD)"
fi

[[ "$TARGET_SHA" =~ ^[0-9a-f]{40}$ ]] || {
  echo "usage: deploy/blink.sh <40-char commit sha>" >&2
  exit 64
}

SHORT_SHA="${TARGET_SHA:0:7}"
echo "==> deploy chatting@$SHORT_SHA to blink"
ssh \
  -i "$KEY" \
  -o IdentitiesOnly=yes \
  -o StrictHostKeyChecking=accept-new \
  edward@blink.int.alcachofa.faith \
  "cd /home/edward/develop/chatting && \
   export CHATTING_RUNTIME_IMAGE=ghcr.io/edwardsalkeld/chatting:sha-$SHORT_SHA && \
   docker compose pull && \
   docker compose up -d"
