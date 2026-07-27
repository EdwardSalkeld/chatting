#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
HANDLER_BIN=${CHATTING_HANDLER_BIN:-"$REPO_ROOT/.runtime/bin/chatting-handler"}
CONFIG_DIR=${CHATTING_CONFIG_DIR:-/etc/chatting}
CONFIG_PATH=${CHATTING_HANDLER_CONFIG_PATH:-"$CONFIG_DIR/handler.json"}

if [ ! -x "$HANDLER_BIN" ]; then
    echo "missing handler runtime: run deploy/host_runtime/build-runtime.sh first" >&2
    exit 1
fi

exec "$HANDLER_BIN" --config "$CONFIG_PATH"
