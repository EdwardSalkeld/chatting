#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
HANDLER_BIN=${CHATTING_HANDLER_BIN:-"$REPO_ROOT/.runtime/bin/chatting-handler"}
CONFIG_PATH=${CHATTING_HANDLER_CONFIG_PATH:-/etc/chatting/handler.json}

if [ ! -x "$HANDLER_BIN" ]; then
    echo "missing handler runtime: run deploy/magpie/build-runtime.sh first" >&2
    exit 1
fi

exec "$HANDLER_BIN" --config "$CONFIG_PATH"
