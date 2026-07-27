#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
PYTHON_BIN=${CHATTING_WORKER_PYTHON:-"$REPO_ROOT/.venv/bin/python"}
CONFIG_DIR=${CHATTING_CONFIG_DIR:-/etc/chatting}
CONFIG_PATH=${CHATTING_WORKER_CONFIG_PATH:-"$CONFIG_DIR/worker.json"}

if [ ! -x "$PYTHON_BIN" ]; then
    echo "missing worker venv: run deploy/host_runtime/build-runtime.sh first" >&2
    exit 1
fi

cd "$REPO_ROOT"
exec "$PYTHON_BIN" -m app.main_worker --config "$CONFIG_PATH"
