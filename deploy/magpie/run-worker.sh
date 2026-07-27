#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
PYTHON_BIN=${CHATTING_WORKER_PYTHON:-"$REPO_ROOT/.venv/bin/python"}
CONFIG_PATH=${CHATTING_WORKER_CONFIG_PATH:-/etc/chatting/worker.json}

if [ ! -x "$PYTHON_BIN" ]; then
    echo "missing worker venv: run deploy/magpie/build-runtime.sh first" >&2
    exit 1
fi

cd "$REPO_ROOT"
exec "$PYTHON_BIN" -m app.main_worker --config "$CONFIG_PATH"
