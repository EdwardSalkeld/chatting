#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
BBMB_BIN=${CHATTING_BBMB_BIN:-"$REPO_ROOT/.runtime/bin/bbmb-server"}

if [ ! -x "$BBMB_BIN" ]; then
    echo "missing bbmb runtime: run deploy/magpie/build-runtime.sh first" >&2
    exit 1
fi

exec "$BBMB_BIN" --port=9876 --metrics-port=9877
