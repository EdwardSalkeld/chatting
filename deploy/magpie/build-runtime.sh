#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
RUNTIME_DIR=${CHATTING_RUNTIME_DIR:-"$REPO_ROOT/.runtime"}
BIN_DIR="$RUNTIME_DIR/bin"
BBMB_VERSION=${CHATTING_BBMB_VERSION:-latest}

mkdir -p "$BIN_DIR"

cd "$REPO_ROOT"
uv sync --locked --no-dev --no-install-project

(
    cd "$REPO_ROOT/go/handler"
    go build -trimpath -o "$BIN_DIR/chatting-handler" ./cmd/chatting-handler
)

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT INT TERM HUP

curl -fsSL \
    -o "$TMPDIR/bbmb-server-linux-amd64" \
    "https://github.com/EdwardSalkeld/bbmb/releases/${BBMB_VERSION}/download/bbmb-server-linux-amd64"
curl -fsSL \
    -o "$TMPDIR/bbmb-server-linux-amd64.sha256" \
    "https://github.com/EdwardSalkeld/bbmb/releases/${BBMB_VERSION}/download/bbmb-server-linux-amd64.sha256"
(
    cd "$TMPDIR"
    sha256sum -c bbmb-server-linux-amd64.sha256
)
install -m 0755 "$TMPDIR/bbmb-server-linux-amd64" "$BIN_DIR/bbmb-server"
