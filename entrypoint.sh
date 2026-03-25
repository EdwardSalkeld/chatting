#!/bin/sh
set -e

PUID=${PUID:-0}
PGID=${PGID:-0}

if [ "$PUID" != "0" ] || [ "$PGID" != "0" ]; then
    if ! getent group "$PGID" > /dev/null 2>&1; then
        groupadd -g "$PGID" chatting
    fi
    if ! getent passwd "$PUID" > /dev/null 2>&1; then
        useradd -u "$PUID" -g "$PGID" -d "$HOME" -s /bin/sh "chatting_$PUID"
    fi
    chown -R "$PUID:$PGID" "$HOME"
    exec gosu "$PUID:$PGID" "$@"
fi

exec "$@"
