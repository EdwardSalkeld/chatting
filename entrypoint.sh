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
    # Runtime state lives on mounted volumes; ensure they remain writable after dropping privileges.
    for path in /data /tmp; do
        if [ -e "$path" ]; then
            chown -R "$PUID:$PGID" "$path"
        fi
    done
    exec gosu "$PUID:$PGID" "$@"
fi

exec "$@"
