"""Request termination from the owning supervisor, then wait to be killed."""

from __future__ import annotations

import json
import os
import socket
import time


def main() -> int:
    try:
        path = os.environ["CHATTING_FINISH_SOCKET"]
        request = {
            "task_id": os.environ["CHATTING_FINISH_TASK_ID"],
            "execution_id": os.environ["CHATTING_FINISH_EXECUTION_ID"],
        }
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.connect(path)
            connection.sendall(json.dumps(request).encode() + b"\n")
            response = connection.recv(4096)
            if response:
                print(response.decode(), flush=True)
                return 1
        # EOF is not permission to exit; the supervisor owns termination.
        while True:
            time.sleep(60)
    except (KeyError, OSError) as error:
        print(f"finish request failed: {error}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
