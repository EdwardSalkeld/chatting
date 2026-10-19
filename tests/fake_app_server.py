"""Stdio protocol peer for persistent-executor integration tests."""

import json
import os
from pathlib import Path
import sys
import time
import uuid


def send(value):
    print(json.dumps(value), flush=True)


def main():
    log = Path(sys.argv[1])
    for line in sys.stdin:
        request = json.loads(line)
        with log.open("a") as stream:
            stream.write(json.dumps({**request, "pid": os.getpid()}) + "\n")
        method = request.get("method")
        params = request.get("params", {})
        if method == "initialize":
            send({"id": request["id"], "result": {}})
        elif method in ("thread/start", "thread/resume"):
            thread_id = params.get("threadId") or str(uuid.uuid4())
            send({"id": request["id"], "result": {"thread": {"id": thread_id}}})
        elif method == "turn/start":
            payload = json.loads(params["input"][0]["text"])
            task = payload["task"]
            if task["content"] == "escalate":
                contract = payload.get("escalation_contract")
                if contract:
                    Path(contract["request_path"]).write_text(
                        json.dumps(
                            {
                                "task_id": task["task_id"],
                                "reason": "Needs a broader change",
                            }
                        )
                    )
            turn_id = str(uuid.uuid4())
            # Notifications may arrive before the RPC response.
            send({"method": "item/started", "params": {"threadId": thread_id}})
            send({"id": request["id"], "result": {"turn": {"id": turn_id}}})
            if task["content"] == "timeout":
                time.sleep(60)
            if task["content"] == "crash":
                os._exit(7)
            if task["content"] == "unsupported":
                send(
                    {
                        "method": "item/tool/requestUserInput",
                        "id": "server-request",
                        "params": {},
                    }
                )
                continue
            print("test stderr", file=sys.stderr, flush=True)
            send(
                {
                    "method": "item/completed",
                    "params": {
                        "threadId": thread_id,
                        "item": {"type": "agentMessage", "text": "done"},
                    },
                }
            )
            send(
                {
                    "method": "turn/completed",
                    "params": {
                        "threadId": thread_id,
                        "turn": {
                            "id": turn_id,
                            "status": {
                                "fail": "failed",
                                "interrupt": "interrupted",
                            }.get(task["content"], "completed"),
                            "error": None,
                        },
                    },
                }
            )


if __name__ == "__main__":
    main()
