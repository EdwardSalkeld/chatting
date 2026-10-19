"""Opt-in check of the installed protocol and actual persisted model context."""

import json
import os
from pathlib import Path
import tempfile
import time
import unittest

from app.worker.executor.app_server_session import AppServer


@unittest.skipUnless(
    os.environ.get("CHATTING_CODEX_APP_SERVER_BIN"),
    "requires opt-in Codex authentication and model access",
)
class AppServerLiveTests(unittest.TestCase):
    def test_context_survives_process_restart(self):
        events = []

        def emit(name, line):
            if name == "stdout":
                events.append(json.loads(line))

        with tempfile.TemporaryDirectory() as directory:
            thread_id = None
            for content in (
                "Protocol smoke test. Do not use tools. Remember the keyword otter and reply exactly OK.",
                "Protocol smoke test continued. Do not use tools. Reply with only the keyword I asked you to remember.",
            ):
                server = AppServer(
                    [os.environ["CHATTING_CODEX_APP_SERVER_BIN"], "app-server"]
                )
                try:
                    deadline = time.monotonic() + 90
                    server.rpc(
                        "initialize",
                        {
                            "clientInfo": {
                                "name": "chatting_protocol_test",
                                "version": "0.1.0",
                            }
                        },
                        deadline,
                        emit,
                    )
                    server.send({"method": "initialized"})
                    params = {"cwd": str(Path(directory)), "approvalPolicy": "never"}
                    if thread_id:
                        params["threadId"] = thread_id
                        params["excludeTurns"] = True
                    result = server.rpc(
                        "thread/resume" if thread_id else "thread/start",
                        params,
                        deadline,
                        emit,
                    )
                    if thread_id:
                        self.assertEqual(result["thread"]["id"], thread_id)
                    thread_id = result["thread"]["id"]
                    server.turn(
                        {
                            "payload": {"task": {"content": content}},
                            "cwd": directory,
                            "model_tier": "low",
                        },
                        thread_id,
                        deadline,
                        emit,
                    )
                finally:
                    server.close()
        messages = [
            event["params"]["item"]["text"]
            for event in events
            if event.get("method") == "item/completed"
            and event.get("params", {}).get("item", {}).get("type") == "agentMessage"
        ]
        self.assertTrue(messages)
        self.assertEqual(messages[-1].strip().lower(), "otter")
