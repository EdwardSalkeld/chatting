import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.models import ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore
from app.worker.model_router import choose_model
from datetime import datetime, timezone


class ModelRouterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = SQLiteStateStore(str(Path(self.tmp.name) / "worker.db"))
        self.envelope = TaskEnvelope(
            id="test:1",
            source="im",
            received_at=datetime(2026, 10, 5, tzinfo=timezone.utc),
            actor="edward",
            content="What Linux version is Sparrow running?",
            attachments=[],
            context_refs=[],
            reply_channel=ReplyChannel(
                type="telegram", target="chat", metadata={"message_id": 101}
            ),
            dedupe_key="test:1",
        )

    def test_missing_key_uses_sol_without_network_call(self):
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("app.worker.model_router.urllib.request.urlopen") as call,
        ):
            decision = choose_model(store=self.store, envelope=self.envelope)
        self.assertEqual((decision.tier, decision.reason), ("high", "jev_key_missing"))
        call.assert_not_called()

    def test_confident_low_uses_luna_and_sends_bounded_state(self):
        answer = {
            "model": "jev-1.13.0",
            "answers": {
                "executor_tier": {
                    "type": "choice",
                    "choice": "low",
                    "confidence": 0.94,
                    "probabilities": {"low": 0.94, "high": 0.06},
                }
            },
        }
        with (
            patch.dict(os.environ, {"TYPESAFE_API_KEY": "test-key"}),
            patch(
                "app.worker.model_router.urllib.request.urlopen",
                return_value=io.BytesIO(json.dumps(answer).encode()),
            ) as call,
        ):
            decision = choose_model(store=self.store, envelope=self.envelope)
        self.assertEqual(decision.tier, "low")
        sent = json.loads(call.call_args.args[0].data)
        self.assertEqual(sent["state"]["current_request"], self.envelope.content)
        self.assertEqual(sent["model"], "jev-latest")
        self.assertEqual(call.call_args.kwargs["timeout"], 5)

    def test_uncertain_or_failed_choice_uses_sol(self):
        answer = {
            "model": "jev-latest",
            "answers": {
                "executor_tier": {
                    "type": "choice",
                    "choice": "low",
                    "confidence": 0.70,
                    "probabilities": {"low": 0.70, "high": 0.30},
                }
            },
        }
        with (
            patch.dict(os.environ, {"TYPESAFE_API_KEY": "test-key"}),
            patch(
                "app.worker.model_router.urllib.request.urlopen",
                return_value=io.BytesIO(json.dumps(answer).encode()),
            ),
        ):
            self.assertEqual(
                choose_model(store=self.store, envelope=self.envelope).tier, "high"
            )
        with (
            patch.dict(os.environ, {"TYPESAFE_API_KEY": "test-key"}),
            patch(
                "app.worker.model_router.urllib.request.urlopen",
                side_effect=TimeoutError,
            ),
        ):
            self.assertEqual(
                choose_model(store=self.store, envelope=self.envelope).reason,
                "jev_unavailable_or_invalid",
            )


if __name__ == "__main__":
    unittest.main()
