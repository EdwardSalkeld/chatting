import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.models import ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore
from app.worker.model_router import ModelDecision, choose_model


def _choice(confidence: float, low: float | None = None) -> dict:
    low = (1 + confidence) / 2 if low is None else low
    return {
        "type": "choice",
        "choice": "low",
        "confidence": confidence,
        "probabilities": {"low": low, "high": 1 - low},
    }


class ModelRouterTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = SQLiteStateStore(str(Path(tmp.name) / "worker.db"))
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

    def choose(self, answer, env=None) -> tuple[ModelDecision, object]:
        reply = (
            {"side_effect": answer}
            if isinstance(answer, BaseException)
            else {"return_value": {"answers": {"executor_tier": answer}}}
        )
        with (
            patch.dict(
                os.environ,
                {"TYPESAFE_API_KEY": "k"} if env is None else env,
                clear=True,
            ),
            patch("app.worker.model_router.typesafe.post", **reply) as call,
        ):
            return choose_model(store=self.store, envelope=self.envelope), call

    def test_missing_key_uses_high_without_network_call(self):
        decision, call = self.choose(_choice(0.94), env={})
        self.assertEqual((decision.tier, decision.reason), ("high", "jev_key_missing"))
        call.assert_not_called()

    def test_confident_low_uses_luna_and_sends_bounded_state(self):
        decision, call = self.choose(_choice(0.94))
        self.assertEqual(decision.tier, "low")
        sent = call.call_args.args[2]
        self.assertEqual(sent["state"]["current_request"], self.envelope.content)
        self.assertEqual(call.call_args.kwargs["timeout"], 5)
        self.assertFalse(call.call_args.kwargs["retry_429"])

    def test_real_binary_confidence_boundaries(self):
        for confidence, expected in ((0.79, "high"), (0.8, "low"), (0.81, "low")):
            with self.subTest(confidence=confidence):
                self.assertEqual(self.choose(_choice(confidence))[0].tier, expected)

    def test_failed_or_malformed_results_never_choose_luna(self):
        valid = _choice(0.9)
        for answer in (
            TimeoutError(),
            {**valid, "probabilities": {"low": 0.95, "high": 0.95}},
            {**valid, "confidence": True},
            {**valid, "confidence": float("nan")},
            {**valid, "confidence": 0.99},
            {**valid, "probabilities": {"low": 0.95, "other": 0.05}},
        ):
            with self.subTest(answer=answer):
                decision, _ = self.choose(answer)
                self.assertEqual(
                    (decision.tier, decision.reason),
                    ("high", "jev_unavailable_or_invalid"),
                )
