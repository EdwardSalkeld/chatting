import copy
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
            patch("app.worker.model_router.typesafe.post") as call,
        ):
            decision = choose_model(store=self.store, envelope=self.envelope)
        self.assertEqual((decision.tier, decision.reason), ("high", "jev_key_missing"))
        call.assert_not_called()

    def test_confident_low_uses_luna_and_sends_bounded_state(self):
        answer = {
            "model": "jev-1.14.0",
            "answers": {
                "executor_tier": {
                    "type": "choice",
                    "choice": "low",
                    "confidence": 0.94,
                    "probabilities": {"low": 0.97, "high": 0.03},
                }
            },
        }
        with (
            patch.dict(os.environ, {"TYPESAFE_API_KEY": "test-key"}),
            patch(
                "app.worker.model_router.typesafe.post",
                return_value=answer,
            ) as call,
        ):
            decision = choose_model(store=self.store, envelope=self.envelope)
        self.assertEqual(decision.tier, "low")
        sent = call.call_args.args[2]
        self.assertEqual(sent["state"]["current_request"], self.envelope.content)
        self.assertEqual(sent["model"], "jev-1.14.0")
        self.assertEqual(call.call_args.kwargs["timeout"], 5)
        self.assertFalse(call.call_args.kwargs["retry_429"])

    def test_uncertain_or_failed_choice_uses_sol(self):
        answer = {
            "model": "jev-1.14.0",
            "answers": {
                "executor_tier": {
                    "type": "choice",
                    "choice": "low",
                    "confidence": 0.70,
                    "probabilities": {"low": 0.85, "high": 0.15},
                }
            },
        }
        with (
            patch.dict(os.environ, {"TYPESAFE_API_KEY": "test-key"}),
            patch(
                "app.worker.model_router.typesafe.post",
                return_value=answer,
            ),
        ):
            self.assertEqual(
                choose_model(store=self.store, envelope=self.envelope).tier, "high"
            )
        with (
            patch.dict(os.environ, {"TYPESAFE_API_KEY": "test-key"}),
            patch(
                "app.worker.model_router.typesafe.post",
                side_effect=TimeoutError,
            ),
        ):
            self.assertEqual(
                choose_model(store=self.store, envelope=self.envelope).reason,
                "jev_unavailable_or_invalid",
            )

    def test_malformed_results_never_choose_luna(self):
        valid = {
            "type": "choice",
            "choice": "low",
            "confidence": 0.9,
            "probabilities": {"low": 0.95, "high": 0.05},
        }
        cases = [
            {**valid, "probabilities": {"low": 0.95, "high": 0.95}},
            {**valid, "confidence": True},
            {**valid, "confidence": "0.9"},
            {**valid, "confidence": float("nan")},
            {**valid, "confidence": 0.99},
            {**valid, "choice": "high"},
            {**valid, "probabilities": {"low": True, "high": 0}},
            {**valid, "probabilities": {"low": 0.95, "other": 0.05}},
        ]
        for answer in cases:
            with (
                self.subTest(answer=answer),
                patch.dict(os.environ, {"TYPESAFE_API_KEY": "test"}),
                patch(
                    "app.worker.model_router.typesafe.post",
                    return_value={"answers": {"executor_tier": copy.deepcopy(answer)}},
                ),
            ):
                decision = choose_model(store=self.store, envelope=self.envelope)
                self.assertEqual(
                    (decision.tier, decision.reason),
                    ("high", "jev_unavailable_or_invalid"),
                )

    def test_real_binary_confidence_boundaries(self):
        for confidence, expected in ((0.79, "high"), (0.8, "low"), (0.81, "low")):
            p = (1 + confidence) / 2
            answer = {
                "type": "choice",
                "choice": "low",
                "confidence": confidence,
                "probabilities": {"low": p, "high": 1 - p},
            }
            with (
                self.subTest(confidence=confidence),
                patch.dict(os.environ, {"TYPESAFE_API_KEY": "test"}),
                patch(
                    "app.worker.model_router.typesafe.post",
                    return_value={"answers": {"executor_tier": answer}},
                ),
            ):
                self.assertEqual(
                    choose_model(store=self.store, envelope=self.envelope).tier,
                    expected,
                )


if __name__ == "__main__":
    unittest.main()
