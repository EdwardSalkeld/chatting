import unittest

from app.applier import NoOpApplier
from app.models import (
    ActionProposal,
    ConfigUpdateDecision,
    OutboundMessage,
    PolicyDecision,
)


class NoOpApplierTests(unittest.TestCase):
    def test_skips_approved_actions_and_dispatches_messages(self) -> None:
        decision = PolicyDecision(
            approved_actions=[ActionProposal(type="write_file", path="docs/notes.md")],
            blocked_actions=[],
            approved_messages=[OutboundMessage(channel="email", target="alice@example.com", body="Done.")],
            config_updates=ConfigUpdateDecision(),
            reason_codes=[],
        )

        result = NoOpApplier().apply(decision)

        self.assertEqual(result.applied_actions, [])
        self.assertEqual([action.type for action in result.skipped_actions], ["write_file"])
        self.assertEqual(
            [message.channel for message in result.dispatched_messages],
            ["email"],
        )
        self.assertEqual(result.reason_codes, ["noop_applier_skipped_actions"])

    def test_emits_reason_when_blocked_actions_exist(self) -> None:
        decision = PolicyDecision(
            approved_actions=[],
            blocked_actions=[ActionProposal(type="run_shell", path="rm -rf /")],
            approved_messages=[],
            config_updates=ConfigUpdateDecision(),
            reason_codes=["action_not_allowed"],
        )

        result = NoOpApplier().apply(decision)

        self.assertEqual(result.reason_codes, ["policy_blocked_actions_present"])


if __name__ == "__main__":
    unittest.main()
