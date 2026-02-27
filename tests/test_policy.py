import unittest

from app.models import ActionProposal, ConfigUpdate, ExecutionResult, OutboundMessage
from app.policy import AllowlistPolicyEngine


class AllowlistPolicyEngineTests(unittest.TestCase):
    def test_blocks_disallowed_actions_and_approves_messages(self) -> None:
        result = ExecutionResult(
            messages=[OutboundMessage(channel="email", target="alice@example.com", body="Done.")],
            actions=[
                ActionProposal(type="write_file", path="docs/notes.md", content="hello"),
                ActionProposal(type="run_shell", path="echo test"),
            ],
            config_updates=[],
            requires_human_review=False,
            errors=[],
        )
        engine = AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"}))

        decision = engine.evaluate(result)

        self.assertEqual([action.type for action in decision.approved_actions], ["write_file"])
        self.assertEqual([action.type for action in decision.blocked_actions], ["run_shell"])
        self.assertEqual(len(decision.approved_messages), 1)
        self.assertEqual(decision.reason_codes, ["action_not_allowed"])

    def test_sorts_config_updates_into_approved_pending_and_rejected(self) -> None:
        result = ExecutionResult(
            messages=[],
            actions=[],
            config_updates=[
                ConfigUpdate(path="routing.default_timeout", value=240),
                ConfigUpdate(path="secrets.api_key", value="new-secret"),
                ConfigUpdate(path="routing.unknown", value=True),
            ],
            requires_human_review=False,
            errors=[],
        )
        engine = AllowlistPolicyEngine(
            allowed_config_paths=frozenset({"routing.default_timeout"})
        )

        decision = engine.evaluate(result)

        self.assertEqual(
            [update.path for update in decision.config_updates.approved],
            ["routing.default_timeout"],
        )
        self.assertEqual(
            [update.path for update in decision.config_updates.pending_review],
            ["secrets.api_key"],
        )
        self.assertEqual(
            [update.path for update in decision.config_updates.rejected],
            ["routing.unknown"],
        )
        self.assertEqual(
            decision.reason_codes,
            ["config_update_requires_review", "config_update_not_allowed"],
        )

    def test_reports_executor_signals_in_reason_codes(self) -> None:
        result = ExecutionResult(
            messages=[],
            actions=[],
            config_updates=[],
            requires_human_review=True,
            errors=["executor timeout"],
        )
        engine = AllowlistPolicyEngine()

        decision = engine.evaluate(result)

        self.assertEqual(
            decision.reason_codes,
            ["executor_requires_human_review", "executor_reported_errors"],
        )


if __name__ == "__main__":
    unittest.main()
