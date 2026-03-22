import unittest

from app.models import ActionProposal, ExecutionResult
from app.worker.policy import AllowlistPolicyEngine


class AllowlistPolicyEngineTests(unittest.TestCase):
    def test_blocks_disallowed_actions(self) -> None:
        result = ExecutionResult(
            actions=[
                ActionProposal(type="write_file", path="docs/notes.md", content="hello"),
                ActionProposal(type="run_shell", path="echo test"),
            ],
            errors=[],
        )
        engine = AllowlistPolicyEngine(allowed_action_types=frozenset({"write_file"}))

        decision = engine.evaluate(result)

        self.assertEqual([action.type for action in decision.approved_actions], ["write_file"])
        self.assertEqual([action.type for action in decision.blocked_actions], ["run_shell"])
        self.assertEqual(decision.approved_messages, [])
        self.assertEqual(decision.reason_codes, ["action_not_allowed"])

    def test_reports_executor_errors_in_reason_codes(self) -> None:
        result = ExecutionResult(
            actions=[],
            errors=["executor timeout"],
        )
        engine = AllowlistPolicyEngine()

        decision = engine.evaluate(result)

        self.assertEqual(decision.reason_codes, ["executor_reported_errors"])


if __name__ == "__main__":
    unittest.main()
