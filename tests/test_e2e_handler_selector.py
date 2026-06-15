import unittest
from pathlib import Path

from tests.e2e.handler_selector import (
    HANDLER_IMPLEMENTATION_ENV,
    message_handler_command,
    selected_handler_implementation,
)


class E2EHandlerSelectorTests(unittest.TestCase):
    def test_default_handler_implementation_is_go(self) -> None:
        self.assertEqual(selected_handler_implementation({}), "go")

    def test_go_handler_command_uses_go_entrypoint(self) -> None:
        command = message_handler_command(
            Path("/tmp/handler.json"),
            env={HANDLER_IMPLEMENTATION_ENV: "go"},
        )

        self.assertEqual(
            command,
            [
                "sh",
                "-c",
                'cd go/handler && exec go run ./cmd/chatting-handler --config "$1"',
                "chatting-handler",
                "/tmp/handler.json",
            ],
        )

    def test_unknown_handler_implementation_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, HANDLER_IMPLEMENTATION_ENV):
            selected_handler_implementation({HANDLER_IMPLEMENTATION_ENV: "python"})


if __name__ == "__main__":
    unittest.main()
